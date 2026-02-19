from __future__ import annotations

import html as ihtml
import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from ..utils import parse_float, parse_ft_date

log = logging.getLogger("scrapers.ft_scraper")

# Divisas ISO 4217 más comunes para distinguir moneda de código de mercado
KNOWN_CURRENCIES = {
    "EUR", "GBP", "USD", "CHF", "JPY", "SEK", "NOK", "DKK", "PLN", "CZK",
    "HUF", "RON", "BGN", "ISK", "TRY", "CAD", "AUD", "NZD", "SGD",
    "HKD", "CNY", "BRL", "MXN", "ZAR", "INR", "KRW",
}

ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")


def _resolve_ft_symbol(raw: str, default_currency: str = "EUR") -> Tuple[str, str, str]:
    """
    Devuelve (base, currency, ft_symbol_full).

    Ejemplos:
      "LU0563745743"     -> ("LU0563745743", "EUR", "LU0563745743:EUR")
      "AMEE:GER"         -> ("AMEE:GER",     "EUR", "AMEE:GER:EUR")
      "AMEE:GER:EUR"     -> ("AMEE:GER",     "EUR", "AMEE:GER:EUR")
      "LU0563745743:EUR" -> ("LU0563745743", "EUR", "LU0563745743:EUR")
      "SWDA:LSE:GBP"     -> ("SWDA:LSE",     "GBP", "SWDA:LSE:GBP")
    """
    s = raw.strip()
    parts = s.split(":")

    if len(parts) == 1:
        # Sin dos puntos: ISIN puro o ticker sin mercado
        base, currency = parts[0], default_currency

    elif len(parts) == 2:
        # ISIN:EUR  o  TICKER:EXCHANGE
        if parts[1].upper() in KNOWN_CURRENCIES:
            base, currency = parts[0], parts[1].upper()
        else:
            # Es TICKER:EXCHANGE → base completo, añadir divisa por defecto
            base, currency = s, default_currency

    else:
        # 3+ partes → última es divisa si es conocida (AMEE:GER:EUR, SWDA:LSE:GBP)
        if parts[-1].upper() in KNOWN_CURRENCIES:
            base, currency = ":".join(parts[:-1]), parts[-1].upper()
        else:
            base, currency = s, default_currency

    return base, currency, f"{base}:{currency}"


def _build_ft_urls(ft_symbol_full: str, base: str) -> List[str]:
    """
    Construye lista de URLs a intentar en orden de prioridad.
    - ISIN → funds primero, etfs como fallback
    - Ticker:Exchange → etfs primero, funds como fallback
    """
    is_isin = bool(ISIN_RE.match(base.upper()))
    is_etf = ":" in base  # contiene mercado → es ETF

    if is_isin:
        return [
            f"https://markets.ft.com/data/funds/tearsheet/historical?s={ft_symbol_full}",
            f"https://markets.ft.com/data/etfs/tearsheet/historical?s={ft_symbol_full}",
        ]
    elif is_etf:
        return [
            f"https://markets.ft.com/data/etfs/tearsheet/historical?s={ft_symbol_full}",
            f"https://markets.ft.com/data/funds/tearsheet/historical?s={ft_symbol_full}",
        ]
    else:
        return [
            f"https://markets.ft.com/data/funds/tearsheet/historical?s={ft_symbol_full}",
            f"https://markets.ft.com/data/etfs/tearsheet/historical?s={ft_symbol_full}",
        ]


def _extract_tearsheet_metadata(soup: BeautifulSoup, ft_symbol_full: str, url: str) -> Dict:
    meta: Dict = {"ft_symbol_used": ft_symbol_full, "url": url}

    h1 = soup.select_one(
        "h1.mod-tearsheet-overview__header__name,"
        "h1.mod-tearsheet-overview__header__name--large"
    )
    if h1:
        meta["name"] = h1.get_text(" ", strip=True)

    m = re.search(r":([A-Z]{3})$", ft_symbol_full)
    if m and m.group(1) in KNOWN_CURRENCIES:
        meta["currency"] = m.group(1)

    return meta


def _extract_historical_app_config(soup: BeautifulSoup) -> Optional[Dict]:
    app = soup.select_one('div[data-module-name="HistoricalPricesApp"][data-mod-config]')
    if not app:
        container = soup.select_one('div[data-f2-app-id="mod-tearsheet-historical-prices"]')
        if container:
            app = container.select_one("div[data-mod-config]")
    if not app:
        return None

    raw = app.get("data-mod-config", "")
    if not raw:
        return None

    try:
        cfg = json.loads(ihtml.unescape(raw).strip())
        return cfg if isinstance(cfg, dict) else None
    except Exception:
        return None


def _to_ft_date_param(d: date) -> str:
    return d.strftime("%Y/%m/%d")


def _date_chunks(start: date, end: date, chunk_days: int = 365) -> List[Tuple[date, date]]:
    chunks, cur = [], start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=chunk_days - 1))
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


def _looks_like_html(text: str) -> bool:
    t = (text or "").lstrip().lower()
    return t.startswith("<!doctype html") or t.startswith("<html")


def _fetch_ajax_html(session, symbol_numeric: str, start: date, end: date) -> Optional[str]:
    url = "https://markets.ft.com/data/equities/ajax/get-historical-prices"
    params = {
        "startDate": _to_ft_date_param(start),
        "endDate": _to_ft_date_param(end),
        "symbol": str(symbol_numeric),
    }
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://markets.ft.com/",
    }
    try:
        r = session.get(url, params=params, headers=headers, timeout=25)
        log.debug("FT AJAX: GET %s params=%s status=%s", url, params, r.status_code)

        if r.status_code != 200:
            return None
        if _looks_like_html(r.text):
            log.warning("FT AJAX: respuesta HTML en vez de JSON (posible bloqueo). symbol=%s", symbol_numeric)
            return None

        payload = r.json()
        if isinstance(payload, dict) and payload.get("html"):
            return payload["html"]

    except Exception as e:
        log.warning("FT AJAX error: %s", e)

    return None


def _parse_prices_html_fragment(html_fragment: str) -> List[Tuple[str, float]]:
    soup = BeautifulSoup(f"<table>{html_fragment}</table>", "lxml")
    out: List[Tuple[str, float]] = []
    log_rows = os.getenv("FT_LOG_ROWS", "0").strip() == "1"

    for i, tr in enumerate(soup.select("tr"), start=1):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        date_raw = tds[0].get_text(" ", strip=True)
        close_raw = tds[4].get_text(" ", strip=True)

        if log_rows:
            log.debug("FT AJAX: Fila %s - Fecha raw: %r, Close raw: %r", i, date_raw, close_raw)

        try:
            out.append((parse_ft_date(date_raw), parse_float(close_raw)))
        except Exception as e:
            if log_rows:
                log.debug("FT AJAX: fila %s no parseable (%s)", i, e)

    return out


def scrape_ft_prices_and_metadata(
    session,
    ft_symbol: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> Tuple[List[Tuple[str, float]], Dict]:
    """
    - Resuelve automáticamente el símbolo completo (añade :EUR si falta).
    - Detecta si es fondo (/funds/) o ETF (/etfs/) y prueba el otro como fallback.
    - full_refresh=True: backfill completo desde inception.
    - full_refresh=False: incremental desde start_date o últimos 45 días.
    """
    meta: Dict = {"ft_symbol_requested": ft_symbol}

    base, currency, ft_symbol_full = _resolve_ft_symbol(ft_symbol)
    urls_to_try = _build_ft_urls(ft_symbol_full, base)
    end = end_date or date.today()

    log.debug("FT: símbolo resuelto %r -> %r (base=%r, currency=%r)", ft_symbol, ft_symbol_full, base, currency)

    for tearsheet_url in urls_to_try:
        meta.update({"url": tearsheet_url, "ft_symbol_used": ft_symbol_full})

        try:
            r = session.get(tearsheet_url, timeout=25)
            meta["status_code"] = r.status_code
            meta["final_url"] = str(getattr(r, "url", tearsheet_url))
            log.debug("FT: GET %s status=%s final_url=%s", tearsheet_url, r.status_code, meta["final_url"])

            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text or "", "lxml")
            meta.update(_extract_tearsheet_metadata(soup, ft_symbol_full, tearsheet_url))

            cfg = _extract_historical_app_config(soup)
            if not cfg:
                log.warning("FT: No se encontró HistoricalPricesApp en %s", tearsheet_url)
                continue

            symbol_numeric = str(cfg.get("symbol", "")).strip()
            if not symbol_numeric:
                log.warning("FT: data-mod-config sin 'symbol': %s", cfg)
                continue

            inception_dt: Optional[date] = None
            inception_raw = str(cfg.get("inception", "")).strip()
            if inception_raw:
                try:
                    inception_dt = datetime.fromisoformat(inception_raw.replace("Z", "+00:00")).date()
                except Exception:
                    pass

            meta["symbol_numeric"] = symbol_numeric
            if inception_dt:
                meta["inception_date"] = inception_dt.isoformat()

            # Rango de fechas
            if full_refresh:
                start = inception_dt or (date.today() - timedelta(days=365 * 10))
                chunks = _date_chunks(start, end, chunk_days=365)
            else:
                start = start_date or (end - timedelta(days=45))
                chunks = [(start, end)]

            collected: Dict[str, float] = {}
            for s, e in chunks:
                frag = _fetch_ajax_html(session, symbol_numeric, s, e)
                if frag:
                    for d, c in _parse_prices_html_fragment(frag):
                        collected[d] = c
                if full_refresh:
                    time.sleep(0.2)

            if collected:
                prices = sorted(collected.items())
                log.debug("FT: %s precios para %s (full_refresh=%s)", len(prices), ft_symbol_full, full_refresh)
                return prices, meta

            log.warning("FT: 0 precios desde %s (symbol_numeric=%s)", tearsheet_url, symbol_numeric)

        except Exception as e:
            log.error("FT error url=%s: %s", tearsheet_url, e, exc_info=True)

    return [], meta
