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

# ── Rutas tearsheet soportadas por FT ────────────────────────────────────────
# FT usa diferentes sub-rutas según el tipo de instrumento:
#   /data/funds/tearsheet/historical?s=LU0563745743:EUR   (fondos)
#   /data/etfs/tearsheet/historical?s=AMEE:GER:EUR        (ETFs)
#   /data/equities/tearsheet/historical?s=VOD:LSE:GBP     (acciones, fallback)
# No diferenciamos por tipo: probamos todas las rutas hasta que una devuelva
# 200 con data-mod-config válido.
_TEARSHEET_PATHS = ["funds", "etfs", "equities"]


def _tearsheet_urls(ft_symbol: str) -> List[str]:
    """Genera todas las URLs tearsheet para un símbolo (funds → etfs → equities)."""
    sym = (ft_symbol or "").strip()
    if not sym:
        return []
    return [
        f"https://markets.ft.com/data/{path}/tearsheet/historical?s={sym}"
        for path in _TEARSHEET_PATHS
    ]


def _symbol_variants(ft_symbol: str) -> List[str]:
    sym = (ft_symbol or "").strip()
    if not sym:
        return []
    variants = [sym]
    if ":" in sym:
        no_colon = sym.replace(":", "")
        if no_colon not in variants:
            variants.append(no_colon)
    else:
        m = re.match(r"^(.+?)([A-Z]{3})$", sym)
        if m:
            with_colon = f"{m.group(1)}:{m.group(2)}"
            if with_colon not in variants:
                variants.append(with_colon)
    return variants


def _extract_tearsheet_metadata(soup: BeautifulSoup, sym_used: str, url: str) -> Dict:
    meta: Dict = {"ft_symbol_used": sym_used, "url": url}

    h1 = soup.select_one(
        "h1.mod-tearsheet-overview__header__name,"
        "h1.mod-tearsheet-overview__header__name--large"
    )
    if h1:
        meta["name"] = h1.get_text(" ", strip=True)

    # Extraer divisa: último segmento tras ':' si tiene 3 letras mayúsculas
    # Cubre: ISIN:EUR, TICKER:EXCHANGE:CCY (como AMEE:GER:EUR)
    parts = sym_used.rsplit(":", 1)
    if len(parts) == 2 and len(parts[-1]) == 3 and parts[-1].isupper():
        meta["currency"] = parts[-1]
    else:
        m = re.search(r"([A-Z]{3})$", sym_used)
        if m:
            meta["currency"] = m.group(1)

    return meta


def _extract_historical_app_config(soup: BeautifulSoup) -> Optional[Dict]:
    """
    Busca HistoricalPricesApp con data-mod-config (JSON escapado con &quot;).
    Igual para fondos y ETFs.
    """
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
    chunks: List[Tuple[date, date]] = []
    cur = start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=chunk_days - 1))
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


def _looks_like_html_page(text: str) -> bool:
    t = (text or "").lstrip().lower()
    return t.startswith("<!doctype html") or t.startswith("<html")


def _fetch_ajax_html(
    session, symbol_numeric: str, start: date, end: date
) -> Optional[str]:
    """
    Endpoint AJAX de FT → JSON con clave 'html' (fragmento de tabla).
    Probamos equities primero (más genérico) y luego funds.
    """
    ajax_endpoints = [
        "https://markets.ft.com/data/equities/ajax/get-historical-prices",
        "https://markets.ft.com/data/funds/ajax/get-historical-prices",
    ]
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

    for ep in ajax_endpoints:
        try:
            r = session.get(ep, params=params, headers=headers, timeout=25)
            log.debug("FT AJAX: GET %s params=%s status=%s", ep, params, r.status_code)

            if r.status_code != 200:
                continue

            if _looks_like_html_page(r.text):
                log.warning("FT AJAX: Respuesta HTML (posible bloqueo) en %s", ep)
                continue

            try:
                payload = r.json()
            except Exception as e:
                sample = re.sub(r"\s+", " ", (r.text or "")[:300])
                log.debug("FT AJAX: JSON inválido en %s (%s). Sample=%r", ep, e, sample)
                continue

            html_frag = (payload or {}).get("html", "")
            if html_frag:
                return html_frag

        except Exception as e:
            log.error("FT AJAX error ep=%s: %s", ep, e, exc_info=True)

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
            d = parse_ft_date(date_raw)
            c = parse_float(close_raw)
            out.append((d, c))
        except Exception as e:
            if log_rows:
                log.debug("FT AJAX: No se pudo parsear fila %s (%s)", i, e)

    return out


def _try_tearsheet(
    session,
    sym: str,
    end: date,
    start_date: Optional[date],
    full_refresh: bool,
    meta: Dict,
) -> Optional[List[Tuple[str, float]]]:
    """
    Prueba todas las rutas tearsheet (funds/etfs/equities) para el símbolo dado.
    Devuelve lista de (fecha, precio) en cuanto tenga resultados, o None.
    """
    for url in _tearsheet_urls(sym):
        try:
            r = session.get(url, timeout=25)
            log.debug("FT: GET %s status=%s", url, r.status_code)

            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text or "", "lxml")
            meta.update(_extract_tearsheet_metadata(soup, sym, url))

            cfg = _extract_historical_app_config(soup)
            if not cfg:
                log.debug("FT: Sin data-mod-config en %s", url)
                continue

            symbol_numeric = str(cfg.get("symbol", "")).strip()
            inception_raw = str(cfg.get("inception", "")).strip()

            if not symbol_numeric:
                log.debug("FT: data-mod-config sin 'symbol' en %s: %s", url, cfg)
                continue

            inception_dt: Optional[date] = None
            if inception_raw:
                try:
                    inception_dt = datetime.fromisoformat(
                        inception_raw.replace("Z", "+00:00")
                    ).date()
                except Exception:
                    pass

            meta["symbol_numeric"] = symbol_numeric
            if inception_dt:
                meta["inception_date"] = inception_dt.isoformat()
            meta["tearsheet_url_used"] = url

            # Rango de fechas
            if full_refresh:
                start = inception_dt or (date.today() - timedelta(days=365 * 8))
                chunks = _date_chunks(start, end, chunk_days=365)
            else:
                start = start_date or (end - timedelta(days=45))
                chunks = [(start, end)]

            collected: Dict[str, float] = {}
            for s, e in chunks:
                frag = _fetch_ajax_html(session, symbol_numeric, s, e)
                if not frag:
                    continue
                for d, c in _parse_prices_html_fragment(frag):
                    collected[d] = c
                if full_refresh:
                    time.sleep(0.2)

            if collected:
                prices = sorted(collected.items())  # ascendente por fecha
                log.debug(
                    "FT: %s precios OK (url=%s, full_refresh=%s)",
                    len(prices), url, full_refresh,
                )
                return prices

            log.debug("FT: 0 precios AJAX en %s (symbol=%s)", url, symbol_numeric)

        except Exception as e:
            log.error("FT error url=%s: %s", url, e, exc_info=True)

    return None


def scrape_ft_prices_and_metadata(
    session,
    ft_symbol: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> Tuple[List[Tuple[str, float]], Dict]:
    """
    Scraper FT unificado. Soporta sin configuración:
      - Fondos:   .../funds/tearsheet/historical?s=LU0563745743:EUR
      - ETFs:     .../etfs/tearsheet/historical?s=AMEE:GER:EUR
      - Acciones: .../equities/tearsheet/historical?s=VOD:LSE:GBP

    Si FT falla o devuelve 0 precios → devuelve ([], meta).
    El caller (app.py) complementa con Fundsquare si hace falta.
    """
    meta: Dict = {"ft_symbol_requested": ft_symbol}
    end = end_date or date.today()

    for sym in _symbol_variants(ft_symbol):
        meta["ft_symbol_used"] = sym
        result = _try_tearsheet(session, sym, end, start_date, full_refresh, meta)
        if result is not None:
            return result, meta

    log.warning("FT: 0 precios para '%s'. Revisa DEBUG para diagnóstico.", ft_symbol)
    return [], meta
