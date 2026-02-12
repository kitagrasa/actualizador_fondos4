from __future__ import annotations

import html as ihtml
import json
import logging
import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from ..utils import parse_float, parse_ft_date

log = logging.getLogger("scrapers.ft_scraper")


def _symbol_variants(ft_symbol: str) -> List[str]:
    """
    FT a veces acepta "ISIN:EUR" y otras termina redirigiendo/buscando.
    Probamos variantes para maximizar tasa de éxito.
    """
    sym = (ft_symbol or "").strip()
    if not sym:
        return []

    variants = [sym]

    if ":" in sym:
        variants.append(sym.replace(":", ""))
    else:
        m = re.match(r"^(.+?)([A-Z]{3})$", sym)
        if m:
            variants.append(f"{m.group(1)}:{m.group(2)}")

    out, seen = [], set()
    for v in variants:
        if v not in seen:
            out.append(v)
            seen.add(v)
    return out


def _extract_tearsheet_metadata(soup: BeautifulSoup, sym_used: str, url: str) -> Dict:
    meta: Dict = {"ft_symbol_used": sym_used, "url": url}

    h1 = soup.select_one(
        "h1.mod-tearsheet-overview__header__name, "
        "h1.mod-tearsheet-overview__header__name--large"
    )
    if h1:
        meta["name"] = h1.get_text(" ", strip=True)

    m = re.search(r":([A-Z]{3})$", sym_used)
    if m:
        meta["currency"] = m.group(1)
    else:
        m = re.search(r"([A-Z]{3})$", sym_used)
        if m:
            meta["currency"] = m.group(1)

    return meta


def _extract_historical_app_config(soup: BeautifulSoup) -> Optional[Dict]:
    """
    Busca el módulo HistoricalPricesApp que incluye data-mod-config con JSON
    (en el HTML fuente aparece con &quot; escapado) [file:42].
    """
    app = soup.select_one('div[data-module-name="HistoricalPricesApp"][data-mod-config]')
    if not app:
        # Fallback por si cambia el selector, pero existe el contenedor del app-id:
        container = soup.select_one('div[data-f2-app-id="mod-tearsheet-historical-prices"]')
        if container:
            app = container.select_one('div[data-mod-config]')
    if not app:
        return None

    raw = app.get("data-mod-config")
    if not raw:
        return None

    # data-mod-config suele venir con entidades HTML (&quot;)
    raw_unescaped = ihtml.unescape(raw).strip()
    try:
        cfg = json.loads(raw_unescaped)
        if isinstance(cfg, dict):
            return cfg
    except Exception:
        return None
    return None


def _date_chunks(start: date, end: date, chunk_days: int = 365) -> List[Tuple[date, date]]:
    chunks = []
    cur = start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=chunk_days - 1))
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


def _try_ajax_endpoints(
    session,
    symbol_numeric: str,
    start: date,
    end: date,
) -> Optional[str]:
    """
    Devuelve el campo 'html' del JSON, si existe.

    Hay precedentes de endpoints tipo:
    /data/equities/ajax/get-historical-prices?startDate=...&endDate=...&symbol=...
    que devuelven JSON con una clave 'html' para parsear [web:48].
    """
    endpoints = [
        "https://markets.ft.com/data/funds/ajax/get-historical-prices",
        "https://markets.ft.com/data/equities/ajax/get-historical-prices",
    ]

    params = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "symbol": str(symbol_numeric),
    }

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://markets.ft.com/",
    }

    for ep in endpoints:
        try:
            r = session.get(ep, params=params, headers=headers, timeout=25)
            log.debug(
                "FT AJAX: GET %s params=%s status=%s",
                ep,
                params,
                r.status_code,
            )
            if r.status_code != 200:
                continue
            try:
                payload = r.json()
            except Exception as e:
                sample = re.sub(r"\s+", " ", (r.text or "")[:400])
                log.debug("FT AJAX: JSON inválido (%s). Sample=%r", e, sample)
                continue

            if isinstance(payload, dict) and "html" in payload and payload["html"]:
                return payload["html"]

        except Exception as e:
            log.error("FT AJAX error ep=%s: %s", ep, e, exc_info=True)

    return None


def _parse_prices_html_fragment(html_fragment: str) -> List[Tuple[str, float]]:
    """
    El endpoint AJAX suele devolver 'html' con filas/tabla. Lo envolvemos en <table>.
    """
    soup = BeautifulSoup(f"<table>{html_fragment}</table>", "lxml")
    out: List[Tuple[str, float]] = []

    for i, tr in enumerate(soup.select("tr"), start=1):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        # Formato típico: Date, Open, High, Low, Close, Volume (Close suele ser índice 4) [web:48]
        date_raw = tds[0].get_text(" ", strip=True)
        close_raw = ""
        if len(tds) >= 5:
            close_raw = tds[4].get_text(" ", strip=True)
        else:
            # fallback: última columna numérica
            close_raw = tds[-1].get_text(" ", strip=True)

        log.debug("FT AJAX: Fila %s - Fecha raw: %r, Close raw: %r", i, date_raw, close_raw)

        try:
            d = parse_ft_date(date_raw)
            c = parse_float(close_raw)
            out.append((d, c))
        except Exception as e:
            log.debug("FT AJAX: No se pudo parsear fila %s (%s)", i, e)

    return out


def scrape_ft_prices_and_metadata(session, ft_symbol: str) -> Tuple[List[Tuple[str, float]], Dict]:
    """
    Estrategia robusta:
    1) Descargar tearsheet HTML.
    2) Extraer data-mod-config (symbol numérico + inception) del HistoricalPricesApp [file:42].
    3) Llamar al endpoint AJAX get-historical-prices por rangos y parsear el HTML del JSON [web:48].
    """
    meta: Dict = {"ft_symbol_requested": ft_symbol}
    symbols_to_try = _symbol_variants(ft_symbol)

    for sym in symbols_to_try:
        url = f"https://markets.ft.com/data/funds/tearsheet/historical?s={sym}"
        meta["url"] = url
        meta["ft_symbol_used"] = sym

        try:
            r = session.get(url, timeout=25)
            meta["status_code"] = r.status_code
            meta["final_url"] = str(getattr(r, "url", url))

            log.debug("FT: GET %s status=%s final_url=%s", url, r.status_code, meta["final_url"])

            if r.status_code != 200:
                continue

            html_text = r.text or ""
            soup = BeautifulSoup(html_text, "lxml")

            meta.update(_extract_tearsheet_metadata(soup, sym, url))

            cfg = _extract_historical_app_config(soup)
            if not cfg:
                sample = re.sub(r"\s+", " ", html_text[:700])
                log.debug("FT: No se encontró data-mod-config del HistoricalPricesApp. Sample=%r", sample)
                continue

            symbol_numeric = str(cfg.get("symbol", "")).strip()
            inception_raw = str(cfg.get("inception", "")).strip()

            if not symbol_numeric:
                log.debug("FT: data-mod-config sin 'symbol': %s", cfg)
                continue

            inception_dt = None
            if inception_raw:
                try:
                    # Ejemplo: 2013-10-29T00:00:00Z [file:42]
                    inception_dt = datetime.fromisoformat(inception_raw.replace("Z", "+00:00")).date()
                except Exception:
                    inception_dt = None

            start_date = inception_dt or (date.today() - timedelta(days=365 * 8))
            end_date = date.today()

            meta["symbol_numeric"] = symbol_numeric
            if inception_dt:
                meta["inception_date"] = inception_dt.isoformat()

            # Pedimos en chunks de 1 año para estabilidad/performance.
            collected: Dict[str, float] = {}

            for (s, e) in _date_chunks(start_date, end_date, chunk_days=365):
                frag = _try_ajax_endpoints(session, symbol_numeric, s, e)
                if not frag:
                    log.debug("FT AJAX: Sin html para rango %s..%s (symbol=%s)", s, e, symbol_numeric)
                    continue

                rows = _parse_prices_html_fragment(frag)
                for d, c in rows:
                    collected[d] = c  # upsert por fecha

            out = sorted(collected.items(), key=lambda x: x[0], reverse=False)
            if out:
                log.debug("FT: Total precios parseados=%s para %s (symbol=%s)", len(out), sym, symbol_numeric)
                return out, meta

            log.debug("FT: 0 precios tras AJAX para %s (symbol=%s). cfg=%s", sym, symbol_numeric, cfg)

        except Exception as e:
            log.error("FT error symbol=%s: %s", sym, e, exc_info=True)

    return [], meta
