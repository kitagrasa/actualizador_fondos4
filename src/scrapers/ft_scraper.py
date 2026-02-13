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


def _symbol_variants(ft_symbol: str) -> List[str]:
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
    # En FT suele existir HistoricalPricesApp con data-mod-config escapado en HTML [file:42]
    app = soup.select_one('div[data-module-name="HistoricalPricesApp"][data-mod-config]')
    if not app:
        container = soup.select_one('div[data-f2-app-id="mod-tearsheet-historical-prices"]')
        if container:
            app = container.select_one('div[data-mod-config]')
    if not app:
        return None

    raw = app.get("data-mod-config")
    if not raw:
        return None

    raw_unescaped = ihtml.unescape(raw).strip()
    try:
        cfg = json.loads(raw_unescaped)
        return cfg if isinstance(cfg, dict) else None
    except Exception:
        return None


def _to_ft_date_param(d: date) -> str:
    # Formato que FT suele aceptar en el AJAX: YYYY/MM/DD [web:48]
    return d.strftime("%Y/%m/%d")


def _date_chunks(start: date, end: date, chunk_days: int) -> List[Tuple[date, date]]:
    chunks = []
    cur = start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=chunk_days - 1))
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


def _looks_like_full_html_document(text: str) -> bool:
    t = (text or "").lstrip().lower()
    return t.startswith("<!doctype html") or t.startswith("<html")


def _fetch_ajax_html(session, symbol_numeric: str, start: date, end: date) -> Optional[str]:
    """
    Endpoint AJAX público/observado para obtener histórico: devuelve JSON con una clave 'html'. [web:48]
    """
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

    r = session.get(url, params=params, headers=headers, timeout=25)
    log.debug("FT AJAX: GET %s params=%s status=%s", url, params, r.status_code)

    if r.status_code != 200:
        return None

    # Si te devuelven HTML completo en vez de JSON, es típico de bloqueo/antibot.
    if _looks_like_full_html_document(r.text):
        sample = re.sub(r"\s+", " ", (r.text or "")[:250])
        log.warning("FT AJAX: Respuesta HTML (posible bloqueo). Sample=%r", sample)
        return None

    try:
        payload = r.json()
    except Exception as e:
        sample = re.sub(r"\s+", " ", (r.text or "")[:250])
        log.warning("FT AJAX: JSON inválido (%s). Sample=%r", e, sample)
        return None

    if isinstance(payload, dict) and payload.get("html"):
        return payload["html"]

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


def scrape_ft_prices_and_metadata(
    session,
    ft_symbol: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> Tuple[List[Tuple[str, float]], Dict]:
    """
    - full_refresh=True: backfill completo (troceado por años).
    - full_refresh=False: incremental (un rango único start_date..end_date).
    """
    meta: Dict = {"ft_symbol_requested": ft_symbol}

    symbols_to_try = _symbol_variants(ft_symbol)
    if not symbols_to_try:
        return [], meta

    end = end_date or date.today()

    for sym in symbols_to_try:
        tearsheet_url = f"https://markets.ft.com/data/funds/tearsheet/historical?s={sym}"
        meta["url"] = tearsheet_url
        meta["ft_symbol_used"] = sym

        try:
            r = session.get(tearsheet_url, timeout=25)
            meta["status_code"] = r.status_code
            meta["final_url"] = str(getattr(r, "url", tearsheet_url))
            log.debug("FT: GET %s status=%s final_url=%s", tearsheet_url, r.status_code, meta["final_url"])

            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text or "", "lxml")
            meta.update(_extract_tearsheet_metadata(soup, sym, tearsheet_url))

            cfg = _extract_historical_app_config(soup)
            if not cfg:
                log.warning("FT: No se encontró HistoricalPricesApp/data-mod-config (DOM cambió o bloqueo).")
                continue

            symbol_numeric = str(cfg.get("symbol", "")).strip()
            inception_raw = str(cfg.get("inception", "")).strip()
            if not symbol_numeric:
                log.warning("FT: data-mod-config sin 'symbol': %s", cfg)
                continue

            inception_dt: Optional[date] = None
            if inception_raw:
                try:
                    inception_dt = datetime.fromisoformat(inception_raw.replace("Z", "+00:00")).date()
                except Exception:
                    inception_dt = None

            meta["symbol_numeric"] = symbol_numeric
            if inception_dt:
                meta["inception_date"] = inception_dt.isoformat()

            # Rango
            if full_refresh:
                start = inception_dt or (date.today() - timedelta(days=365 * 8))
                chunks = _date_chunks(start, end, chunk_days=365)
            else:
                start = start_date or (end - timedelta(days=45))
                chunks = [(start, end)]

            collected: Dict[str, float] = {}
            for (s, e) in chunks:
                frag = _fetch_ajax_html(session, symbol_numeric, s, e)
                if not frag:
                    continue

                rows = _parse_prices_html_fragment(frag)
                for d, c in rows:
                    collected[d] = c

                if full_refresh:
                    time.sleep(0.25)

            if collected:
                prices = sorted(collected.items(), key=lambda x: x[0])  # ascendente
                log.debug("FT: precios recibidos=%s (full_refresh=%s)", len(prices), full_refresh)
                return prices, meta

            log.warning("FT: 0 precios (symbol=%s). Puede ser bloqueo, o endpoint cambió.", symbol_numeric)

        except Exception as e:
            log.error("FT error symbol=%s: %s", sym, e, exc_info=True)

    return [], meta
