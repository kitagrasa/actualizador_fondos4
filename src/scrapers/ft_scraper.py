from __future__ import annotations

import html as ihtml
import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from ..utils import parse_float, parse_ft_date

log = logging.getLogger("scrapers.ft_scraper")


def _normalize_ft_url(ft_url: str) -> str:
    """
    Auto-corrige URLs de FT que apuntan a summary en vez de historical.
    Ej: /tearsheet/summary? → /tearsheet/historical?
    """
    return re.sub(r"/tearsheet/[^/?]+\?", "/tearsheet/historical?", ft_url)


def _extract_symbol(ft_url: str) -> Optional[str]:
    """Extrae ?s=SYMBOL de la URL de FT."""
    try:
        sym = parse_qs(urlparse(ft_url).query).get("s", [None])[0]
        return sym.strip() if sym else None
    except Exception:
        return None


def _extract_metadata(soup: BeautifulSoup, ft_url: str) -> Dict:
    meta: Dict = {"url": ft_url}
    h1 = soup.select_one(
        "h1.mod-tearsheet-overview__header__name,"
        "h1.mod-tearsheet-overview__header__name--large"
    )
    if h1:
        meta["name"] = h1.get_text(" ", strip=True)
    sym = _extract_symbol(ft_url)
    if sym:
        m = re.search(r":([A-Z]{3})$", sym)
        if m:
            meta["currency"] = m.group(1)
    return meta


def _extract_app_config(soup: BeautifulSoup) -> Optional[Dict]:
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


def _to_date_param(d: date) -> str:
    return d.strftime("%Y/%m/%d")


def _date_chunks(start: date, end: date, chunk_days: int = 365) -> List[Tuple[date, date]]:
    chunks, cur = [], start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=chunk_days - 1))
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


def _fetch_ajax_html(session, symbol_numeric: str, start: date, end: date) -> Optional[str]:
    url = "https://markets.ft.com/data/equities/ajax/get-historical-prices"
    params = {"startDate": _to_date_param(start), "endDate": _to_date_param(end), "symbol": symbol_numeric}
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://markets.ft.com/",
    }
    try:
        r = session.get(url, params=params, headers=headers, timeout=25)
        log.debug("FT AJAX: status=%s symbol=%s %s..%s", r.status_code, symbol_numeric, start, end)
        if r.status_code != 200:
            return None
        t = (r.text or "").lstrip().lower()
        if t.startswith("<!doctype") or t.startswith("<html"):
            log.warning("FT AJAX: respuesta HTML (bloqueo?). symbol=%s", symbol_numeric)
            return None
        payload = r.json()
        return payload.get("html") if isinstance(payload, dict) else None
    except Exception as e:
        log.warning("FT AJAX error: %s", e)
        return None


def _parse_fragment(html_fragment: str) -> List[Tuple[str, float]]:
    soup = BeautifulSoup(f"<table>{html_fragment}</table>", "lxml")
    log_rows = os.getenv("FT_LOG_ROWS", "0").strip() == "1"
    out: List[Tuple[str, float]] = []
    for i, tr in enumerate(soup.select("tr"), 1):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        date_raw = tds[0].get_text(" ", strip=True)
        close_raw = tds[4].get_text(" ", strip=True)
        if log_rows:
            log.debug("FT: Fila %s - %r / %r", i, date_raw, close_raw)
        try:
            out.append((parse_ft_date(date_raw), parse_float(close_raw)))
        except Exception as e:
            if log_rows:
                log.debug("FT: Fila %s no parseable: %s", i, e)
    return out


def scrape_ft_prices(
    session,
    ft_url: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> Tuple[List[Tuple[str, float]], Dict]:
    """Acepta la URL completa de FT. Auto-corrige summary→historical. Devuelve (prices, meta)."""
    meta: Dict = {"url": ft_url}
    if not ft_url:
        return [], meta

    # Auto-corrección: summary → historical
    ft_url_normalized = _normalize_ft_url(ft_url)
    if ft_url_normalized != ft_url:
        log.info("FT: URL corregida automáticamente: %s → %s", ft_url, ft_url_normalized)
    ft_url = ft_url_normalized
    meta["url"] = ft_url

    end = end_date or date.today()
    try:
        r = session.get(ft_url, timeout=25)
        meta["status_code"] = r.status_code
        log.debug("FT: GET %s status=%s", ft_url, r.status_code)
        if r.status_code != 200:
            log.warning("FT: status=%s url=%s", r.status_code, ft_url)
            return [], meta

        soup = BeautifulSoup(r.text or "", "lxml")
        meta.update(_extract_metadata(soup, ft_url))

        cfg = _extract_app_config(soup)
        if not cfg:
            log.warning("FT: No se encontró HistoricalPricesApp en %s", ft_url)
            return [], meta

        symbol_numeric = str(cfg.get("symbol", "")).strip()
        if not symbol_numeric:
            log.warning("FT: data-mod-config sin 'symbol': %s", cfg)
            return [], meta

        meta["symbol_numeric"] = symbol_numeric

        inception_dt: Optional[date] = None
        inception_raw = str(cfg.get("inception", "")).strip()
        if inception_raw:
            try:
                inception_dt = datetime.fromisoformat(inception_raw.replace("Z", "+00:00")).date()
                meta["inception_date"] = inception_dt.isoformat()
            except Exception:
                pass

        if full_refresh:
            start = inception_dt or (date.today() - timedelta(days=365 * 10))
            chunks = _date_chunks(start, end)
        else:
            start = start_date or (end - timedelta(days=45))
            chunks = [(start, end)]

        collected: Dict[str, float] = {}
        for s, e in chunks:
            frag = _fetch_ajax_html(session, symbol_numeric, s, e)
            if frag:
                for d, c in _parse_fragment(frag):
                    collected[d] = c
            if full_refresh:
                time.sleep(0.2)

        if collected:
            prices = sorted(collected.items())
            log.debug("FT: %s precios (full_refresh=%s) para %s", len(prices), full_refresh, ft_url)
            return prices, meta

        log.warning("FT: 0 precios desde %s (symbol=%s)", ft_url, symbol_numeric)

    except Exception as e:
        log.error("FT error url=%s: %s", ft_url, e, exc_info=True)

    return [], meta
