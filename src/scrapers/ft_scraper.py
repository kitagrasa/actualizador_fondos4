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


def symbol_variants(ft_symbol: str) -> List[str]:
    sym = (ft_symbol or "").strip()
    if not sym:
        return []
    variants = [sym]
    if ":" in sym:
        variants.append(sym.replace(":", ""))
    else:
        m = re.match(r"(.+?)([A-Z]{3})$", sym)
        if m:
            variants.append(f"{m.group(1)}:{m.group(2)}")
    out, seen = [], set()
    for v in variants:
        if v not in seen:
            out.append(v)
            seen.add(v)
    return out


def extract_tearsheet_metadata(soup: BeautifulSoup, sym_used: str, url: str) -> Dict:
    meta: Dict = {"ft_symbol_used": sym_used, "url": url}
    h1 = soup.select_one(
        "h1.mod-tearsheet-overview__header__name,"
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


def extract_historical_app_config(soup: BeautifulSoup) -> Optional[Dict]:
    app = soup.select_one(
        "div[data-module-name='HistoricalPricesApp'][data-mod-config]"
    )
    if not app:
        container = soup.select_one(
            "div[data-f2-app-id='mod-tearsheet-historical-prices']"
        )
        if container:
            app = container.select_one("div[data-mod-config]")
    if not app:
        return None
    raw = app.get("data-mod-config")
    if not raw:
        return None
    raw_unescaped = ihtml.unescape(raw.strip())
    try:
        cfg = json.loads(raw_unescaped)
        return cfg if isinstance(cfg, dict) else None
    except Exception:
        return None


def to_ft_date_param(d: date) -> str:
    return d.strftime("%Y%m%d")


def date_chunks(start: date, end: date, chunk_days: int) -> List[Tuple[date, date]]:
    chunks = []
    cur = start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=chunk_days - 1))
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


def looks_like_full_html_document(text: str) -> bool:
    t = (text or "").lstrip().lower()
    return t.startswith("<!doctype html") or t.startswith("<html")


def fetch_ajax_html(session, symbol_numeric: str, start: date, end: date) -> Optional[str]:
    url = "https://markets.ft.com/data/equities/ajax/get-historical-prices"
    params = {
        "startDate": to_ft_date_param(start),
        "endDate":   to_ft_date_param(end),
        "symbol":    str(symbol_numeric),
    }
    headers = {
        "Accept":           "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer":          "https://markets.ft.com/",
    }
    try:
        r = session.get(url, params=params, headers=headers, timeout=25)
        log.debug("FT AJAX GET %s params=%s status=%s", url, params, r.status_code)
        if r.status_code != 200:
            return None
        if looks_like_full_html_document(r.text):
            # ── CORRECCIÓN: r"\s+" en vez de r"\\s+" ──
            sample = re.sub(r"\s+", " ", r.text or "")[:250]
            log.warning("FT AJAX: respuesta HTML (posible bloqueo). Sample: %r", sample)
            return None
        try:
            payload = r.json()
        except Exception as e:
            # ── CORRECCIÓN: r"\s+" en vez de r"\\s+" ──
            sample = re.sub(r"\s+", " ", r.text or "")[:250]
            log.warning("FT AJAX: JSON inválido %s. Sample: %r", e, sample)
            return None
        if isinstance(payload, dict) and payload.get("html"):
            return payload["html"]
        return None
    except Exception as e:
        log.error("FT AJAX error: %s", e)
        return None


def parse_prices_html_fragment(html_fragment: str) -> List[Tuple[str, float]]:
    soup = BeautifulSoup(f"<table>{html_fragment}</table>", "lxml")
    out: List[Tuple[str, float]] = []
    log_rows = os.getenv("FT_LOG_ROWS", "0").strip() == "1"
    for i, tr in enumerate(soup.select("tr"), start=1):
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        date_raw  = tds[0].get_text(" ", strip=True)
        close_raw = tds[4].get_text(" ", strip=True)
        if log_rows:
            log.debug("FT AJAX Fila %s - Fecha raw %r, Close raw %r", i, date_raw, close_raw)
        try:
            d = parse_ft_date(date_raw)
            c = parse_float(close_raw)
            out.append((d, c))
        except Exception as e:
            if log_rows:
                log.debug("FT AJAX No se pudo parsear fila %s: %s", i, e)
    return out


def scrape_ft_prices(
    session,
    ft_url: str,
    startdate: Optional[date] = None,
    enddate: Optional[date] = None,
    fullrefresh: bool = False,
) -> Tuple[List[Tuple[str, float]], Dict]:
    """
    Scraper FT incremental o full-refresh.
    - ft_url vacío → retorna silenciosamente sin warning.
    - fullrefresh=False → rango startdate..enddate (incremental)
    - fullrefresh=True  → backfill completo desde inception en chunks anuales
    Devuelve ([(YYYY-MM-DD, close)], metadata_dict)
    """
    # ── CORRECCIÓN: URL vacía → saltar silenciosamente ────────────────────
    if not ft_url:
        return [], {}
    # ─────────────────────────────────────────────────────────────────────

    meta: Dict = {"ft_url_requested": ft_url}
    symbols_to_try = symbol_variants(ft_url) if not ft_url.startswith("http") else []

    # Si ft_url es una URL completa, extraemos el símbolo del parámetro ?s=
    if ft_url.startswith("http"):
        m = re.search(r"[?&]s=([^&]+)", ft_url)
        if m:
            symbols_to_try = symbol_variants(m.group(1))
        else:
            symbols_to_try = []

    if not symbols_to_try:
        log.warning("FT: no se pudo extraer símbolo de %r", ft_url)
        return [], meta

    end = enddate or date.today()

    for sym in symbols_to_try:
        tearsheet_url = f"https://markets.ft.com/data/funds/tearsheet/historical?s={sym}"
        meta["url"] = tearsheet_url
        meta["ft_symbol_used"] = sym

        try:
            r = session.get(tearsheet_url, timeout=25)
            meta["status_code"] = r.status_code
            meta["final_url"] = str(getattr(r, "url", tearsheet_url))
            log.debug("FT GET %s status=%s final_url=%s",
                      tearsheet_url, r.status_code, meta["final_url"])
            if r.status_code != 200:
                continue

            soup = BeautifulSoup(r.text or "", "lxml")
            meta.update(extract_tearsheet_metadata(soup, sym, tearsheet_url))

            cfg = extract_historical_app_config(soup)
            if not cfg:
                log.warning("FT: no se encontró HistoricalPricesApp[data-mod-config] "
                            "(DOM cambió o bloqueo).")
                continue

            symbol_numeric = str(cfg.get("symbol", "")).strip()
            inception_raw  = str(cfg.get("inception", "")).strip()
            if not symbol_numeric:
                log.warning("FT: data-mod-config sin 'symbol': %s", cfg)
                continue

            inception_dt: Optional[date] = None
            if inception_raw:
                try:
                    inception_dt = datetime.fromisoformat(
                        inception_raw.replace("Z", "+00:00")
                    ).date()
                except Exception:
                    inception_dt = None

            meta["symbol_numeric"] = symbol_numeric
            if inception_dt:
                meta["inception_date"] = inception_dt.isoformat()

            # ── Rango de fechas ──────────────────────────────────────────────
            if fullrefresh:
                start = inception_dt or (date.today() - timedelta(days=365 * 8))
                chunks = date_chunks(start, end, chunk_days=365)
            else:
                start = startdate or (end - timedelta(days=45))
                chunks = [(start, end)]

            collected: Dict[str, float] = {}
            for s, e_chunk in chunks:
                frag = fetch_ajax_html(session, symbol_numeric, s, e_chunk)
                if not frag:
                    continue
                rows = parse_prices_html_fragment(frag)
                for d_iso, close in rows:
                    collected[d_iso] = close
                if fullrefresh:
                    time.sleep(0.25)

            if collected:
                prices = sorted(collected.items(), key=lambda x: x[0])
                log.debug("FT: %s precios recibidos (fullrefresh=%s)", len(prices), fullrefresh)
                return prices, meta

            log.warning("FT: 0 precios para symbol_numeric=%s. "
                        "Posible bloqueo o endpoint cambió.", symbol_numeric)

        except Exception as e:
            log.error("FT error symbol=%s: %s", sym, e, exc_info=True)

    return [], meta
