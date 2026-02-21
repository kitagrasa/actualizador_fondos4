from __future__ import annotations

import logging
import re
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from ..utils import parse_float

log = logging.getLogger("scrapers.investing")

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

# Señales fuertes de challenge (best-effort)
_BLOCK_MARKERS = [
    "/cdn-cgi/challenge-platform",
    "cf-turnstile",
    "challenges.cloudflare.com",
    "cf_chl_",
    "verify you are human",
    "attention required",
    "captcha",
]


def _looks_blocked(html: str) -> bool:
    if not html:
        return True
    low = html.lower()
    return any(m in low for m in _BLOCK_MARKERS)


def _fetch_html(session, investing_url: str) -> Optional[str]:
    domain = urlparse(investing_url).netloc or "www.investing.com"
    headers = {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Referer": f"https://{domain}/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        r = session.get(investing_url, headers=headers, timeout=30, allow_redirects=True)
        if r.status_code != 200:
            log.warning("Investing HTML: status=%s url=%s", r.status_code, investing_url)
            return None

        html = r.text or ""
        if len(html) < 1500:
            log.warning("Investing HTML: demasiado corto (len=%s) url=%s", len(html), investing_url)
            return None

        # No “castigamos” si hay markers pero viene la tabla. Solo warning si parece challenge Y no hay currtable.
        has_currtable_hint = ('id="currtable"' in html) or ("id=currtable" in html)
        if _looks_blocked(html) and not has_currtable_hint:
            log.warning(
                "Investing HTML: parece bloqueado/challenge url=%s (len=%s)",
                investing_url, len(html),
            )

        return html
    except Exception as e:
        log.error("Investing HTML error url=%s: %s", investing_url, e)
        return None


# ── Extraer pair_id / sml_id desde el HTML (para cache / AJAX) ───────────────
# IMPORTANTE: regex SIN doble-escape; el HTML real incluye:
#   window.histDataExcessInfo pairId 1036800, smlId 25737662
# [file:299]

_RE_HISTINFO = re.compile(
    r"histDataExcessInfo.*?pairId\s*(?:[:=]\s*)?(?P<pair>\d{3,10}).*?smlId\s*(?:[:=]\s*)?(?P<sml>\d{3,12})",
    re.I | re.S,
)
_RE_PAIRID_GENERIC = re.compile(r"\bpairId\b\s*(?:[:=]\s*)?(\d{3,10})", re.I)
_RE_DATA_PAIR_ID = re.compile(r"\bdata-pair-id\b\s*=\s*['\"]?(\d{3,10})", re.I)


def _extract_pair_and_sml(html: str) -> Tuple[Optional[str], Optional[str]]:
    if not html:
        return None, None

    m = _RE_HISTINFO.search(html)
    if m:
        return m.group("pair"), m.group("sml")

    m2 = _RE_PAIRID_GENERIC.search(html)
    if m2:
        return m2.group(1), None

    m3 = _RE_DATA_PAIR_ID.search(html)
    if m3:
        return m3.group(1), None

    return None, None


# ── Parse de tabla HTML (#currtable) ─────────────────────────────────────────
# En el HTML real: table#currtable + td[0] epoch + td[1] último con data-real-value [file:299]

def _parse_currtable_from_soup(soup: BeautifulSoup) -> List[Tuple[str, float]]:
    table = soup.select_one("table#currtable") or soup.select_one("table.historicalTbl#currtable")
    if not table:
        return []

    out: List[Tuple[str, float]] = []
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        # Fecha
        d_iso: Optional[str] = None
        epoch = (tds[0].get("data-real-value") or "").strip()
        if epoch.isdigit():
            try:
                d_iso = datetime.utcfromtimestamp(int(epoch)).date().isoformat()
            except Exception:
                d_iso = None
        if not d_iso:
            try:
                d_iso = datetime.strptime(tds[0].get_text(strip=True), "%d.%m.%Y").date().isoformat()
            except Exception:
                continue

        # Último
        raw = tds[1].get("data-real-value") or tds[1].get_text(strip=True)
        try:
            out.append((d_iso, parse_float(str(raw))))
        except Exception:
            continue

    out.sort(key=lambda x: x[0])
    return out


def _filter_range(rows: List[Tuple[str, float]], start: date, end: date) -> List[Tuple[str, float]]:
    if not rows:
        return []
    out: List[Tuple[str, float]] = []
    for d_iso, close in rows:
        try:
            d = datetime.strptime(d_iso, "%Y-%m-%d").date()
        except Exception:
            continue
        if start <= d <= end:
            out.append((d_iso, close))
    return out


# ── HistoricalDataAjax (puede dar 403 en Actions) ────────────────────────────

def _ajax_post(
    session,
    pair_id: str,
    st: date,
    en: date,
    referer: str,
    sml_id: Optional[str] = None,
) -> Optional[str]:
    domain = urlparse(referer).netloc or "es.investing.com"
    url = f"https://{domain}/instruments/HistoricalDataAjax"

    headers = {
        "User-Agent": _UA,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": f"https://{domain}",
        "Referer": referer,
        "Connection": "keep-alive",
    }

    data = {
        "curr_id": pair_id,
        "st_date": st.strftime("%m/%d/%Y"),
        "end_date": en.strftime("%m/%d/%Y"),
        "interval_sec": "Daily",
        "sort_col": "date",
        "sort_ord": "DESC",
        "action": "historical_data",
    }
    if sml_id and sml_id.isdigit():
        data["smlID"] = sml_id
        data["smlId"] = sml_id

    try:
        r = session.post(url, data=data, headers=headers, timeout=30)
        if r.status_code != 200:
            log.warning("Investing AJAX: status=%s pair_id=%s", r.status_code, pair_id)
            return None

        payload = r.json()
        if isinstance(payload, dict):
            frag = payload.get("data", "")
            if frag and len(frag) > 10:
                return frag
        return None
    except Exception as e:
        log.debug("Investing AJAX error pair_id=%s: %s", pair_id, e)
        return None


def _parse_ajax_fragment(html_fragment: str) -> List[Tuple[str, float]]:
    if not html_fragment:
        return []
    try:
        soup = BeautifulSoup(f"<table><tbody>{html_fragment}</tbody></table>", "lxml")
        out: List[Tuple[str, float]] = []
        for tr in soup.select("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            d_iso: Optional[str] = None
            epoch = (tds[0].get("data-real-value") or "").strip()
            if epoch.isdigit():
                try:
                    d_iso = datetime.utcfromtimestamp(int(epoch)).date().isoformat()
                except Exception:
                    d_iso = None
            if not d_iso:
                try:
                    d_iso = datetime.strptime(tds[0].get_text(strip=True), "%d.%m.%Y").date().isoformat()
                except Exception:
                    continue

            raw = tds[1].get("data-real-value") or tds[1].get_text(strip=True)
            try:
                out.append((d_iso, parse_float(str(raw))))
            except Exception:
                continue

        return out
    except Exception as e:
        log.debug("Investing: error parseando fragmento AJAX: %s", e)
        return []


def _date_chunks(start: date, end: date, months: int) -> List[Tuple[date, date]]:
    chunks: List[Tuple[date, date]] = []
    cur = start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=months * 30))
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


# ── Entry point ───────────────────────────────────────────────────────────────

def scrape_investing_prices(
    session,
    investing_url: str,
    cached_pair_id: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> Tuple[List[Tuple[str, float]], Optional[str]]:
    """
    - Incremental: usa tabla HTML (#currtable) y listo.
    - Full refresh: intenta AJAX; si falla (403), vuelve a tabla HTML.
    """
    if not investing_url:
        return [], None

    end = end_date or date.today()
    start = date(2000, 1, 1) if full_refresh else (start_date or (end - timedelta(days=45)))

    html = _fetch_html(session, investing_url)
    if not html:
        return [], None

    # Una sola pasada de parseo
    soup = BeautifulSoup(html, "lxml")

    # 1) Tabla HTML (más estable en Actions)
    table_rows = _parse_currtable_from_soup(soup)
    table_rows = _filter_range(table_rows, start, end)

    # 2) pair_id/sml_id para cache / AJAX
    pair_id_html, sml_id = _extract_pair_and_sml(html)
    pair_id = (cached_pair_id or "").strip() or pair_id_html

    if table_rows and not full_refresh:
        log.info("Investing: %s precios (tabla HTML) para %s", len(table_rows), investing_url)
        return table_rows, pair_id

    # 3) Full refresh (best-effort)
    if full_refresh and pair_id and pair_id.isdigit():
        collected: Dict[str, float] = {}
        for s, e in _date_chunks(start, end, months=12):
            frag = _ajax_post(session, pair_id, s, e, investing_url, sml_id=sml_id)
            if frag:
                for d, c in _parse_ajax_fragment(frag):
                    collected[d] = c
            time.sleep(0.2)

        if collected:
            out = sorted(collected.items(), key=lambda x: x[0])
            log.info("Investing: %s precios (AJAX) pair_id=%s para %s", len(out), pair_id, investing_url)
            return out, pair_id

        if table_rows:
            log.warning(
                "Investing: AJAX sin datos (posible 403/bloqueo). Usando tabla HTML (%s filas) para %s",
                len(table_rows), investing_url,
            )
            return table_rows, pair_id

        log.warning("Investing: sin datos por AJAX ni tabla HTML para %s (pair_id=%s).", investing_url, pair_id)
        return [], pair_id

    # Si no hay tabla y no podemos/queremos AJAX
    if not table_rows:
        log.warning("Investing: tabla HTML vacía/no encontrada para %s", investing_url)
        return [], pair_id

    return table_rows, pair_id
