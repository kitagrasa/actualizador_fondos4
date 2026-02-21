from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from ..utils import parse_float

log = logging.getLogger("scrapers.investing")

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

BLOCK_MARKERS = [
    "cdn-cgi/challenge-platform",
    "cf-turnstile",
    "challenges.cloudflare.com",
    "cfchl",
    "captcha",
    "verify you are human",
    "attention required",
]

RE_HIST_INFO  = re.compile(
    r"histDataExcessInfo\s*[=:]\s*\{[^}]*?pairId[\"'\s:=]+(?P<pair>\d{3,10})"
    r"(?:[^}]*?smlId[\"'\s:=]+(?P<sml>\d{5,12}))?",
    re.I | re.S,
)
RE_PAIRID     = re.compile(r'(?:pair|instrument)[_\-]?[Ii]d["\'\s:=]+(?P<id>\d{3,10})', re.I)
RE_DATA_PAIR  = re.compile(r'data-pair-id=["\'\s]*(?P<id>\d{3,10})', re.I)


# ── Helpers ────────────────────────────────────────────────────────────────

def _build_headers(domain: str, referer: str, accept: str = "text/html,*/*;q=0.8") -> Dict:
    return {
        "User-Agent":                UA,
        "Accept":                    accept,
        "Accept-Language":           "es-ES,es;q=0.9,en;q=0.8",
        "Referer":                   referer,
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def _fetch_html(session, url: str) -> Optional[str]:
    domain = urlparse(url).netloc or "es.investing.com"
    try:
        r = session.get(
            url,
            headers=_build_headers(domain, f"https://{domain}"),
            timeout=25,
            allow_redirects=True,
        )
        if r.status_code != 200:
            log.warning("Investing HTML status=%s url=%s", r.status_code, url)
            return None
        html = r.text or ""
        if len(html) < 1500:
            log.warning("Investing HTML demasiado corto len=%s url=%s", len(html), url)
            return None
        return html
    except Exception as e:
        log.error("Investing HTML error url=%s %s", url, e)
        return None


def _looks_blocked(html: str) -> bool:
    if not html:
        return True
    low = html.lower()
    return any(m in low for m in BLOCK_MARKERS)


def _extract_pair_sml(html: str) -> Tuple[Optional[str], Optional[str]]:
    if not html:
        return None, None
    m = RE_HIST_INFO.search(html)
    if m:
        return m.group("pair"), m.group("sml")
    m = RE_PAIRID.search(html)
    if m:
        return m.group("id"), None
    m = RE_DATA_PAIR.search(html)
    if m:
        return m.group("id"), None
    try:
        soup = BeautifulSoup(html, "lxml")
        node = soup.select_one("[data-pair-id]")
        if node:
            return node.get("data-pair-id"), None
    except Exception:
        pass
    return None, None


def _parse_html_table(html: str) -> List[Tuple[str, float]]:
    if not html:
        return []
    try:
        soup = BeautifulSoup(html, "lxml")
        table = (
            soup.select_one("table#curr_table")
            or soup.select_one("table.historicalTbl#curr_table")
            or soup.select_one("table.historicalTbl")
        )
        if not table:
            return []
        out: List[Tuple[str, float]] = []
        for tr in table.select("tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            diso: Optional[str] = None
            epoch = (tds[0].get("data-real-value") or "").strip()
            if epoch.isdigit():
                try:
                    diso = datetime.utcfromtimestamp(int(epoch)).date().isoformat()
                except Exception:
                    pass
            if not diso:
                try:
                    diso = datetime.strptime(tds[0].get_text(" ", strip=True), "%d.%m.%Y").date().isoformat()
                except Exception:
                    continue
            raw = tds[1].get("data-real-value") or tds[1].get_text(" ", strip=True)
            try:
                out.append((diso, parse_float(str(raw))))
            except Exception:
                continue
        out.sort(key=lambda x: x[0])
        return out
    except Exception as e:
        log.debug("Investing error parseando tabla HTML: %s", e)
        return []


def _post_ajax(
    session,
    investing_url: str,
    pairid: str,
    smlid: Optional[str],
    st: date,
    en: date,
) -> Optional[str]:
    domain = urlparse(investing_url).netloc or "es.investing.com"
    ajax_url = f"https://{domain}/instruments/HistoricalDataAjax"
    headers = _build_headers(domain, investing_url, "application/json, */*;q=0.01")
    headers.update({
        "Content-Type":    "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin":          f"https://{domain}",
    })
    data: Dict = {
        "curr_id":     pairid,
        "st_date":     st.strftime("%m/%d/%Y"),
        "end_date":    en.strftime("%m/%d/%Y"),
        "interval_sec": "Daily",
        "sort_col":    "date",
        "sort_ord":    "DESC",
        "action":      "historical_data",
    }
    if smlid and smlid.isdigit():
        data["smlID"] = smlid
        data["smlId"] = smlid
    try:
        r = session.post(ajax_url, data=data, headers=headers, timeout=25)
        if r.status_code != 200:
            log.debug("Investing AJAX status=%s pairid=%s", r.status_code, pairid)
            return None
        text = (r.text or "").strip()
        if not text:
            return None
        if text.startswith("{"):
            try:
                payload = json.loads(text)
                frag = payload.get("data") if isinstance(payload, dict) else None
                if isinstance(frag, str) and frag.strip():
                    return frag
            except Exception:
                pass
        return text
    except Exception as e:
        log.debug("Investing AJAX error pairid=%s %s", pairid, e)
        return None


def _parse_ajax_fragment(html_fragment: str) -> List[Tuple[str, float]]:
    if not html_fragment:
        return []
    try:
        soup = BeautifulSoup(
            f"<table><tbody>{html_fragment}</tbody></table>", "lxml"
        )
        out: List[Tuple[str, float]] = []
        for tr in soup.select("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            diso: Optional[str] = None
            epoch = (tds[0].get("data-real-value") or "").strip()
            if epoch.isdigit():
                try:
                    diso = datetime.utcfromtimestamp(int(epoch)).date().isoformat()
                except Exception:
                    pass
            if not diso:
                try:
                    diso = datetime.strptime(
                        tds[0].get_text(" ", strip=True), "%d.%m.%Y"
                    ).date().isoformat()
                except Exception:
                    continue
            raw = tds[1].get("data-real-value") or tds[1].get_text(" ", strip=True)
            try:
                out.append((diso, parse_float(str(raw))))
            except Exception:
                continue
        return out
    except Exception as e:
        log.debug("Investing error parseando fragmento AJAX: %s", e)
        return []


def _chunks_backward(end: date, chunk_days: int, max_chunks: int) -> List[Tuple[date, date]]:
    chunks: List[Tuple[date, date]] = []
    cur_end = end
    min_date = date(2000, 1, 1)
    for _ in range(max_chunks):
        cur_start = max(min_date, cur_end - timedelta(days=chunk_days - 1))
        chunks.append((cur_start, cur_end))
        if cur_start <= min_date:
            break
        cur_end = cur_start - timedelta(days=1)
    return chunks


# ── Entry point ────────────────────────────────────────────────────────────

def scrape_investing_prices(
    session,
    investing_url: str,
    cached_pairid: Optional[str] = None,
    cached_pair_id: Optional[str] = None,   # ← COMPATIBILIDAD AÑADIDA
    startdate: Optional[date] = None,
    enddate: Optional[date] = None,
    fullrefresh: bool = False,
) -> Tuple[List[Tuple[str, float]], Optional[str]]:
    """
    Scraper Investing.com robusto para GitHub Actions.
    Admite cached_pairid (nuevo) o cached_pair_id (antiguo).
    """
    # Unificamos el parámetro:
    effective_pairid = (cached_pairid or cached_pair_id or "").strip() or None

    if not investing_url:
        return [], None

    end = enddate or date.today()

    # 1. GET HTML
    html = _fetch_html(session, investing_url)
    blocked = _looks_blocked(html) if html else True
    if blocked and html:
        log.debug(
            "Investing HTML: markers de challenge detectados (len=%s) url=%s",
            len(html), investing_url,
        )

    # Extraer pairid: cache > HTML
    pairid = effective_pairid
    smlid: Optional[str] = None
    if not pairid and html:
        pairid, smlid = _extract_pair_sml(html)

    if not pairid:
        rows = _parse_html_table(html) if html else []
        if rows:
            log.info("Investing: %s precios tabla HTML (sin pairid) para %s", len(rows), investing_url)
        else:
            log.warning("Investing: sin pairid y sin tabla HTML para %s", investing_url)
        return rows, None

    # 2. AJAX
    if not fullrefresh:
        st = startdate or (end - timedelta(days=45))
        frag = _post_ajax(session, investing_url, pairid, smlid, st, end)
        rows = _parse_ajax_fragment(frag) if frag else []
        if rows:
            log.info("Investing: %s precios AJAX pairid=%s para %s", len(rows), pairid, investing_url)
            return rows, pairid
        
        rows = _parse_html_table(html) if html else []
        if rows:
            log.info("Investing: %s precios tabla HTML para %s", len(rows), investing_url)
        else:
            log.warning(
                "Investing: 0 precios (AJAX y tabla vacíos) para %s pairid=%s",
                investing_url, pairid,
            )
        return rows, pairid

    # 3. Full refresh
    CHUNK_DAYS = 365 * 3
    MAX_CHUNKS = 20
    collected: Dict[str, float] = {}
    empty_streak = 0

    for st, en_chunk in _chunks_backward(end, CHUNK_DAYS, MAX_CHUNKS):
        frag = _post_ajax(session, investing_url, pairid, smlid, st, en_chunk)
        rows = _parse_ajax_fragment(frag) if frag else []
        if rows:
            for d, c in rows:
                collected[d] = c
            empty_streak = 0
        else:
            empty_streak += 1
            if empty_streak >= 2:
                break
        time.sleep(0.15)

    if collected:
        out = sorted(collected.items())
        log.info(
            "Investing: %s precios AJAX full refresh pairid=%s para %s",
            len(out), pairid, investing_url,
        )
        return out, pairid

    rows = _parse_html_table(html) if html else []
    if rows:
        log.warning(
            "Investing: AJAX sin datos, usando tabla HTML inline (%s filas) para %s pairid=%s",
            len(rows), investing_url, pairid,
        )
    else:
        log.warning(
            "Investing: sin datos AJAX ni tabla HTML para %s pairid=%s",
            investing_url, pairid,
        )
    return rows, pairid
