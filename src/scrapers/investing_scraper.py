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

# ── Regexes (backslashes corregidos) ──────────────────────────────────────
RE_HIST_INFO = re.compile(
    r"histDataExcessInfo\s*[=:]\s*\{[^}]*?pairId[\"'\s:=]+(?P<pair>\d{3,10})"
    r"(?:[^}]*?smlId[\"'\s:=]+(?P<sml>\d{5,12}))?",
    re.I | re.S,
)
RE_PAIRID    = re.compile(r'(?:pair|instrument)[_\-]?id["\'\s:=]+(?P<id>\d{3,10})', re.I)
RE_DATA_PAIR = re.compile(r'data-pair-id=["\']*(?P<id>\d{3,10})', re.I)

# ── TVC hosts (más fiables en GitHub Actions que AJAX) ────────────────────
TVC_HOSTS = ["tvc6.investing.com", "tvc4.investing.com", "tvc2.investing.com"]


# ── Helpers generales ──────────────────────────────────────────────────────

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


# ── Parseo tabla HTML (fallback visual) ────────────────────────────────────

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
        out.sort(key=lambda x: x[0])
        return out
    except Exception as e:
        log.debug("Investing error parseando tabla HTML: %s", e)
        return []


# ── AJAX (/instruments/HistoricalDataAjax) ─────────────────────────────────

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
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin":           f"https://{domain}",
    })
    data: Dict = {
        "curr_id":      pairid,
        "st_date":      st.strftime("%m/%d/%Y"),
        "end_date":     en.strftime("%m/%d/%Y"),
        "interval_sec": "Daily",
        "sort_col":     "date",
        "sort_ord":     "DESC",
        "action":       "historical_data",
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


# ── TVC (tvc6/tvc4/tvc2.investing.com/history) ────────────────────────────

def _date_to_ts(d: date) -> int:
    """Fin del día en UNIX timestamp."""
    return int(datetime(d.year, d.month, d.day, 23, 59, 59).timestamp())


def _date_from_ts(d: date) -> int:
    """Inicio del día en UNIX timestamp."""
    return int(datetime(d.year, d.month, d.day, 0, 0, 0).timestamp())


def _fetch_tvc(
    session,
    pairid: str,
    start_date: date,
    end_date: date,
    referer: str,
) -> List[Tuple[str, float]]:
    """
    Obtiene histórico via TVC (más fiable que AJAX en GitHub Actions).
    Prueba tvc6 → tvc4 → tvc2 en orden.
    Respuesta JSON: {s:"ok", t:[ts,...], c:[close,...]}
    """
    from_ts = _date_from_ts(start_date)
    to_ts   = _date_to_ts(end_date)
    ts_now  = int(time.time())

    headers = {
        "User-Agent":      UA,
        "Accept":          "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Origin":          "https://es.investing.com",
        "Referer":         referer,
        "Connection":      "keep-alive",
    }

    for host in TVC_HOSTS:
        url = (
            f"https://{host}/{ts_now}/{ts_now}/1/1/8/history"
            f"?symbol={pairid}&resolution=D&from={from_ts}&to={to_ts}"
        )
        try:
            r = session.get(url, headers=headers, timeout=20)
            if r.status_code != 200:
                log.debug("TVC %s status=%s pairid=%s", host, r.status_code, pairid)
                continue
            data = r.json()
            if not isinstance(data, dict):
                continue
            status = data.get("s", "")
            if status == "no_data":
                log.debug("TVC no_data host=%s pairid=%s", host, pairid)
                return []
            if status != "ok":
                log.debug("TVC status inesperado=%s host=%s pairid=%s", status, host, pairid)
                continue
            t_list = data.get("t") or []
            c_list = data.get("c") or []
            if not t_list or not c_list:
                continue
            out: List[Tuple[str, float]] = []
            for ts, close in zip(t_list, c_list):
                try:
                    d = datetime.utcfromtimestamp(int(ts)).date().isoformat()
                    out.append((d, float(close)))
                except Exception:
                    continue
            if out:
                out.sort(key=lambda x: x[0])
                log.info(
                    "TVC: %s precios (%s→%s) pairid=%s host=%s",
                    len(out), out[0][0], out[-1][0], pairid, host,
                )
                return out
        except Exception as e:
            log.debug("TVC error host=%s pairid=%s %s", host, pairid, e)
            continue

    log.warning("TVC: sin datos en ningún host para pairid=%s", pairid)
    return []


def _fetch_tvc_full(
    session,
    pairid: str,
    end_date: date,
    referer: str,
) -> List[Tuple[str, float]]:
    """
    Obtiene todo el histórico disponible via TVC.
    1) Intenta una sola petición desde 2000-01-01 (muchos instrumentos lo devuelven todo).
    2) Si no, chunking de 3 años hacia atrás hasta 2 chunks vacíos consecutivos.
    """
    min_date = date(2000, 1, 1)

    # Intento único con rango completo
    rows = _fetch_tvc(session, pairid, min_date, end_date, referer)
    if rows:
        return rows

    # Chunking hacia atrás
    collected: Dict[str, float] = {}
    cur_end = end_date
    chunk_days = 365 * 3
    empty_streak = 0

    while cur_end > min_date and empty_streak < 2:
        cur_start = max(min_date, cur_end - timedelta(days=chunk_days - 1))
        chunk_rows = _fetch_tvc(session, pairid, cur_start, cur_end, referer)
        if chunk_rows:
            for d, c in chunk_rows:
                collected[d] = c
            empty_streak = 0
            earliest = min(chunk_rows, key=lambda x: x[0])[0]
            if earliest > cur_start.isoformat():
                cur_end = datetime.strptime(earliest, "%Y-%m-%d").date() - timedelta(days=1)
            else:
                cur_end = cur_start - timedelta(days=1)
        else:
            empty_streak += 1
            cur_end = cur_start - timedelta(days=1)
        time.sleep(0.15)

    return sorted(collected.items()) if collected else []


# ── Entry point ────────────────────────────────────────────────────────────

def scrape_investing_prices(
    session,
    investing_url: str,
    cached_pairid: Optional[str] = None,
    cached_pair_id: Optional[str] = None,   # compatibilidad con app.py antiguo
    startdate: Optional[date] = None,
    enddate: Optional[date] = None,
    fullrefresh: bool = False,
) -> Tuple[List[Tuple[str, float]], Optional[str]]:
    """
    Scraper Investing.com — 3 capas en orden de fiabilidad:
      1) /instruments/HistoricalDataAjax  (rápido; a veces bloqueado en Actions)
      2) tvc6.investing.com/history       (TVC; más fiable en GitHub Actions)
      3) tabla #curr_table del HTML       (último recurso; solo datos visibles)
    """
    effective_pairid = (cached_pairid or cached_pair_id or "").strip() or None

    if not investing_url:
        return [], None

    end = enddate or date.today()

    # ── GET HTML ─────────────────────────────────────────────────────────
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
            log.info(
                "Investing: %s precios tabla HTML (sin pairid) para %s",
                len(rows), investing_url,
            )
        else:
            log.warning("Investing: sin pairid y sin tabla HTML para %s", investing_url)
        return rows, None

    # ── MODO INCREMENTAL ─────────────────────────────────────────────────
    if not fullrefresh:
        st = startdate or (end - timedelta(days=45))

        # Capa 1: AJAX
        frag = _post_ajax(session, investing_url, pairid, smlid, st, end)
        rows = _parse_ajax_fragment(frag) if frag else []
        if rows:
            log.info(
                "Investing: %s precios AJAX pairid=%s para %s",
                len(rows), pairid, investing_url,
            )
            return rows, pairid

        # Capa 2: TVC
        rows = _fetch_tvc(session, pairid, st, end, investing_url)
        if rows:
            return rows, pairid

        # Capa 3: tabla HTML
        rows = _parse_html_table(html) if html else []
        if rows:
            log.info(
                "Investing: %s precios tabla HTML para %s", len(rows), investing_url,
            )
        else:
            log.warning(
                "Investing: 0 precios (AJAX+TVC+tabla vacíos) para %s pairid=%s",
                investing_url, pairid,
            )
        return rows, pairid

    # ── MODO FULL REFRESH ────────────────────────────────────────────────
    CHUNK_DAYS = 365 * 3
    MAX_CHUNKS = 20
    collected: Dict[str, float] = {}
    ajax_ok = False
    empty_streak = 0

    for st, en_chunk in _chunks_backward(end, CHUNK_DAYS, MAX_CHUNKS):
        frag = _post_ajax(session, investing_url, pairid, smlid, st, en_chunk)
        rows = _parse_ajax_fragment(frag) if frag else []
        if rows:
            for d, c in rows:
                collected[d] = c
            ajax_ok = True
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

    # AJAX bloqueado → TVC full refresh
    if not ajax_ok:
        log.info(
            "Investing: AJAX bloqueado → TVC full refresh pairid=%s", pairid,
        )
        rows = _fetch_tvc_full(session, pairid, end, investing_url)
        if rows:
            return rows, pairid

    # Último fallback: tabla HTML
    rows = _parse_html_table(html) if html else []
    if rows:
        log.warning(
            "Investing: AJAX+TVC sin datos, tabla HTML inline (%s filas) para %s pairid=%s",
            len(rows), investing_url, pairid,
        )
    else:
        log.warning(
            "Investing: sin datos en ninguna capa para %s pairid=%s",
            investing_url, pairid,
        )
    return rows, pairid
