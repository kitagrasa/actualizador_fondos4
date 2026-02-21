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

# Señales "fuertes" de challenge (best-effort).
# OJO: Investing a veces incluye cadenas “sospechosas” aunque el HTML traiga datos;
# por eso solo hacemos WARNING si NO hay señales claras de tabla/datos.
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

        # Evitar falsos positivos: si hay indicios claros de tabla/datos, no hagas WARNING aunque haya markers.
        low = html.lower()
        has_useful_hint = (
            ("currtable" in low)
            or ("historicaltbl" in low)
            or ("data-real-value" in low)
            or ("data-pair-id" in low)
        )

        if _looks_blocked(html) and not has_useful_hint:
            log.warning(
                "Investing HTML: parece bloqueado/challenge url=%s (len=%s)",
                investing_url, len(html),
            )

        return html
    except Exception as e:
        log.error("Investing HTML error url=%s: %s", investing_url, e)
        return None


# ── Extraer pair_id / sml_id ────────────────────────────────────────────────

_RE_HISTINFO = re.compile(
    r"histDataExcessInfo.*?pairId\s*(?:[:=]\s*)?(?P<pair>\d{3,10}).*?smlId\s*(?:[:=]\s*)?(?P<sml>\d{3,12})",
    re.I | re.S,
)
_RE_PAIRID_GENERIC = re.compile(r"\bpairId\b\s*(?:[:=]\s*)?(\d{3,10})", re.I)
_RE_DATALAYER_INSTRUMENTID = re.compile(r"\binstrumentid\s*(?:[:=]\s*)?(\d{3,12})", re.I)
_RE_DATA_PAIR_ID = re.compile(r"\bdata-pair-id\b\s*=\s*['\"]?(\d{3,10})", re.I)


def _extract_pair_and_sml(html: str, soup: Optional[BeautifulSoup] = None) -> Tuple[Optional[str], Optional[str]]:
    if not html:
        return None, None

    m = _RE_HISTINFO.search(html)
    if m:
        return m.group("pair"), m.group("sml")

    # 1) En muchos fondos aparece como data-pair-id en el DOM principal
    try:
        if soup is not None:
            node = soup.select_one("div.instrumentHead [data-pair-id]") or soup.select_one("[data-pair-id]")
            if node:
                pid = (node.get("data-pair-id") or "").strip()
                if pid.isdigit():
                    return pid, None
    except Exception:
        pass

    # 2) dataLayer.push('instrumentId1036800') o variantes
    m0 = _RE_DATALAYER_INSTRUMENTID.search(html)
    if m0 and m0.group(1).isdigit():
        return m0.group(1), None

    # 3) pairId suelto
    m2 = _RE_PAIRID_GENERIC.search(html)
    if m2:
        return m2.group(1), None

    # 4) data-pair-id suelto en HTML
    m3 = _RE_DATA_PAIR_ID.search(html)
    if m3:
        return m3.group(1), None

    return None, None


# ── Parse tabla HTML (#currtable) ───────────────────────────────────────────

def _epoch_to_date_iso(epoch_str: str) -> Optional[str]:
    s = (epoch_str or "").strip()
    if not s.isdigit():
        return None
    try:
        n = int(s)
        # seconds vs ms
        if n > 10_000_000_000:
            n = n // 1000
        return datetime.utcfromtimestamp(n).date().isoformat()
    except Exception:
        return None


def _find_hist_table(soup: BeautifulSoup):
    # Orden: lo más específico primero
    t = soup.select_one("table#currtable")
    if t:
        return t
    t = soup.find("table", id=re.compile(r"currtable", re.I))
    if t:
        return t
    # Fallbacks frecuentes
    t = soup.select_one("table.historicalTbl")
    if t:
        return t
    t = soup.select_one("table.genTbl.historicalTbl")
    if t:
        return t
    return None


def _parse_html_currtable(html: str) -> List[Tuple[str, float]]:
    """
    Extrae (YYYY-MM-DD, close) desde la tabla renderizada en la propia página.

    IMPORTANTE: NO dependemos de <tbody>, porque a veces no existe en el HTML servido.
    """
    if not html:
        return []

    try:
        soup = BeautifulSoup(html, "lxml")
        table = _find_hist_table(soup)
        if not table:
            return []

        out: List[Tuple[str, float]] = []

        # No asumir <tbody>: coger todos los tr y filtrar por td>=2
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            # Fecha
            d_iso = _epoch_to_date_iso((tds[0].get("data-real-value") or "").strip())
            if not d_iso:
                try:
                    d_iso = datetime.strptime(tds[0].get_text(strip=True), "%d.%m.%Y").date().isoformat()
                except Exception:
                    continue

            # Close (Último)
            raw = tds[1].get("data-real-value") or tds[1].get_text(strip=True)
            try:
                out.append((d_iso, parse_float(str(raw))))
            except Exception:
                continue

        out.sort(key=lambda x: x[0])
        return out
    except Exception as e:
        log.debug("Investing: error parseando tabla HTML: %s", e)
        return []


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


# ── HistoricalDataAjax (fallback / full refresh) ────────────────────────────

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

            d_iso = _epoch_to_date_iso((tds[0].get("data-real-value") or "").strip())
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

        out.sort(key=lambda x: x[0])
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


# ── Entry point ─────────────────────────────────────────────────────────────

def scrape_investing_prices(
    session,
    investing_url: str,
    cached_pair_id: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> Tuple[List[Tuple[str, float]], Optional[str]]:
    """
    Estrategia robusta (Actions-friendly):
      1) GET HTML y parse de tabla (#currtable) SIN depender de <tbody>.
      2) Extraer pair_id/sml_id del HTML para cachear.
      3) Si tabla vacía: fallback AJAX (1 intento) para el rango incremental.
      4) Si full_refresh=True: AJAX por chunks; si falla, devuelve lo de la tabla.
    """
    if not investing_url:
        return [], None

    end = end_date or date.today()
    start = date(2000, 1, 1) if full_refresh else (start_date or (end - timedelta(days=45)))

    html = _fetch_html(session, investing_url)
    if not html:
        return [], None

    # Parse tabla HTML (más estable)
    table_rows = _parse_html_currtable(html)
    table_rows = _filter_range(table_rows, start, end)

    # Extraer pair_id/sml_id para cache / AJAX
    soup: Optional[BeautifulSoup] = None
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = None

    pair_id_html, sml_id = _extract_pair_and_sml(html, soup=soup)
    pair_id = (cached_pair_id or "").strip() or pair_id_html

    # Si hay tabla y NO es full refresh, terminamos (incremental)
    if table_rows and not full_refresh:
        log.info("Investing: %s precios (tabla HTML) para %s", len(table_rows), investing_url)
        return table_rows, pair_id

    # Si la tabla vino vacía, prueba 1 AJAX “rápido” incluso sin full_refresh (soluciona casos sin currtable).
    if (not table_rows) and pair_id and pair_id.isdigit():
        frag = _ajax_post(session, pair_id, start, end, investing_url, sml_id=sml_id)
        if frag:
            ajax_rows = _parse_ajax_fragment(frag)
            ajax_rows = _filter_range(ajax_rows, start, end)
            if ajax_rows:
                log.info("Investing: %s precios (AJAX fallback) para %s", len(ajax_rows), investing_url)
                return ajax_rows, pair_id

    # Full refresh: AJAX por chunks (best-effort)
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

        # Si AJAX no da nada, devuelve lo que tengas de la tabla (aunque sea vacío)
        if table_rows:
            log.warning(
                "Investing: AJAX sin datos (posible 403/bloqueo). Usando tabla HTML (%s filas) para %s",
                len(table_rows), investing_url,
            )
            return table_rows, pair_id

        log.warning("Investing: sin datos por AJAX ni por tabla HTML para %s (pair_id=%s).", investing_url, pair_id)
        return [], pair_id

    # No full_refresh y no hay nada
    if not table_rows:
        log.warning("Investing: tabla HTML vacía/no encontrada para %s.", investing_url)
        return [], pair_id

    return table_rows, pair_id
