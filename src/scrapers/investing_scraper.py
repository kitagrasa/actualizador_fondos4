from __future__ import annotations

import logging
import re
import time
from collections import Counter
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

# ── Detección de HTML bloqueado (best-effort) ────────────────────────────────

_BLOCK_MARKERS = [
    "cf-chl",  # Cloudflare challenge
    "challenge-platform",
    "attention required",
    "verify you are human",
    "captcha",
    "access denied",
    "temporarily unavailable",
]


def _looks_blocked(html: str) -> bool:
    """
    No existe un detector perfecto. Solo marcamos como bloqueado si
    hay señales claras de challenge/captcha.
    """
    if not html:
        return True
    low = html.lower()
    return any(m in low for m in _BLOCK_MARKERS)


# ── Fetch HTML ───────────────────────────────────────────────────────────────

def _fetch_html(session, investing_url: str) -> Optional[str]:
    """
    Fetch simple con headers de navegador. Importante:
    - NO descartamos por "no contiene investing/pairid/etc." porque hay HTML válidos
      que no contienen esas palabras exactas y aun así traen data-pair-id o scripts útiles.
    """
    domain = urlparse(investing_url).netloc or "www.investing.com"
    headers = {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        # Ojo: requests ya negocia y descomprime; no hace falta forzar Accept-Encoding
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

        if _looks_blocked(html):
            log.warning("Investing HTML: parece bloqueado/challenge url=%s (len=%s)", investing_url, len(html))
            # Aun así lo devolvemos: a veces el HTML “raro” contiene ids.
            return html

        return html
    except Exception as e:
        log.error("Investing HTML error url=%s: %s", investing_url, e)
        return None


# ── Extracción robusta de pair_id ────────────────────────────────────────────

_RE_HIST_EXCESS = re.compile(r"histDataExcessInfo.*?pairId\s*[:=]\s*(\d{3,10})", re.I | re.S)
_RE_INSTRUMENT_ID = re.compile(r"\binstrument[_-]?id\b\s*[:=]\s*['\"]?(\d{3,10})", re.I)
_RE_PAIR_ID_JSON = re.compile(r"\bpair_?id\b\s*[:=]\s*['\"]?(\d{3,10})", re.I)
_RE_PAIRID_INLINE = re.compile(r"\bpairid\s*(\d{3,10})\b", re.I)
_RE_PID_LAST = re.compile(r"\bpid[-_](\d{3,10})[-_](?:last|time|pc|pcp)\b", re.I)
_RE_DATA_PAIR_ID = re.compile(r"\bdata-pair-id\b\s*=\s*['\"]?(\d{3,10})", re.I)


def _score_pair_id(candidate: str, html: str) -> int:
    """
    Heurística: elegimos el id que más "pinta" de ser el principal.
    """
    if not candidate or not candidate.isdigit():
        return -10
    cid = int(candidate)
    if cid <= 100:
        return -10

    score = 0
    # Apariciones fuertes
    if re.search(rf"histDataExcessInfo.*?pairId\s*[:=]\s*{candidate}\b", html, re.I | re.S):
        score += 8
    if re.search(rf"\bdata-pair-id\s*=\s*['\"]?{candidate}\b", html, re.I):
        score += 6
    if re.search(rf"\bpid[-_]{candidate}[-_](?:last|time|pc|pcp)\b", html, re.I):
        score += 4
    if re.search(rf"\bpairid\s*{candidate}\b", html, re.I):
        score += 3

    # Bonus: cuanto más aparezca, mejor (pero con tope)
    score += min(5, html.lower().count(candidate) // 10)

    return score


def _pair_ids_from_dom(html: str) -> List[str]:
    """
    Extrae IDs desde atributos data-pair-id (muy común en Investing).
    """
    ids: List[str] = []
    try:
        soup = BeautifulSoup(html, "lxml")
        for node in soup.select("[data-pair-id]"):
            v = (node.get("data-pair-id") or "").strip()
            if v.isdigit():
                ids.append(v)
    except Exception:
        pass
    return ids


def _pair_ids_from_regex(html: str) -> List[str]:
    ids: List[str] = []
    for rx in (_RE_HIST_EXCESS, _RE_INSTRUMENT_ID, _RE_PAIR_ID_JSON, _RE_PAIRID_INLINE, _RE_PID_LAST, _RE_DATA_PAIR_ID):
        try:
            for m in rx.finditer(html):
                ids.append(m.group(1))
        except Exception:
            continue
    return ids


def _pair_id_from_html(html: str) -> Optional[str]:
    if not html:
        return None

    candidates = []
    candidates.extend(_pair_ids_from_dom(html))
    candidates.extend(_pair_ids_from_regex(html))

    # Normalizar y filtrar
    candidates = [c for c in candidates if c and c.isdigit() and int(c) > 100]
    if not candidates:
        return None

    # Si hay varios, escogemos por score + frecuencia
    freq = Counter(candidates)
    unique = list(freq.keys())
    unique.sort(key=lambda c: (_score_pair_id(c, html), freq[c]), reverse=True)
    return unique[0]


# ── HistoricalDataAjax ───────────────────────────────────────────────────────

def _ajax_post(
    session,
    pair_id: str,
    st: date,
    en: date,
    referer: str,
) -> Optional[str]:
    """
    POST a /instruments/HistoricalDataAjax en el mismo dominio del referer.
    Devuelve fragmento HTML (filas <tr>...</tr>) o None.
    """
    domain = urlparse(referer).netloc or "es.investing.com"
    url = f"https://{domain}/instruments/HistoricalDataAjax"

    headers = {
        "User-Agent": _UA,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded",
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

    try:
        r = session.post(url, data=data, headers=headers, timeout=30)
        log.debug("Investing AJAX: status=%s pair_id=%s %s..%s", r.status_code, pair_id, st, en)

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
    """
    Parsea el fragmento HTML de HistoricalDataAjax.
    Cada fila trae fecha + último (y más columnas).
    """
    if not html_fragment:
        return []

    try:
        soup = BeautifulSoup(f"<table><tbody>{html_fragment}</tbody></table>", "lxml")
        out: List[Tuple[str, float]] = []

        for tr in soup.select("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            # Fecha: epoch en data-real-value o texto DD.MM.YYYY
            d_iso: Optional[str] = None
            epoch = tds[0].get("data-real-value")
            if epoch and str(epoch).strip().isdigit():
                try:
                    d_iso = datetime.utcfromtimestamp(int(str(epoch).strip())).date().isoformat()
                except Exception:
                    d_iso = None

            if not d_iso:
                txt = tds[0].get_text(strip=True)
                try:
                    d_iso = datetime.strptime(txt, "%d.%m.%Y").date().isoformat()
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
    No requiere nada más que la URL.
    - Si cached_pair_id existe, lo usa primero.
    - Si no existe (o si falla), lo extrae del HTML de la propia página.
    - Descarga precios por HistoricalDataAjax en chunks.

    Devuelve (prices, pair_id) para que app.py cachee pair_id automáticamente.
    """
    if not investing_url:
        return [], None

    # 1) Conseguir pair_id (cache -> HTML)
    pair_id = (cached_pair_id or "").strip() or None

    html: Optional[str] = None
    if not pair_id:
        html = _fetch_html(session, investing_url)
        if html:
            pair_id = _pair_id_from_html(html)

    if not pair_id:
        # Último intento: aunque tengamos HTML "bloqueado", a veces aún trae data-pair-id.
        if html:
            pid = _pair_id_from_html(html)
            if pid:
                pair_id = pid

    if not pair_id:
        sample = ""
        try:
            if html:
                sample = re.sub(r"\s+", " ", html[:350])
        except Exception:
            sample = ""
        log.warning("Investing: no se pudo obtener pair_id para %s. HTML_sample=%r", investing_url, sample)
        return [], None

    log.debug("Investing: pair_id=%s para %s", pair_id, investing_url)

    # 2) Rango
    end = end_date or date.today()
    start = date(2000, 1, 1) if full_refresh else (start_date or (end - timedelta(days=45)))

    # 3) Descargar en chunks
    chunk_months = 12 if full_refresh else 3
    collected: Dict[str, float] = {}

    for s, e in _date_chunks(start, end, months=chunk_months):
        frag = _ajax_post(session, pair_id, s, e, investing_url)
        if frag:
            for d, c in _parse_ajax_fragment(frag):
                collected[d] = c

        if full_refresh:
            time.sleep(0.2)

    # 4) Si no hemos sacado nada, reintentar 1 vez re-extrayendo pair_id (por si el cached era malo)
    if not collected and cached_pair_id:
        html2 = _fetch_html(session, investing_url)
        pid2 = _pair_id_from_html(html2) if html2 else None
        if pid2 and pid2 != cached_pair_id:
            log.info("Investing: pair_id cambió %s -> %s (reintento AJAX)", cached_pair_id, pid2)
            pair_id = pid2
            for s, e in _date_chunks(start, end, months=chunk_months):
                frag = _ajax_post(session, pair_id, s, e, investing_url)
                if frag:
                    for d, c in _parse_ajax_fragment(frag):
                        collected[d] = c
                if full_refresh:
                    time.sleep(0.2)

    if collected:
        out = sorted(collected.items(), key=lambda x: x[0])
        log.info("Investing: %s precios (pair_id=%s) para %s", len(out), pair_id, investing_url)
        return out, pair_id

    log.warning(
        "Investing: 0 precios para %s (pair_id=%s). "
        "Posible bloqueo o cambio en endpoint AJAX; se seguirá con FT/Fundsquare.",
        investing_url,
        pair_id,
    )
    return [], pair_id
