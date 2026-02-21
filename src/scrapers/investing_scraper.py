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

# ── Extracción de pair_id desde HTML ─────────────────────────────────────────

def _pair_id_from_html(html: str) -> Optional[str]:
    if not html:
        return None
    for pattern in [
        r'histDataExcessInfo\s*=\s*\{[^}]*?pairId\s*:\s*(?P<id>\d+)',
        r'instrument[_-]?id["\']?\s*:\s*["\']?(?P<id>\d+)',
        r'"?pair_?id"?\s*:\s*"?(?P<id>\d+)"?',
        r'\bdata-pair-id\s*=\s*"?(?P<id>\d+)"?',
    ]:
        m = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
        if m:
            val = m.group("id")
            if val.isdigit() and int(val) > 100:  # Filtrar IDs triviales (0, 1, etc.)
                return val
    return None


def _fetch_html(session, investing_url: str) -> Optional[str]:
    """
    Fetch simple con headers de browser real.
    Sin curl_cffi (daba 403 + contenido binario inutilizable).
    """
    domain = urlparse(investing_url).netloc or "www.investing.com"
    headers = {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"https://{domain}/",
        "Connection": "keep-alive",
    }
    try:
        r = session.get(investing_url, headers=headers, timeout=30)
        if r.status_code != 200:
            log.warning("Investing HTML: status=%s url=%s", r.status_code, investing_url)
            return None
        html = r.text or ""
        # Validar que es HTML real (no contenido binario ni página de bloqueo vacía)
        if len(html) < 2000 or not any(k in html.lower() for k in ["investing", "pairid", "pair_id", "instrument"]):
            log.warning("Investing HTML: respuesta inválida/bloqueada para %s (len=%s)", investing_url, len(html))
            return None
        return html
    except Exception as e:
        log.error("Investing HTML error url=%s: %s", investing_url, e)
        return None


# ── HistoricalDataAjax (mismo dominio, sin subdominios tvcX) ─────────────────

def _ajax_post(
    session,
    pair_id: str,
    st: date,
    en: date,
    referer: str,
) -> Optional[str]:
    """
    POST a /instruments/HistoricalDataAjax.
    Usa el mismo dominio que la página → no depende de subdominios tvcX
    que fallan DNS cuando el hash es aleatorio.
    Devuelve el fragmento HTML de filas, o None si falla.
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
    """Parsea el fragmento HTML devuelto por HistoricalDataAjax."""
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
            # Fecha: epoch en data-real-value o texto DD.MM.YYYY
            d_iso: Optional[str] = None
            try:
                epoch = tds[0].get("data-real-value")
                if epoch:
                    d_iso = datetime.utcfromtimestamp(int(epoch)).date().isoformat()
            except Exception:
                pass
            if not d_iso:
                try:
                    d_iso = datetime.strptime(
                        tds[0].get_text(strip=True), "%d.%m.%Y"
                    ).date().isoformat()
                except Exception:
                    continue
            # Precio: data-real-value o texto del td "Último"
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
    chunks, cur = [], start
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
    Scraper Investing.com robusto para GitHub Actions.

    Estrategia:
      1. pair_id: usa cached_pair_id (metadata) si disponible.
         Si no, intenta extraerlo del HTML (solo funciona si no hay bloqueo).
      2. Datos: HistoricalDataAjax (POST, mismo dominio es.investing.com).
         No usa subdominios tvcX que fallan DNS con hash aleatorio.
      3. Devuelve (prices, pair_id) para que app.py cachee el pair_id.

    Args:
        cached_pair_id: pair_id guardado en fundsmetadata.json (evita el fetch HTML).
    Returns:
        Tuple (lista de (YYYY-MM-DD, close), pair_id obtenido o None).
    """
    if not investing_url:
        return [], None

    # 1. Obtener pair_id: caché > HTML
    pair_id = cached_pair_id
    if not pair_id:
        html = _fetch_html(session, investing_url)
        pair_id = _pair_id_from_html(html) if html else None

    if not pair_id:
        log.warning(
            "Investing: no se pudo obtener pair_id para %s "
            "(si persiste, añade manualmente el pair_id en fundsmetadata.json)",
            investing_url,
        )
        return [], None

    log.debug("Investing: pair_id=%s para %s", pair_id, investing_url)

    # 2. Rango de fechas
    end = end_date or date.today()
    start = date(2000, 1, 1) if full_refresh else (start_date or (end - timedelta(days=45)))

    # 3. Fetch por chunks via HistoricalDataAjax
    chunk_months = 12 if full_refresh else 3
    collected: Dict[str, float] = {}

    for s, e in _date_chunks(start, end, months=chunk_months):
        frag = _ajax_post(session, pair_id, s, e, investing_url)
        if frag:
            for d, c in _parse_ajax_fragment(frag):
                collected[d] = c
        if full_refresh:
            time.sleep(0.2)

    if collected:
        out = sorted(collected.items(), key=lambda x: x[0])
        log.info(
            "Investing: %s precios (pair_id=%s) para %s",
            len(out), pair_id, investing_url,
        )
        return out, pair_id

    log.warning(
        "Investing: 0 precios para %s (pair_id=%s). "
        "Posible bloqueo desde GitHub Actions → usando FT/Fundsquare.",
        investing_url, pair_id,
    )
    return [], pair_id  # Devolvemos pair_id aunque no haya precios (para cachearlo)
