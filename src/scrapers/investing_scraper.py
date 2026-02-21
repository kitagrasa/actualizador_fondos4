from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from ..utils import parse_float

log = logging.getLogger("scrapers.investing")

try:
    import cloudscraper as _cs
    _session = _cs.create_scraper(
        browser={"browser": "chrome", "platform": "linux", "mobile": False}
    )
    log.debug("Investing: cloudscraper listo.")
except Exception as _e:
    _session = None
    log.warning("Investing: cloudscraper no disponible (%s).", _e)


def _get(url: str, **kwargs):
    if _session is not None:
        return _session.get(url, **kwargs)
    import requests
    return requests.get(url, **kwargs)


def _post(url: str, **kwargs):
    if _session is not None:
        return _session.post(url, **kwargs)
    import requests
    return requests.post(url, **kwargs)


def _get_pair_id(investing_url: str) -> Optional[str]:
    """
    Obtiene el pair_id para la llamada AJAX.
    Estrategia 1 (sin red): ?cid= en la propia URL → par_id directo.
    Estrategia 2 (red): BeautifulSoup data-pair-id + regex sobre el HTML.
    """
    # FIX 1: extraer ?cid= de la URL antes de hacer cualquier request
    cid = parse_qs(urlparse(investing_url).query).get("cid", [None])[0]
    if cid and cid.isdigit():
        log.debug("Investing: pair_id=%s (desde ?cid= en URL)", cid)
        return cid

    # Cargar la página para buscar el pair_id
    try:
        r = _get(investing_url, timeout=30)
        if r.status_code != 200:
            log.warning("Investing: status=%s url=%s", r.status_code, investing_url)
            return None

        soup = BeautifulSoup(r.text, "lxml")

        # BeautifulSoup: atributo data-pair-id (más fiable)
        el = soup.find(attrs={"data-pair-id": True})
        if el:
            val = str(el.get("data-pair-id", "")).strip()
            if val.isdigit():
                log.debug("Investing: pair_id=%s (data-pair-id attr)", val)
                return val

        # Regex sobre el HTML
        for pattern in (
            r'pairId["\s:,]+(\d+)',
            r'pairid["\s:,]+(\d+)',
            r'pid-eu-(\d+)',
            r'dimension138[,\s]+(\d+)',
        ):
            m = re.search(pattern, r.text, re.IGNORECASE)
            if m:
                log.debug("Investing: pair_id=%s (regex %r)", m.group(1), pattern)
                return m.group(1)

        log.warning("Investing: No se encontró pair_id en %s", investing_url)
        return None

    except Exception as e:
        log.error("Investing: Error al obtener pair_id de %s: %s", investing_url, e, exc_info=True)
        return None


def _parse_date_str(s: str) -> Optional[str]:
    s = str(s).strip()
    for fmt in ("%b %d, %Y", "%d de %b. de %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else None


def _fetch_historical_ajax(
    pair_id: str,
    investing_url: str,
    start: date,
    end: date,
) -> List[Tuple[str, float]]:
    """
    POST a HistoricalDataAjax. Devuelve [(YYYY-MM-DD, close)].
    FIX 2: usa el MISMO dominio que investing_url para el POST
    (las cookies de cloudscraper son válidas para ese dominio, no para www.investing.com).
    """
    domain = urlparse(investing_url).netloc  # "es.investing.com"
    ajax_url = f"https://{domain}/instruments/HistoricalDataAjax"

    form_data = {
        "curr_id": pair_id,
        "st_date": start.strftime("%m/%d/%Y"),
        "end_date": end.strftime("%m/%d/%Y"),
        "interval_sec": "Daily",
        "sort_col": "date",
        "sort_ord": "DESC",
        "action": "historical_data",
    }
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Referer": investing_url,
        "Accept": "text/plain, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": f"https://{domain}",
    }

    try:
        r = _post(ajax_url, data=form_data, headers=headers, timeout=30)
        log.debug("Investing AJAX: status=%s pair_id=%s %s..%s",
                  r.status_code, pair_id, start, end)

        if r.status_code != 200:
            log.warning("Investing AJAX: status=%s pair_id=%s resp=%s",
                        r.status_code, pair_id, r.text[:300])
            return []

        soup = BeautifulSoup(r.text, "lxml")
        table = (
            soup.find("table", {"id": "curr_table"})
            or soup.find("table", class_=re.compile(r"histor", re.I))
            or soup.find("table")
        )
        if not table:
            log.warning("Investing AJAX: No se encontró tabla. pair_id=%s resp=%s",
                        pair_id, r.text[:300])
            return []

        # Determinar índice de la columna de precio de cierre
        th_tags = table.select("thead th") or table.select("tr:first-child th")
        headers_text = [th.get_text(strip=True).lower() for th in th_tags]
        log.debug("Investing AJAX: columnas=%s", headers_text)

        close_idx = 1  # "Último" / "Price" suele ser la segunda columna
        for i, h in enumerate(headers_text):
            if h in ("price", "precio", "último", "ultimo", "close",
                     "cierre", "last", "nav"):
                close_idx = i
                break

        out: List[Tuple[str, float]] = []
        for tr in table.select("tbody tr"):
            tds = tr.find_all("td")
            if len(tds) <= close_idx:
                continue
            try:
                # Preferir <time datetime="YYYY-MM-DD"> si existe
                time_el = tds[0].find("time")
                if time_el and time_el.get("datetime"):
                    date_val = str(time_el["datetime"])[:10]
                else:
                    date_val = _parse_date_str(tds[0].get_text(strip=True))

                if not date_val:
                    continue

                close_val = parse_float(tds[close_idx].get_text(strip=True))
                out.append((date_val, close_val))
            except Exception:
                continue

        log.debug("Investing AJAX: %s precios para pair_id=%s", len(out), pair_id)
        return sorted(out)

    except Exception as e:
        log.error("Investing AJAX error pair_id=%s: %s", pair_id, e, exc_info=True)
        return []


def scrape_investing_prices(
    session,  # mantenido por compatibilidad de firma; se usa cloudscraper internamente
    investing_url: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> List[Tuple[str, float]]:
    """
    Acepta la URL de la página histórica de investing.com (web legacy).
    1. Extrae pair_id de ?cid= (si existe) o de la página.
    2. POST a HistoricalDataAjax en el mismo dominio → cookies válidas.
    3. Parsea tabla HTML de respuesta. Devuelve [(YYYY-MM-DD, close)].
    """
    if not investing_url:
        log.debug("Investing: URL vacía, se omite.")
        return []

    if _session is None:
        log.warning("Investing: cloudscraper no disponible. pip install cloudscraper>=1.2.71")
        return []

    pair_id = _get_pair_id(investing_url)
    if not pair_id:
        return []

    end = end_date or date.today()
    start = date(2000, 1, 1) if full_refresh else (start_date or (end - timedelta(days=45)))

    return _fetch_historical_ajax(pair_id, investing_url, start, end)
