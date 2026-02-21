from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup

from ..utils import parse_float

log = logging.getLogger("scrapers.investing")

# cloudscraper bypasea Cloudflare (IPs de GitHub Actions bloqueadas en investing.com)
try:
    import cloudscraper as _cs
    _session = _cs.create_scraper(
        browser={"browser": "chrome", "platform": "linux", "mobile": False}
    )
    log.debug("Investing: cloudscraper listo.")
except Exception as _e:
    _session = None
    log.warning("Investing: cloudscraper no disponible (%s).", _e)

# Endpoint LEGACY de investing.com (POST, devuelve HTML con tabla)
# No usar api.investing.com: esta web no es Next.js y ese endpoint no existe aquí
_AJAX_URL = "https://www.investing.com/instruments/HistoricalDataAjax"


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
    Obtiene el pair_id de la página histórica de investing.com.
    Estrategia 1: atributo data-pair-id (BeautifulSoup, más fiable).
    Estrategia 2: múltiples regex sobre el HTML.
    """
    try:
        r = _get(investing_url, timeout=30)
        if r.status_code != 200:
            log.warning("Investing: status=%s url=%s", r.status_code, investing_url)
            return None

        soup = BeautifulSoup(r.text, "lxml")

        # 1) Atributo data-pair-id en cualquier elemento HTML
        el = soup.find(attrs={"data-pair-id": True})
        if el:
            val = str(el.get("data-pair-id", "")).strip()
            if val.isdigit():
                log.debug("Investing: pair_id=%s (data-pair-id attr)", val)
                return val

        # 2) Regex en el HTML (varios formatos del JS legacy de investing.com)
        for pattern in (
            r'pairId["\s:,]+(\d+)',      # instrumentPopupParams[N] = {pairId: N}
            r'pairid["\s:,]+(\d+)',      # allKeyValue = {..., pairid: N, ...}
            r'pid-eu-(\d+)',             # triggersocketRetry, pid-eu-N, ... (primer elemento = instrumento actual)
            r'dimension138[,\s]+(\d+)',  # gaallSitesTracker.set('dimension138', N)
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
    """Parsea fecha en varios formatos que puede devolver investing.com."""
    s = str(s).strip()
    for fmt in ("%b %d, %Y", "%d de %b. de %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    # Último recurso: cualquier ISO fecha al inicio
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    return None


def _fetch_historical_ajax(
    pair_id: str,
    investing_url: str,
    start: date,
    end: date,
) -> List[Tuple[str, float]]:
    """
    POST a HistoricalDataAjax. Devuelve [(YYYY-MM-DD, close)].
    La respuesta es HTML con <table id="curr_table">.
    Columnas típicas: Fecha | Último/Price | Apertura | Máx | Mín | % Var.
    """
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
        "Origin": "https://www.investing.com",
    }

    try:
        r = _post(_AJAX_URL, data=form_data, headers=headers, timeout=30)
        log.debug("Investing AJAX: status=%s pair_id=%s %s..%s",
                  r.status_code, pair_id, start, end)

        if r.status_code != 200:
            log.warning("Investing AJAX: status=%s pair_id=%s resp=%s",
                        r.status_code, pair_id, r.text[:300])
            return []

        soup = BeautifulSoup(r.text, "lxml")

        # Buscar la tabla de datos históricos
        table = (
            soup.find("table", {"id": "curr_table"})
            or soup.find("table", class_=re.compile(r"histor", re.I))
            or soup.find("table")
        )
        if not table:
            log.warning("Investing AJAX: No se encontró tabla. pair_id=%s resp=%s",
                        pair_id, r.text[:300])
            return []

        # Determinar índice de la columna de precio de cierre por cabecera
        th_tags = table.select("thead th") or table.select("tr:first-child th")
        headers_text = [th.get_text(strip=True).lower() for th in th_tags]
        log.debug("Investing AJAX: columnas detectadas=%s", headers_text)

        close_idx = 1  # segunda columna por defecto (Último/Price)
        for i, h in enumerate(headers_text):
            if h in ("price", "precio", "último", "ultimo", "close", "cierre",
                     "last", "last price", "nav"):
                close_idx = i
                break

        out: List[Tuple[str, float]] = []
        for tr in table.select("tbody tr"):
            tds = tr.find_all("td")
            if len(tds) <= close_idx:
                continue
            try:
                # Fecha: preferir <time datetime="YYYY-MM-DD"> si existe
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
    1. Obtiene pair_id de la página (data-pair-id o regex).
    2. POST a HistoricalDataAjax con rango de fechas.
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
