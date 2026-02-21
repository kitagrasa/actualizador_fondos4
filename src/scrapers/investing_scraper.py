from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from ..utils import parse_float

log = logging.getLogger("scrapers.investing")

# cloudscraper bypasea el JS Challenge de Cloudflare que bloquea GitHub Actions IPs.
# Se crea una sola vez a nivel de módulo y se reutiliza en todas las llamadas.
try:
    import cloudscraper as _cs
    _session = _cs.create_scraper(
        browser={"browser": "chrome", "platform": "linux", "mobile": False}
    )
    log.debug("Investing: cloudscraper listo.")
except Exception as _e:
    _session = None
    log.warning("Investing: cloudscraper no disponible (%s). Añade 'cloudscraper>=1.2.71' a requirements.txt.", _e)

_API_URL = "https://api.investing.com/api/financialdata/historical/{}"


def _find_key(obj, key: str):
    """Búsqueda recursiva de una clave en dict/list anidado."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = _find_key(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _find_key(item, key)
            if r is not None:
                return r
    return None


def _get(url: str, **kwargs):
    """GET con cloudscraper (bypass Cloudflare). Fallback a requests si no está instalado."""
    if _session is not None:
        return _session.get(url, **kwargs)
    import requests
    return requests.get(url, **kwargs)


def _get_instrument_id(investing_url: str) -> Optional[str]:
    """Extrae instrument_id de la página histórica de investing.com."""
    try:
        r = _get(investing_url, timeout=30)
        if r.status_code != 200:
            log.warning("Investing: status=%s url=%s", r.status_code, investing_url)
            return None

        # 1) Buscar en __NEXT_DATA__ (Next.js)
        soup = BeautifulSoup(r.text, "lxml")
        tag = soup.find("script", id="__NEXT_DATA__")
        if tag and tag.string:
            try:
                data = json.loads(tag.string)
                for key in ("instrument_id", "instrumentId", "pair_id"):
                    val = _find_key(data, key)
                    if val:
                        log.debug("Investing: instrument_id=%s (desde __NEXT_DATA__)", val)
                        return str(val)
            except Exception as e:
                log.debug("Investing: Error parseando __NEXT_DATA__: %s", e)

        # 2) Fallback: regex en HTML
        for pattern in (
            r'"instrument_id"\s*:\s*"?(\d+)"?',
            r'"pair_id"\s*:\s*"?(\d+)"?',
            r'data-pair-id="(\d+)"',
        ):
            m = re.search(pattern, r.text)
            if m:
                log.debug("Investing: instrument_id=%s (desde regex)", m.group(1))
                return m.group(1)

        log.warning("Investing: No se encontró instrument_id en %s", investing_url)
        return None

    except Exception as e:
        log.error("Investing: Error obteniendo instrument_id de %s: %s", investing_url, e, exc_info=True)
        return None


def _parse_date(s: str) -> Optional[str]:
    s = str(s).strip()
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}", s):
            return datetime.strptime(s[:10], "%Y-%m-%d").date().isoformat()
        return datetime.strptime(s, "%b %d, %Y").date().isoformat()
    except Exception:
        return None


def scrape_investing_prices(
    session,  # mantenido por compatibilidad de firma; no se usa (usamos cloudscraper)
    investing_url: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> List[Tuple[str, float]]:
    """
    Acepta la URL de la página histórica de investing.com.
    Usa cloudscraper para bypassear Cloudflare. Devuelve [(YYYY-MM-DD, close)].
    """
    if not investing_url:
        log.debug("Investing: URL vacía, se omite.")
        return []

    if _session is None:
        log.warning("Investing: cloudscraper no disponible. Instala: pip install cloudscraper>=1.2.71")
        return []

    instrument_id = _get_instrument_id(investing_url)
    if not instrument_id:
        return []

    end = end_date or date.today()
    start = date(2000, 1, 1) if full_refresh else (start_date or (end - timedelta(days=45)))

    domain = urlparse(investing_url).netloc or "www.investing.com"
    url = _API_URL.format(instrument_id)
    params = {
        "start-date": start.isoformat(),
        "end-date": end.isoformat(),
        "time-frame": "Daily",
        "add-missing-rows": "false",
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "domain-id": domain,
        "Referer": investing_url,
    }

    try:
        r = _get(url, params=params, headers=headers, timeout=30)
        log.debug("Investing API: status=%s instrument_id=%s %s..%s",
                  r.status_code, instrument_id, start, end)

        if r.status_code != 200:
            log.warning("Investing API: status=%s instrument_id=%s", r.status_code, instrument_id)
            return []

        payload = r.json()

        # Localizar la lista de datos en la respuesta
        data_list = None
        if isinstance(payload, list):
            data_list = payload
        elif isinstance(payload, dict):
            for key in ("data", "historical", "historicalData", "results", "Data"):
                if key in payload and isinstance(payload[key], list):
                    data_list = payload[key]
                    break
            if data_list is None:
                data_list = _find_key(payload, "data")

        if not data_list:
            log.warning("Investing API: Sin datos. instrument_id=%s resp=%s",
                        instrument_id, str(payload)[:300])
            return []

        out: List[Tuple[str, float]] = []
        for row in data_list:
            if not isinstance(row, dict):
                continue

            date_val = None
            for dk in ("rowDateRaw", "date", "Date", "rowDate"):
                if dk in row and row[dk]:
                    date_val = _parse_date(row[dk])
                    if date_val:
                        break

            close_val = None
            for ck in ("last_close", "close", "Close", "last_closeRaw", "price"):
                if ck in row and row[ck] is not None:
                    try:
                        close_val = parse_float(str(row[ck]))
                        break
                    except Exception:
                        continue

            if date_val and close_val is not None:
                out.append((date_val, close_val))

        log.debug("Investing: %s precios para instrument_id=%s", len(out), instrument_id)
        return sorted(out)

    except Exception as e:
        log.error("Investing error url=%s: %s", investing_url, e, exc_info=True)
        return []
