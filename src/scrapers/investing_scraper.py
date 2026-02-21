from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from ..utils import parse_float

log = logging.getLogger("scrapers.investing")

_API_URL = "https://api.investing.com/api/financialdata/historical/{}"

_BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)


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


def _get_instrument_id(session, investing_url: str) -> Optional[str]:
    """Extrae instrument_id de la página histórica de investing.com."""
    try:
        headers = {
            "User-Agent": _BROWSER_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Referer": "https://www.investing.com/",
        }
        r = session.get(investing_url, headers=headers, timeout=30)
        if r.status_code != 200:
            log.warning("Investing: status=%s url=%s", r.status_code, investing_url)
            return None

        # 1) Buscar en __NEXT_DATA__ (Next.js)
        soup = BeautifulSoup(r.text, "lxml")
        tag = soup.find("script", id="__NEXT_DATA__")
        if tag and tag.string:
            try:
                data = json.loads(tag.string)
                for key in ("instrument_id", "instrumentId"):
                    val = _find_key(data, key)
                    if val:
                        log.debug("Investing: instrument_id=%s (desde __NEXT_DATA__)", val)
                        return str(val)
            except Exception as e:
                log.debug("Investing: Error parseando __NEXT_DATA__: %s", e)

        # 2) Fallback: regex en HTML
        for pattern in (r'"instrument_id"\s*:\s*"?(\d+)"?', r'"pair_id"\s*:\s*"?(\d+)"?'):
            m = re.search(pattern, r.text)
            if m:
                log.debug("Investing: instrument_id=%s (desde regex)", m.group(1))
                return m.group(1)

        log.warning("Investing: No se encontró instrument_id en %s", investing_url)
        return None

    except Exception as e:
        log.error("Investing: Error obteniendo instrument_id de %s: %s", investing_url, e, exc_info=True)
        return None


def _parse_date(value) -> Optional[str]:
    if not value:
        return None
    try:
        s = str(value).strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}", s):
            return datetime.strptime(s[:10], "%Y-%m-%d").date().isoformat()
        if re.match(r"^\d{2}/\d{2}/\d{4}", s):
            return datetime.strptime(s[:10], "%m/%d/%Y").date().isoformat()
        if re.match(r"^\d{10,13}$", s):
            ts = int(s) // 1000 if int(s) > 1e10 else int(s)
            return datetime.utcfromtimestamp(ts).date().isoformat()
    except Exception:
        pass
    return None


def _parse_price(value) -> Optional[float]:
    try:
        if isinstance(value, (int, float)):
            return float(value)
        return parse_float(str(value))
    except Exception:
        return None


def scrape_investing_prices(
    session,
    investing_url: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> List[Tuple[str, float]]:
    """
    Acepta la URL de la página histórica de investing.com.
    Extrae instrument_id y llama a la API. Devuelve [(YYYY-MM-DD, close)].
    """
    if not investing_url:
        log.debug("Investing: URL vacía, se omite.")
        return []

    instrument_id = _get_instrument_id(session, investing_url)
    if not instrument_id:
        return []

    end = end_date or date.today()
    start = date(2000, 1, 1) if full_refresh else (start_date or (end - timedelta(days=45)))

    api_url = _API_URL.format(instrument_id)
    params = {
        "start-date": start.isoformat(),
        "end-date": end.isoformat(),
        "time-frame": "Daily",
        "add-missing-rows": "false",
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "domain-id": "www.investing.com",
        "Referer": investing_url,
        "User-Agent": _BROWSER_UA,
    }

    try:
        r = session.get(api_url, params=params, headers=headers, timeout=30)
        log.debug("Investing API: status=%s instrument_id=%s %s..%s", r.status_code, instrument_id, start, end)
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
            log.warning("Investing API: Sin datos en respuesta. instrument_id=%s. Resp=%s",
                        instrument_id, str(payload)[:300])
            return []

        out: List[Tuple[str, float]] = []
        for row in data_list:
            if not isinstance(row, dict):
                continue
            date_val = None
            for dk in ("rowDateRaw", "date", "Date", "time", "timestamp", "rowDate"):
                if dk in row:
                    date_val = _parse_date(row[dk])
                    if date_val:
                        break
            close_val = None
            for ck in ("last_close", "close", "Close", "last_closeRaw", "price", "Price"):
                if ck in row:
                    close_val = _parse_price(row[ck])
                    if close_val is not None:
                        break
            if date_val and close_val is not None:
                out.append((date_val, close_val))

        log.debug("Investing: %s precios para instrument_id=%s", len(out), instrument_id)
        return sorted(out)

    except Exception as e:
        log.error("Investing error url=%s: %s", investing_url, e, exc_info=True)
        return []
