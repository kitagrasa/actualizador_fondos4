from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup

from ..utils import parse_float

log = logging.getLogger("scrapers.investing")

# cloudscraper bypasea el JS Challenge de Cloudflare (IPs de GitHub Actions bloqueadas).
# Se instancia una vez a nivel de módulo y se reutiliza.
try:
    import cloudscraper as _cs
    _session = _cs.create_scraper(
        browser={"browser": "chrome", "platform": "linux", "mobile": False}
    )
    log.debug("Investing: cloudscraper listo.")
except Exception as _e:
    _session = None
    log.warning("Investing: cloudscraper no disponible (%s).", _e)


# ── Utilidades ────────────────────────────────────────────────────────────────

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


def _find_price_array(obj, min_records: int = 2):
    """
    Busca recursivamente el primer array que parezca datos históricos de precios.
    Un array válido contiene dicts con campo de fecha Y campo de precio.
    """
    if isinstance(obj, list) and len(obj) >= min_records:
        sample = obj[:3]
        if all(isinstance(r, dict) for r in sample):
            s = sample[0]
            has_date  = any(k in s for k in ("rowDate", "rowDateRaw", "date", "Date", "time"))
            has_price = any(k in s for k in ("last_close", "close", "Close", "price", "Price", "lastClose"))
            if has_date and has_price:
                return obj

    if isinstance(obj, dict):
        # Priorizar claves semánticas antes de búsqueda general
        for pk in ("historicalData", "historical", "quotes", "data", "prices"):
            if pk in obj:
                r = _find_price_array(obj[pk], min_records)
                if r is not None:
                    return r
        for v in obj.values():
            if isinstance(v, (dict, list)):
                r = _find_price_array(v, min_records)
                if r is not None:
                    return r
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                r = _find_price_array(item, min_records)
                if r is not None:
                    return r
    return None


def _parse_date(s: str) -> Optional[str]:
    s = str(s).strip()
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}", s):
            return datetime.strptime(s[:10], "%Y-%m-%d").date().isoformat()
        return datetime.strptime(s, "%b %d, %Y").date().isoformat()
    except Exception:
        return None


def _extract_prices(data_list: list) -> List[Tuple[str, float]]:
    """Extrae [(YYYY-MM-DD, close)] de una lista de dicts de precios."""
    out = []
    for row in data_list:
        if not isinstance(row, dict):
            continue
        date_val = None
        for dk in ("rowDateRaw", "date", "Date", "rowDate", "time"):
            if dk in row and row[dk]:
                date_val = _parse_date(str(row[dk]))
                if date_val:
                    break
        close_val = None
        for ck in ("last_close", "close", "Close", "last_closeRaw", "price", "Price", "lastClose"):
            if ck in row and row[ck] is not None:
                try:
                    close_val = parse_float(str(row[ck]))
                    break
                except Exception:
                    continue
        if date_val and close_val is not None:
            out.append((date_val, close_val))
    return sorted(out)


def _get(url: str, **kwargs):
    """GET con cloudscraper. Fallback a requests si no está disponible."""
    if _session is not None:
        return _session.get(url, **kwargs)
    import requests
    return requests.get(url, **kwargs)


# ── Lógica principal ──────────────────────────────────────────────────────────

def _get_page_data(investing_url: str) -> Tuple[Optional[str], List[Tuple[str, float]]]:
    """
    Fetches la página histórica de investing.com con cloudscraper.
    Devuelve (instrument_id, precios_inline).
    precios_inline puede estar vacío si __NEXT_DATA__ no los incluye.
    """
    try:
        r = _get(investing_url, timeout=30)
        if r.status_code != 200:
            log.warning("Investing: status=%s url=%s", r.status_code, investing_url)
            return None, []

        soup = BeautifulSoup(r.text, "lxml")
        tag = soup.find("script", id="__NEXT_DATA__")

        instrument_id: Optional[str] = None
        inline_prices: List[Tuple[str, float]] = []

        if tag and tag.string:
            try:
                data = json.loads(tag.string)

                # Buscar instrument_id
                for key in ("instrument_id", "instrumentId", "pair_id"):
                    val = _find_key(data, key)
                    if val:
                        instrument_id = str(val)
                        log.debug("Investing: instrument_id=%s (desde __NEXT_DATA__)", instrument_id)
                        break

                # Intentar extraer precios históricos inline (evita llamada a la API)
                price_array = _find_price_array(data)
                if price_array:
                    inline_prices = _extract_prices(price_array)
                    log.debug("Investing: %s precios inline en __NEXT_DATA__", len(inline_prices))

            except Exception as e:
                log.debug("Investing: Error parseando __NEXT_DATA__: %s", e)

        # Fallback regex si no se encontró instrument_id en __NEXT_DATA__
        if not instrument_id:
            for pattern in (
                r'"instrument_id"\s*:\s*"?(\d+)"?',
                r'"pair_id"\s*:\s*"?(\d+)"?',
                r'data-pair-id="(\d+)"',
            ):
                m = re.search(pattern, r.text)
                if m:
                    instrument_id = m.group(1)
                    log.debug("Investing: instrument_id=%s (desde regex)", instrument_id)
                    break

        if not instrument_id:
            log.warning("Investing: No se encontró instrument_id en %s", investing_url)

        return instrument_id, inline_prices

    except Exception as e:
        log.error("Investing: Error al obtener página %s: %s", investing_url, e, exc_info=True)
        return None, []


def _fetch_api(instrument_id: str, start: date, end: date) -> List[Tuple[str, float]]:
    """
    Llama a la API de investing.com. Intenta dos formatos de endpoint por robustez.
    Endpoint primario: /{id}/historical/chart/ con parámetros underscore (correcto).
    Endpoint secundario: /historical/{id} con parámetros hyphen (formato antiguo).
    """
    endpoints = [
        (
            f"https://api.investing.com/api/financialdata/{instrument_id}/historical/chart/",
            {
                "period": "custom",
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "time_frame": "Daily",
                "add_missing_rows": "false",
            },
        ),
        (
            f"https://api.investing.com/api/financialdata/historical/{instrument_id}",
            {
                "start-date": start.isoformat(),
                "end-date": end.isoformat(),
                "time-frame": "Daily",
                "add-missing-rows": "false",
            },
        ),
    ]
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "domain-id": "www.investing.com",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.investing.com/",
    }

    for api_url, params in endpoints:
        try:
            r = _get(api_url, params=params, headers=headers, timeout=30)
            log.debug("Investing API: status=%s url=%s", r.status_code, api_url)

            if r.status_code != 200:
                log.warning("Investing API: status=%s url=%s resp=%s",
                            r.status_code, api_url, r.text[:200])
                continue

            payload = r.json()

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

            if data_list:
                prices = _extract_prices(data_list)
                if prices:
                    log.debug("Investing API: %s precios para instrument_id=%s", len(prices), instrument_id)
                    return prices

        except Exception as e:
            log.warning("Investing API error: url=%s err=%s", api_url, e)

    return []


def scrape_investing_prices(
    session,  # mantenido por compatibilidad de firma; se usa cloudscraper internamente
    investing_url: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> List[Tuple[str, float]]:
    """
    Acepta la URL de la página histórica de investing.com.
    1. Fetcha la página con cloudscraper (bypass Cloudflare).
    2. Extrae precios inline de __NEXT_DATA__ si están disponibles.
    3. Si no, llama a la API con el endpoint correcto.
    Devuelve [(YYYY-MM-DD, close)].
    """
    if not investing_url:
        log.debug("Investing: URL vacía, se omite.")
        return []

    if _session is None:
        log.warning("Investing: cloudscraper no disponible. Instala: pip install cloudscraper>=1.2.71")
        return []

    end = end_date or date.today()
    start = date(2000, 1, 1) if full_refresh else (start_date or (end - timedelta(days=45)))

    # Paso 1 + 2: obtener página y precios inline
    instrument_id, inline_prices = _get_page_data(investing_url)

    # Paso 2b: si hay precios inline, filtrar por rango y devolverlos (sin API)
    if inline_prices:
        filtered = [(d, c) for d, c in inline_prices
                    if start.isoformat() <= d <= end.isoformat()]
        if filtered:
            log.debug("Investing: %s precios inline para %s", len(filtered), investing_url)
            return filtered

    # Paso 3: fallback a API si no hay precios inline
    if not instrument_id:
        return []

    return _fetch_api(instrument_id, start, end)
