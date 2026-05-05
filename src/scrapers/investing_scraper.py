from __future__ import annotations

import logging
import os
import re
import time
from datetime import date, datetime, timedelta
from typing import List, Tuple, Optional

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Proxy FlareProx: si está configurado, todas las peticiones pasan por él.
# Si no, se intenta conexión directa con curl_cffi.
# ---------------------------------------------------------------------------
FLAREPROX_URL = os.environ.get("FLAREPROX_URL", "").strip()

# Cabeceras base para imitar un navegador real
BROWSER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# ---------------------------------------------------------------------------
# Función auxiliar para peticiones con sesión persistente
# ---------------------------------------------------------------------------
def _request_with_session(
    method: str,
    url: str,
    session=None,          # curl_cffi.Session (opcional)
    **kwargs,
) -> Optional[str]:
    """
    Realiza una petición usando curl_cffi con sesión persistente.
    Si FlareProx está configurado, redirige la petición a través de él.
    """
    # 1. Si tenemos FlareProx, redirigimos
    if FLAREPROX_URL:
        proxy_url = f"{FLAREPROX_URL}?url={url}"
        try:
            from curl_cffi import requests as curl_requests
            # Usar una sesión nueva para cada petición al proxy
            resp = curl_requests.request(
                method=method,
                url=proxy_url,
                headers=BROWSER_HEADERS,
                impersonate="chrome124",
                timeout=30,
                **kwargs,
            )
            if resp.status_code == 200:
                return resp.text
            log.warning("FlareProx devolvió %s para %s", resp.status_code, url)
        except Exception as e:
            log.warning("Error con FlareProx: %s", e)

    # 2. Si no hay FlareProx o falló, usar curl_cffi directo con sesión
    try:
        from curl_cffi import requests as curl_requests

        # Si no se proporciona una sesión, crear una nueva
        if session is None:
            session = curl_requests.Session()

        # Configurar la sesión con las cabeceras base
        session.headers.update(BROWSER_HEADERS)

        resp = session.request(
            method=method,
            url=url,
            impersonate="chrome124",
            timeout=30,
            **kwargs,
        )
        if resp.status_code == 200:
            return resp.text
        log.warning("Petición directa devolvió %s para %s", resp.status_code, url)
        return None
    except ImportError:
        # Fallback: requests estándar (poco probable que funcione contra Cloudflare)
        import requests
        s = requests.Session()
        s.headers.update(BROWSER_HEADERS)
        try:
            resp = s.request(method=method, url=url, timeout=30, **kwargs)
            if resp.status_code == 200:
                return resp.text
            log.warning("Fallback requests devolvió %s para %s", resp.status_code, url)
        except Exception as e:
            log.error("Error en fallback requests: %s", e)
    except Exception as e:
        log.error("Error en petición directa: %s", e)

    return None

# ---------------------------------------------------------------------------
# Parseo de tabla histórica (sin cambios)
# ---------------------------------------------------------------------------
def _parse_investing_table(table: BeautifulSoup) -> List[Tuple[str, float]]:
    prices = []
    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]

    for row in rows:
        cols = row.find_all(["td", "th"])
        if len(cols) < 2:
            continue

        raw_date = cols[0].get_text(strip=True).replace(",", "")
        raw_price = cols[1].get_text(strip=True)

        parsed_date = None
        try:
            if "-" in raw_date:
                parsed_date = datetime.strptime(raw_date, "%Y-%m-%d").strftime("%Y-%m-%d")
            elif "." in raw_date:
                parsed_date = datetime.strptime(raw_date, "%d.%m.%Y").strftime("%Y-%m-%d")
            elif "/" in raw_date:
                parsed_date = datetime.strptime(raw_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            else:
                month_es_en = {
                    "Ene": "Jan", "Feb": "Feb", "Mar": "Mar", "Abr": "Apr", "May": "May", "Jun": "Jun",
                    "Jul": "Jul", "Ago": "Aug", "Sep": "Sep", "Oct": "Oct", "Nov": "Nov", "Dic": "Dec"
                }
                for es, en in month_es_en.items():
                    raw_date = raw_date.replace(es, en)
                parsed_date = datetime.strptime(raw_date, "%b %d %Y").strftime("%Y-%m-%d")
        except ValueError:
            continue

        if not parsed_date or not raw_price or raw_price == "-":
            continue

        try:
            if "." in raw_price and "," in raw_price:
                if raw_price.rfind(".") > raw_price.rfind(","):
                    val = float(raw_price.replace(",", ""))
                else:
                    val = float(raw_price.replace(".", "").replace(",", "."))
            elif "," in raw_price:
                val = float(raw_price.replace(",", "."))
            else:
                val = float(raw_price)
            prices.append((parsed_date, val))
        except ValueError:
            continue

    return prices

# ---------------------------------------------------------------------------
# Scraper principal
# ---------------------------------------------------------------------------
def scrape_investing_prices(
    session,                           # ignorado (usamos curl_cffi internamente)
    url: str,
    cached_pair_id: Optional[str] = None,
    startdate: Optional[date] = None,
    enddate: Optional[date] = None,
    fullrefresh: bool = False
) -> Tuple[List[Tuple[str, float]], Optional[str]]:
    """
    Obtiene datos históricos de Investing.com.
    Utiliza una sesión persistente de curl_cffi para mantener cookies
    y evitar bloqueos de Cloudflare.
    """
    if not url or not isinstance(url, str) or not url.startswith("http"):
        return [], cached_pair_id

    pair_id = cached_pair_id
    historical_prices = []

    # Creamos una sesión curl_cffi que conservará cookies
    try:
        from curl_cffi import requests as curl_requests
        curl_session = curl_requests.Session()
        curl_session.headers.update(BROWSER_HEADERS)
    except ImportError:
        curl_session = None
        log.warning("curl_cffi no disponible, se usará requests (puede fallar)")

    # 1. Si conocemos el pair_id, intentamos AJAX directo (con reintentos)
    if pair_id:
        for attempt in range(2):  # 2 intentos con backoff
            try:
                end_dt = enddate or date.today()
                start_dt = startdate or (end_dt - timedelta(days=730))
                if fullrefresh:
                    start_dt = date(2000, 1, 1)

                payload = {
                    "curr_id": pair_id,
                    "st_date": start_dt.strftime("%m/%d/%Y"),
                    "end_date": end_dt.strftime("%m/%d/%Y"),
                    "interval_sec": "Daily",
                    "sort_col": "date",
                    "sort_ord": "DESC",
                    "action": "historical_data"
                }

                html = _request_with_session(
                    "POST",
                    "https://www.investing.com/instruments/HistoricalDataAjax",
                    session=curl_session,
                    data=payload,
                )
                if html:
                    soup = BeautifulSoup(html, "html.parser")
                    table = soup.find("table", id="curr_table")
                    if table:
                        historical_prices = _parse_investing_table(table)
                        if historical_prices:
                            log.info("Investing: %d precios vía AJAX (pair_id=%s)", len(historical_prices), pair_id)
                            return historical_prices, pair_id
            except Exception as e:
                log.warning("Investing: error AJAX intento %d: %s", attempt+1, e)
                time.sleep(2 ** attempt)  # backoff: 1s, 2s

    # 2. Obtener página principal para extraer pair_id o como fallback
    for attempt in range(2):
        html = _request_with_session("GET", url, session=curl_session)
        if html:
            break
        log.warning("Intento %d fallido para obtener página principal", attempt+1)
        time.sleep(2 ** attempt)
    else:
        log.error("Investing: no se pudo cargar la página %s tras varios intentos", url)
        return [], pair_id

    # Extraer pair_id si no lo teníamos
    if not pair_id:
        match = re.search(r'histDataExcessInfo\s*[=:]\s*\{[^}]*?pairId["\'\s:=]+(?P<pair>\d{3,10})', html)
        if match:
            pair_id = match.group("pair")
        else:
            match_alt = re.search(r'data-pair-id=["\']?(?P<pair>\d+)["\']?', html)
            if match_alt:
                pair_id = match_alt.group("pair")
        if pair_id:
            log.info("Investing: pair_id extraído: %s", pair_id)

    # 3. Si conseguimos pair_id, intentamos AJAX de nuevo (con la sesión que ya tiene cookies)
    if pair_id and not historical_prices:
        for attempt in range(2):
            try:
                end_dt = enddate or date.today()
                start_dt = startdate or (end_dt - timedelta(days=730))
                if fullrefresh:
                    start_dt = date(2000, 1, 1)

                payload = {
                    "curr_id": pair_id,
                    "st_date": start_dt.strftime("%m/%d/%Y"),
                    "end_date": end_dt.strftime("%m/%d/%Y"),
                    "interval_sec": "Daily",
                    "sort_col": "date",
                    "sort_ord": "DESC",
                    "action": "historical_data"
                }

                ajax_html = _request_with_session(
                    "POST",
                    "https://www.investing.com/instruments/HistoricalDataAjax",
                    session=curl_session,
                    data=payload,
                )
                if ajax_html:
                    soup = BeautifulSoup(ajax_html, "html.parser")
                    table = soup.find("table", id="curr_table")
                    if table:
                        historical_prices = _parse_investing_table(table)
                        if historical_prices:
                            log.info("Investing: %d precios vía AJAX (con sesión)", len(historical_prices))
                            return historical_prices, pair_id
            except Exception as e:
                log.warning("Investing: fallo en AJAX intento %d: %s", attempt+1, e)
                time.sleep(2 ** attempt)

    # 4. Fallback: tabla HTML estática desde la página principal ya descargada
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"data-test": "historical-data-table"}) or soup.find("table", id="curr_table")
    if table:
        historical_prices = _parse_investing_table(table)
        log.info("Investing: %d precios desde HTML estático", len(historical_prices))
    else:
        log.warning("Investing: no se encontró tabla histórica en %s", url)

    return historical_prices, pair_id
