from __future__ import annotations

import logging
import random
import re
from datetime import date, datetime, timedelta
from typing import List, Tuple, Optional

import requests as req_lib
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lista de proxies gratuitos (HTTP) que se renueva en cada ejecución.
# Usamos la API pública https://proxylist.geonode.com/api/proxy-list
# ---------------------------------------------------------------------------
PROXY_LIST_URL = "https://proxylist.geonode.com/api/proxy-list?limit=50&page=1&sort_by=lastChecked&sort_type=desc&protocols=http&anonymityLevel=elite&anonymityLevel=anonymous"

_proxy_cache: List[str] = []
_proxy_index = 0


def _refresh_proxies() -> None:
    """Obtiene una lista de proxies HTTP fresca y la guarda en caché."""
    global _proxy_cache, _proxy_index
    _proxy_cache = []
    _proxy_index = 0
    try:
        resp = req_lib.get(PROXY_LIST_URL, timeout=10)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            for item in data:
                ip = item.get("ip")
                port = item.get("port")
                if ip and port:
                    _proxy_cache.append(f"http://{ip}:{port}")
            random.shuffle(_proxy_cache)
            log.info("Investing: %d proxies obtenidos", len(_proxy_cache))
    except Exception as e:
        log.warning("Investing: no se pudo actualizar lista de proxies: %s", e)


def _get_next_proxy() -> Optional[str]:
    """Devuelve el siguiente proxy de la caché, o None si se agotaron."""
    global _proxy_index
    if not _proxy_cache:
        _refresh_proxies()
    if _proxy_index >= len(_proxy_cache):
        return None
    proxy = _proxy_cache[_proxy_index]
    _proxy_index += 1
    return proxy


# ---------------------------------------------------------------------------
# Headers realistas (sin cookies de sesión, las añade curl_cffi autom.)
# ---------------------------------------------------------------------------
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


def _request_with_retry(
    method: str,
    url: str,
    **kwargs,
) -> Optional[req_lib.Response]:
    """Realiza una petición GET o POST probando con diferentes proxies."""
    max_retries = len(_proxy_cache) + 1  # proxies + directo

    for attempt in range(max_retries):
        proxy = _get_next_proxy()
        if proxy is None and attempt > 0:
            # ya probamos todos los proxies, intentamos sin proxy
            log.info("Investing: intentando sin proxy (último recurso)")
        try:
            from curl_cffi import requests as curl_requests
            # Si hay proxy, se pasa como diccionario {'http': ..., 'https': ...}
            proxies = None
            if proxy:
                proxies = {"http": proxy, "https": proxy}
            resp = curl_requests.request(
                method=method,
                url=url,
                headers=BROWSER_HEADERS,
                impersonate="chrome124",
                timeout=30,
                proxies=proxies,
                **kwargs,
            )
            if resp.status_code != 403:
                resp.raise_for_status()
                return resp
            log.debug("Investing: 403 con proxy %s", proxy)
        except ImportError:
            # Fallback a requests estándar si curl_cffi no está
            proxies = {"http": proxy, "https": proxy} if proxy else None
            resp = req_lib.request(
                method=method,
                url=url,
                headers=BROWSER_HEADERS,
                timeout=30,
                proxies=proxies,
                **kwargs,
            )
            if resp.status_code == 403:
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            log.debug("Investing: error con proxy %s: %s", proxy, e)

    log.warning("Investing: no se pudo obtener respuesta válida tras %d intentos", max_retries)
    return None


# ---------------------------------------------------------------------------
# Parseo de tabla histórica
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
    session,                           # ignorado
    url: str,
    cached_pair_id: Optional[str] = None,
    startdate: Optional[date] = None,
    enddate: Optional[date] = None,
    fullrefresh: bool = False
) -> Tuple[List[Tuple[str, float]], Optional[str]]:
    """
    Obtiene datos históricos de Investing.com utilizando proxies gratuitos
    rotativos para evitar el bloqueo 403.
    """
    if not url or not isinstance(url, str) or not url.startswith("http"):
        return [], cached_pair_id

    pair_id = cached_pair_id
    historical_prices = []

    # 1. AJAX si ya conocemos pair_id
    if pair_id:
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

            resp = _request_with_retry(
                "POST",
                "https://www.investing.com/instruments/HistoricalDataAjax",
                data=payload,
            )
            if resp and resp.text:
                soup = BeautifulSoup(resp.text, "html.parser")
                table = soup.find("table", id="curr_table")
                if table:
                    historical_prices = _parse_investing_table(table)
                    if historical_prices:
                        log.info("Investing: %d precios vía AJAX", len(historical_prices))
                        return historical_prices, pair_id
        except Exception as e:
            log.warning("Investing: error AJAX con pair_id cacheado: %s", e)

    # 2. Obtener página principal
    resp = _request_with_retry("GET", url)
    if not resp or not resp.text:
        log.error("Investing: no se pudo cargar la página %s", url)
        return [], pair_id

    html = resp.text

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

    # 3. AJAX con el pair_id recién obtenido
    if pair_id:
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

            resp = _request_with_retry(
                "POST",
                "https://www.investing.com/instruments/HistoricalDataAjax",
                data=payload,
            )
            if resp and resp.text:
                soup = BeautifulSoup(resp.text, "html.parser")
                table = soup.find("table", id="curr_table")
                if table:
                    historical_prices = _parse_investing_table(table)
                    if historical_prices:
                        log.info("Investing: %d precios vía AJAX", len(historical_prices))
                        return historical_prices, pair_id
        except Exception as e:
            log.warning("Investing: fallo en AJAX: %s", e)

    # 4. Fallback: tabla HTML estática
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"data-test": "historical-data-table"}) or soup.find("table", id="curr_table")
    if table:
        historical_prices = _parse_investing_table(table)
        log.info("Investing: %d precios desde HTML estático", len(historical_prices))
    else:
        log.warning("Investing: no se encontró tabla histórica en %s", url)

    return historical_prices, pair_id
