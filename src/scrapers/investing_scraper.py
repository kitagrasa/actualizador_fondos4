from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import List, Tuple, Optional

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Headers que imitan exactamente a un navegador Chrome real.
# El orden es importante: Investing.com rechaza peticiones si los headers
# no siguen la secuencia típica de un navegador.
# ---------------------------------------------------------------------------
BROWSER_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# ---------------------------------------------------------------------------
# Helper: obtener una sesión con curl_cffi
# ---------------------------------------------------------------------------

def _get(url: str, headers: dict = None, timeout: int = 25) -> Optional[str]:
    """
    Realiza una petición GET usando curl_cffi con impersonate de Chrome.
    Si curl_cffi no está disponible, retrocede a requests estándar.
    """
    final_headers = {**BROWSER_HEADERS, **(headers or {})}
    try:
        from curl_cffi import requests as curl_requests
        resp = curl_requests.get(
            url,
            headers=final_headers,
            impersonate="chrome124",
            timeout=timeout,
        )
        if resp.status_code == 403:
            log.warning("Investing: curl_cffi también recibe 403 — posible bloqueo de IP")
            return None
        resp.raise_for_status()
        return resp.text
    except ImportError:
        log.warning("Investing: curl_cffi no disponible, usando requests estándar")
        import requests
        resp = requests.get(url, headers=final_headers, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        log.error("Investing: error en petición a %s: %s", url, e)
        return None


def _post(url: str, data: dict, headers: dict = None, timeout: int = 25) -> Optional[str]:
    """POST con curl_cffi (misma lógica que _get)."""
    final_headers = {
        **BROWSER_HEADERS,
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Requested-With": "XMLHttpRequest",
        **(headers or {}),
    }
    try:
        from curl_cffi import requests as curl_requests
        resp = curl_requests.post(
            url,
            data=data,
            headers=final_headers,
            impersonate="chrome124",
            timeout=timeout,
        )
        if resp.status_code == 403:
            log.warning("Investing: curl_cffi POST también recibe 403")
            return None
        resp.raise_for_status()
        return resp.text
    except ImportError:
        import requests
        resp = requests.post(url, data=data, headers=final_headers, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        log.error("Investing: error en POST a %s: %s", url, e)
        return None


# ---------------------------------------------------------------------------
# Parseo de tabla de Investing
# ---------------------------------------------------------------------------

def _parse_investing_table(table: BeautifulSoup) -> List[Tuple[str, float]]:
    """Helper para extraer las fechas y precios de cualquier tabla de Investing."""
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
    session,                                       # ← se ignora, usamos curl_cffi
    url: str,
    cached_pair_id: Optional[str] = None,
    startdate: Optional[date] = None,
    enddate: Optional[date] = None,
    fullrefresh: bool = False
) -> Tuple[List[Tuple[str, float]], Optional[str]]:
    """
    Obtiene datos históricos de Investing.com usando curl_cffi para evitar
    el bloqueo 403 por TLS fingerprinting.

    Retorna (lista de tuplas (fecha, precio), pair_id).
    """
    if not url or not isinstance(url, str) or not url.startswith("http"):
        return [], cached_pair_id

    # 1. Obtener HTML de la página del producto
    html = _get(url)
    if not html:
        log.error("Investing: no se pudo cargar %s", url)
        return [], cached_pair_id

    # 2. Extraer pair_id (necesario para el AJAX)
    pair_id = cached_pair_id
    if not pair_id:
        match = re.search(r'histDataExcessInfo\s*[=:]\s*\{[^}]*?pairId["\'\s:=]+(?P<pair>\d{3,10})', html)
        if match:
            pair_id = match.group("pair")
        else:
            match_alt = re.search(r'data-pair-id=["\']?(?P<pair>\d+)["\']?', html)
            if match_alt:
                pair_id = match_alt.group("pair")

    historical_prices = []

    # 3. AJAX para histórico profundo
    if pair_id:
        try:
            end_dt = enddate or date.today()
            start_dt = startdate or (end_dt - timedelta(days=730))
            if fullrefresh:
                start_dt = date(2000, 1, 1)

            ajax_url = "https://www.investing.com/instruments/HistoricalDataAjax"
            payload = {
                "curr_id": pair_id,
                "st_date": start_dt.strftime("%m/%d/%Y"),
                "end_date": end_dt.strftime("%m/%d/%Y"),
                "interval_sec": "Daily",
                "sort_col": "date",
                "sort_ord": "DESC",
                "action": "historical_data"
            }

            ajax_html = _post(ajax_url, data=payload)
            if ajax_html:
                soup_ajax = BeautifulSoup(ajax_html, "html.parser")
                table_ajax = soup_ajax.find("table", id="curr_table")
                if table_ajax:
                    historical_prices = _parse_investing_table(table_ajax)
                    if historical_prices:
                        log.info("Investing: obtenidos %d precios vía AJAX", len(historical_prices))
                        return historical_prices, pair_id

        except Exception as e:
            log.warning("Investing: fallo en llamada AJAX, probando tabla estática... (%s)", e)

    # 4. Fallback: tabla HTML estática
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"data-test": "historical-data-table"}) or soup.find("table", id="curr_table")

    if table:
        historical_prices = _parse_investing_table(table)
        log.info("Investing: obtenidos %d precios de tabla HTML estática", len(historical_prices))
    else:
        log.warning("Investing: no se encontró tabla histórica en %s", url)

    return historical_prices, pair_id
