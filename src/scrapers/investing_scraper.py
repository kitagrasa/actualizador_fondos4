from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import List, Tuple, Optional

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Proxy FlareProx: si está configurado, todas las peticiones pasan por él.
# Si no, se intenta conexión directa (fallback).
# ---------------------------------------------------------------------------
FLAREPROX_URL = os.environ.get("FLAREPROX_URL", "").strip()

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
# Petición HTTP con soporte para FlareProx y fallback a directo
# ---------------------------------------------------------------------------
def _request(method: str, url: str, **kwargs) -> Optional[str]:
    """
    Realiza una petición GET o POST, primero a través de FlareProx si está
    configurado, y si falla, directamente.
    """
    # Si tenemos FlareProx, redirigimos la petición a través de él
    if FLAREPROX_URL:
        # FlareProx espera recibir la URL destino como query parameter 'url'
        proxy_url = f"{FLAREPROX_URL}?url={url}"
        try:
            from curl_cffi import requests as curl_requests
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

    # Fallback: petición directa (si FlareProx no está o falló)
    try:
        from curl_cffi import requests as curl_requests
        resp = curl_requests.request(
            method=method,
            url=url,
            headers=BROWSER_HEADERS,
            impersonate="chrome124",
            timeout=30,
            **kwargs,
        )
        if resp.status_code == 200:
            return resp.text
        log.warning("Petición directa devolvió %s para %s", resp.status_code, url)
    except ImportError:
        import requests
        resp = requests.request(method=method, url=url, headers=BROWSER_HEADERS, timeout=30, **kwargs)
        if resp.status_code == 200:
            return resp.text
    except Exception as e:
        log.error("Error en petición directa: %s", e)

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
    Obtiene datos históricos de Investing.com.
    Usa FlareProx (si está configurado) para evitar bloqueos.
    """
    if not url or not isinstance(url, str) or not url.startswith("http"):
        return [], cached_pair_id

    pair_id = cached_pair_id
    historical_prices = []

    # 1. Si conocemos el pair_id, AJAX directo
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

            html = _request("POST", "https://www.investing.com/instruments/HistoricalDataAjax", data=payload)
            if html:
                soup = BeautifulSoup(html, "html.parser")
                table = soup.find("table", id="curr_table")
                if table:
                    historical_prices = _parse_investing_table(table)
                    if historical_prices:
                        log.info("Investing: %d precios vía AJAX (pair_id=%s)", len(historical_prices), pair_id)
                        return historical_prices, pair_id
        except Exception as e:
            log.warning("Investing: error AJAX con pair_id cacheado: %s", e)

    # 2. Obtener página principal (para extraer pair_id o como fallback)
    html = _request("GET", url)
    if not html:
        log.error("Investing: no se pudo cargar la página %s", url)
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

    # 3. Si conseguimos pair_id, intentamos AJAX de nuevo
    if pair_id and not historical_prices:
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

            ajax_html = _request("POST", "https://www.investing.com/instruments/HistoricalDataAjax", data=payload)
            if ajax_html:
                soup = BeautifulSoup(ajax_html, "html.parser")
                table = soup.find("table", id="curr_table")
                if table:
                    historical_prices = _parse_investing_table(table)
                    if historical_prices:
                        log.info("Investing: %d precios vía AJAX", len(historical_prices))
                        return historical_prices, pair_id
        except Exception as e:
            log.warning("Investing: fallo en AJAX: %s", e)

    # 4. Fallback: tabla HTML estática desde la página principal ya descargada
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"data-test": "historical-data-table"}) or soup.find("table", id="curr_table")
    if table:
        historical_prices = _parse_investing_table(table)
        log.info("Investing: %d precios desde HTML estático", len(historical_prices))
    else:
        log.warning("Investing: no se encontró tabla histórica en %s", url)

    return historical_prices, pair_id
