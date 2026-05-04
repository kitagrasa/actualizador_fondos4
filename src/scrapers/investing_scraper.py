from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import List, Tuple, Optional

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cookies de consentimiento que evitan el banner de OneTrust y permiten
# que la web cargue el contenido completo (datos históricos).
# Se generan dinámicamente con la fecha actual para mayor realismo.
# ---------------------------------------------------------------------------
def _build_consent_cookies() -> str:
    now = datetime.utcnow()
    datestamp = now.strftime("%a %b %d %Y %H:%M:%S GMT%z (Coordinated Universal Time)")
    # Valor típico de OptanonConsent con todos los grupos habilitados
    optanon = (
        f"isGpcEnabled=0&datestamp={datestamp}&version=6.35.0&isIABGlobal=false"
        "&hosts=&consentId=00000000-0000-0000-0000-000000000000"
        "&interactionCount=1&landingPath=NotLandingPage"
        "&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1"
    )
    # Cierre del banner de consentimiento
    optanon_alert = now.isoformat() + "Z"
    return f"OptanonConsent={optanon}; OptanonAlertBoxClosed={optanon_alert}"


# ---------------------------------------------------------------------------
# Headers que imitan un navegador real (importante para evitar bloqueos)
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
# Funciones auxiliares de petición HTTP con curl_cffi y cookies de consentimiento
# ---------------------------------------------------------------------------

def _get(url: str, referer: str = None, timeout: int = 25) -> Optional[str]:
    """
    Realiza una petición GET con curl_cffi (impersonate Chrome 124).
    Incluye cookies de consentimiento para evitar el banner de OneTrust.
    """
    headers = {**BROWSER_HEADERS}
    if referer:
        headers["Referer"] = referer
    # Añadir cookies de consentimiento
    headers["Cookie"] = _build_consent_cookies()

    try:
        from curl_cffi import requests as curl_requests
        resp = curl_requests.get(
            url,
            headers=headers,
            impersonate="chrome124",
            timeout=timeout,
        )
        if resp.status_code == 403:
            log.warning("Investing: GET %s -> 403 (posible bloqueo de IP)", url)
            return None
        resp.raise_for_status()
        return resp.text
    except ImportError:
        log.warning("Investing: curl_cffi no disponible, usando requests estándar")
        import requests
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        log.error("Investing: error en GET %s: %s", url, e)
        return None


def _post(url: str, data: dict, referer: str = None, timeout: int = 25) -> Optional[str]:
    """POST con curl_cffi y cookies de consentimiento."""
    headers = {
        **BROWSER_HEADERS,
        "Content-Type": "application/x-www-form-urlencoded",
        "X-Requested-With": "XMLHttpRequest",
    }
    if referer:
        headers["Referer"] = referer
    headers["Cookie"] = _build_consent_cookies()

    try:
        from curl_cffi import requests as curl_requests
        resp = curl_requests.post(
            url,
            data=data,
            headers=headers,
            impersonate="chrome124",
            timeout=timeout,
        )
        if resp.status_code == 403:
            log.warning("Investing: POST %s -> 403", url)
            return None
        resp.raise_for_status()
        return resp.text
    except ImportError:
        import requests
        resp = requests.post(url, data=data, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        log.error("Investing: error en POST %s: %s", url, e)
        return None


# ---------------------------------------------------------------------------
# Parseo de la tabla de datos históricos
# ---------------------------------------------------------------------------

def _parse_investing_table(table: BeautifulSoup) -> List[Tuple[str, float]]:
    """Extrae las fechas y precios de cualquier tabla de Investing."""
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
    session,          # ignorado (usamos curl_cffi)
    url: str,
    cached_pair_id: Optional[str] = None,
    startdate: Optional[date] = None,
    enddate: Optional[date] = None,
    fullrefresh: bool = False
) -> Tuple[List[Tuple[str, float]], Optional[str]]:
    """
    Obtiene datos históricos de Investing.com.
    - Si existe pair_id en caché, salta la página principal y ataca directamente
      el endpoint AJAX con cookies de consentimiento.
    - Si no, obtiene la página principal (con cookies) para extraer el pair_id
      y luego usa AJAX.
    - Fallback: parsea la tabla HTML estática si el AJAX falla y tenemos la página.
    """
    if not url or not isinstance(url, str) or not url.startswith("http"):
        return [], cached_pair_id

    pair_id = cached_pair_id
    historical_prices = []

    # 1. Si tenemos pair_id en caché, intentamos directamente AJAX
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

            ajax_html = _post(ajax_url, data=payload, referer=url)
            if ajax_html:
                soup_ajax = BeautifulSoup(ajax_html, "html.parser")
                table_ajax = soup_ajax.find("table", id="curr_table")
                if table_ajax:
                    historical_prices = _parse_investing_table(table_ajax)
                    if historical_prices:
                        log.info("Investing: obtenidos %d precios vía AJAX (caché pair_id=%s)", len(historical_prices), pair_id)
                        return historical_prices, pair_id
            log.warning("Investing: AJAX falló aun con pair_id en caché, se intentará la página principal")
        except Exception as e:
            log.warning("Investing: error en AJAX con pair_id cacheado: %s", e)

    # 2. Si no hay pair_id o el AJAX falló, obtener la página principal para extraer pair_id
    html = _get(url)
    if not html:
        # No pudimos cargar la página, pero si ya teníamos pair_id devolvemos lo que haya (vacío)
        log.error("Investing: no se pudo cargar la página %s", url)
        return [], pair_id

    # Extraer pair_id si aún no lo tenemos
    if not pair_id:
        match = re.search(r'histDataExcessInfo\s*[=:]\s*\{[^}]*?pairId["\'\s:=]+(?P<pair>\d{3,10})', html)
        if match:
            pair_id = match.group("pair")
        else:
            match_alt = re.search(r'data-pair-id=["\']?(?P<pair>\d+)["\']?', html)
            if match_alt:
                pair_id = match_alt.group("pair")
        if pair_id:
            log.info("Investing: pair_id extraído de la página: %s", pair_id)

    # 3. Intentar AJAX con el pair_id (si lo tenemos)
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

            ajax_html = _post(ajax_url, data=payload, referer=url)
            if ajax_html:
                soup_ajax = BeautifulSoup(ajax_html, "html.parser")
                table_ajax = soup_ajax.find("table", id="curr_table")
                if table_ajax:
                    historical_prices = _parse_investing_table(table_ajax)
                    if historical_prices:
                        log.info("Investing: obtenidos %d precios vía AJAX", len(historical_prices))
                        return historical_prices, pair_id
        except Exception as e:
            log.warning("Investing: fallo en llamada AJAX tras obtener página: %s", e)

    # 4. Fallback: tabla HTML estática (si tenemos la página)
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"data-test": "historical-data-table"}) or soup.find("table", id="curr_table")
    if table:
        historical_prices = _parse_investing_table(table)
        log.info("Investing: obtenidos %d precios de tabla HTML estática", len(historical_prices))
    else:
        log.warning("Investing: no se encontró tabla histórica en %s", url)

    return historical_prices, pair_id
