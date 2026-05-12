from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import List, Tuple, Optional

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("scrapers.polar_capital")

# Posibles puntos de entrada para obtener los precios históricos
# Se probarán en orden hasta que uno devuelva datos.
CANDIDATE_ENDPOINTS = [
    # 1) Recarga de parte "Prices" con el id de la parte de la pestaña "Historical Prices"
    # Parámetros: part_id=11938 (el id de la parte Prices en historical-prices), fund_id=67
    "/srp/part/11938/reload/?fund_id=67",
    # 2) Ruta REST hipotética para precios históricos
    "/srp/prices/historical-prices/?fund_id=67&isin=IE00BZ4D7648",
    # 3) Otra variante: vista de página con parámetro de fondo (puede devolver HTML completo)
    "/es/private/Literature-and-Prices/Fund-Prices/?fund_id=67&tab=historical-prices",
]


def _extract_prices_from_html(html: str) -> List[Tuple[str, float]]:
    """
    Intenta extraer la tabla de precios históricos si la respuesta es HTML.
    Busca filas (<tr>) dentro de la tabla con clase 'prices-table'.
    """
    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one("table.prices-table")
    if not table:
        return []
    prices = []
    for tr in table.select("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        date_raw = tds[0].get_text(strip=True)
        price_raw = tds[-2].get_text(strip=True) if len(tds) >= 5 else tds[1].get_text(strip=True)
        # Formato de fecha esperado: DD/MM/YYYY o YYYY-MM-DD
        # Intentamos parsear
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_raw, fmt).date()
                break
            except ValueError:
                continue
        else:
            continue
        # Limpieza de precio: quitar símbolos de moneda y comas europeas
        price_str = re.sub(r"[^\d,\.\-]", "", price_raw).replace(",", ".")
        try:
            price = float(price_str)
        except ValueError:
            continue
        prices.append((dt.isoformat(), price))
    return prices


def _try_api_endpoints(session: requests.Session, base_url: str, fund_id: int) -> Optional[List[Tuple[str, float]]]:
    """
    Prueba los endpoints candidatos y devuelve precios si alguno responde con datos parseables.
    """
    common_headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": base_url,
    }
    for endpoint in CANDIDATE_ENDPOINTS:
        url = f"{base_url}{endpoint}" if not endpoint.startswith("http") else endpoint
        log.info("Probando endpoint: %s", url)
        try:
            resp = session.get(url, headers=common_headers, timeout=25)
            if resp.status_code != 200:
                continue
            content_type = resp.headers.get("Content-Type", "")
            # Si es JSON, intentamos parsear
            if "json" in content_type:
                data = resp.json()
                prices = _parse_json_prices(data)
                if prices:
                    return prices
            # Si es HTML, probamos a extraer tabla
            else:
                prices = _extract_prices_from_html(resp.text)
                if prices:
                    return prices
        except Exception as e:
            log.debug("Error al probar endpoint %s: %s", url, e)
            continue
    return None


def _parse_json_prices(data) -> List[Tuple[str, float]]:
    """
    Espera un JSON con estructura como:
    {"prices": [{"date": "2026-01-02", "nav": 32.76}, ...]} o similar.
    """
    if not isinstance(data, dict):
        return []
    # Buscar una lista de precios en el objeto
    prices_list = data.get("prices") or data.get("data") or data.get("historicalPrices") or data.get("items")
    if isinstance(prices_list, list):
        out = []
        for item in prices_list:
            if not isinstance(item, dict):
                continue
            d = item.get("date") or item.get("navDate") or item.get("valueDate")
            c = item.get("nav") or item.get("price") or item.get("close") or item.get("value")
            if d and c is not None:
                # Normalizar fecha a ISO
                try:
                    # Intentar parsear formato DD/MM/YYYY
                    dt = datetime.strptime(d, "%d/%m/%Y").date()
                except ValueError:
                    try:
                        dt = datetime.fromisoformat(d).date()
                    except Exception:
                        continue
                try:
                    price = float(str(c).replace(",", "."))
                except ValueError:
                    continue
                out.append((dt.isoformat(), price))
        return out
    # Si la respuesta es directamente una lista
    if isinstance(data, list):
        out = []
        for item in data:
            if isinstance(item, dict):
                d = item.get("date") or item.get("navDate")
                c = item.get("nav") or item.get("price")
                if d and c is not None:
                    try:
                        dt = datetime.strptime(d, "%d/%m/%Y").date()
                    except:
                        continue
                    try:
                        price = float(str(c).replace(",", "."))
                    except:
                        continue
                    out.append((dt.isoformat(), price))
        return out
    return []


def scrape_polar_capital_prices(
    session: requests.Session,
    polar_url: str,
) -> List[Tuple[str, float]]:
    """
    Scraper para precios del fondo Polar Capital Global Technology Fund.
    Intenta obtener los precios históricos desde la web de Polar Capital.

    Parámetros:
        session: Sesión HTTP con reintentos.
        polar_url: URL base de la página de precios.

    Retorna:
        Lista de tuplas (fecha_YYYY-MM-DD, precio_float) o lista vacía si falla.
    """
    if not polar_url or not polar_url.startswith("http"):
        return []

    # Extraer el fund_id del ISIN IE00BZ4D7648. De momento lo fijamos a 67.
    fund_id = 67

    # Intentar primero los endpoints API
    prices = _try_api_endpoints(session, polar_url, fund_id)
    if prices:
        log.info("Polar Capital: obtenidos %d precios vía API.", len(prices))
        return prices

    # Fallback: usar curl_cffi para simular navegador y parsear HTML completo.
    try:
        from curl_cffi import requests as curl_requests
    except ImportError:
        log.warning("curl_cffi no disponible, omitiendo fallback.")
        return []

    log.info("Probando fallback con curl_cffi (impersonate=chrome110)...")
    try:
        resp = curl_requests.get(
            polar_url,
            impersonate="chrome110",
            timeout=30,
            headers={
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            },
        )
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        log.error("curl_cffi falló: %s", e)
        return []

    # Intentar extraer tabla de precios del HTML
    prices = _extract_prices_from_html(html)
    if prices:
        log.info("Polar Capital (curl_cffi): %d precios extraídos.", len(prices))
        return prices

    # Como último recurso, buscar en algún <script> un array JSON con los precios.
    soup = BeautifulSoup(html, "lxml")
    for script in soup.find_all("script"):
        if not script.string:
            continue
        try:
            # Buscar objeto que contenga "nav" o "price"
            match = re.search(r'(?:prices|historicalPrices)\s*[:=]\s*(\[.*?\])\s*[,;]', script.string, re.DOTALL)
            if match:
                json_str = match.group(1)
                data = json.loads(json_str)
                if isinstance(data, list):
                    prices = _parse_json_prices(data)
                    if prices:
                        log.info("Polar Capital: precios encontrados en script embebido (%d)", len(prices))
                        return prices
        except Exception:
            continue

    log.warning("Polar Capital: no se pudo obtener ningún precio para el fondo %s.", fund_id)
    return []


# Función de prueba manual
def test_scraper():
    import sys
    from src.http_client import build_session

    logging.basicConfig(level=logging.DEBUG)
    session = build_session()
    url = "https://www.polarcapitalfunds.com/es/private/Literature-and-Prices/Fund-Prices/"
    precios = scrape_polar_capital_prices(session, url)
    print(f"Se obtuvieron {len(precios)} precios:")
    for fecha, nav in precios[:5]:
        print(f"  {fecha}: {nav}")
    if not precios:
        print("No se obtuvo ningún precio. Revisa los logs para intentar los endpoints.")


if __name__ == "__main__":
    test_scraper()
