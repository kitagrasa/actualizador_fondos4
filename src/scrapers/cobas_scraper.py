from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import List, Tuple, Optional

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# Cookie de consentimiento de Cookiebot que garantiza que la web devuelva
# el HTML completo sin banners que puedan ocultar parte del contenido.
COOKIEBOT_CONSENT_COOKIE = (
    "CookieConsent="
    "{stamp:%27-1%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2C"
    "method:%27explicit%27%2Cver:1%2Cutc:1680000000000%2Cregion:%27es%27}"
)


def _extract_from_page(html: str) -> Optional[Tuple[str, float]]:
    """
    Extrae precio y fecha directamente de los elementos HTML de la ficha del producto.
    Busca el bloque <div class="each-data"> que contiene el valor liquidativo y
    el párrafo <p class="date"> con la fecha.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
        # 1) Localizar el precio
        # La estructura típica es:
        # <div class="each-data">
        #   <p class="number">176,540000 €</p>
        #   <p class="title">Valor liquidativo</p>
        # </div>
        price_elem = soup.select_one("div.each-data p.number")
        if not price_elem:
            log.warning("Cobas: no se encontró div.each-data p.number")
            return None

        raw_price = price_elem.get_text(strip=True)   # "176,540000 €"
        # Quitar cualquier cosa que no sea dígito, coma, punto
        price_str = re.sub(r"[^\d,]", "", raw_price).replace(",", ".")
        try:
            nav = float(price_str)
        except ValueError:
            log.warning("Cobas: no se pudo convertir precio: %s", raw_price)
            return None

        # 2) Fecha: <p class="date">Fecha valor liquidativo: 30-4-2026</p>
        date_elem = soup.find("p", class_="date")
        if not date_elem:
            log.warning("Cobas: no se encontró p.date")
            return None

        date_text = date_elem.get_text(strip=True)    # "Fecha valor liquidativo: 30-4-2026"
        # Extraer solo la parte después de ":"
        match = re.search(r":\s*(\d{1,2}-\d{1,2}-\d{4})", date_text)
        if not match:
            log.warning("Cobas: formato de fecha no reconocido en %s", date_text)
            return None

        raw_date = match.group(1)
        try:
            dt = datetime.strptime(raw_date, "%d-%m-%Y")
        except ValueError:
            # reintento con día/mes sin cero (ya debería funcionar con %d-%m-%Y)
            parts = raw_date.split("-")
            if len(parts) != 3:
                return None
            try:
                day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                dt = datetime(year, month, day)
            except Exception:
                return None

        return (dt.strftime("%Y-%m-%d"), nav)

    except Exception as e:
        log.error("Cobas: error en extracción HTML: %s", e, exc_info=True)
        return None


def scrape_cobas_prices(
    session,
    cobas_url: str,
) -> List[Tuple[str, float]]:
    """
    Obtiene el último valor liquidativo directamente de la ficha de producto
    de Cobas AM, extrayendo el HTML visible.

    Args:
        session: requests.Session con reintentos.
        cobas_url: URL de la ficha del producto (ej. .../lux_international_eur/).

    Returns:
        Lista de una tupla (fecha ISO "YYYY-MM-DD", precio) o lista vacía.
    """
    if not cobas_url or not cobas_url.startswith("http"):
        return []

    try:
        resp = session.get(
            cobas_url,
            headers={
                "Cookie": COOKIEBOT_CONSENT_COOKIE,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            },
            timeout=25,
        )
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        log.error("Cobas: error HTTP %s: %s", cobas_url, e)
        return []

    result = _extract_from_page(html)
    if result:
        log.info("Cobas: NAV obtenido de %s → %s = %s", cobas_url, result[0], result[1])
        return [result]

    log.warning("Cobas: no se pudo extraer el precio de %s", cobas_url)
    return []
