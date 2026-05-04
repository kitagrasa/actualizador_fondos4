from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import List, Tuple, Optional

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cookie de consentimiento de Cookiebot que indica que se han aceptado
# todas las categorías de cookies (necesarias, preferencias, estadísticas,
# marketing). Esto permite que la web de Cobas AM devuelva el HTML completo
# en lugar del banner de cookies.
# ---------------------------------------------------------------------------
COOKIEBOT_CONSENT_COOKIE = (
    "CookieConsent="
    "{stamp:%27-1%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2C"
    "method:%27explicit%27%2Cver:1%2Cutc:1680000000000%2Cregion:%27es%27}"
)


# ---------------------------------------------------------------------------
def _extract_product_id_from_url(url: str) -> Optional[str]:
    """
    Del path de la página de producto extrae el identificador.
    Ejemplo:  .../lux_international_eur/  →  LUX_INTERNATIONAL_EUR
    """
    if not url:
        return None
    # Tomamos el último fragmento de la ruta que no esté vacío
    parts = [p for p in url.strip("/").split("/") if p]
    if not parts:
        return None
    return parts[-1].upper()


def _current_price_from_html(html: str, product_id: str) -> Optional[Tuple[str, float]]:
    """
    Parsea el bloque <script id="product-block"> y extrae el
    liquidative_value + liquidative_date para el producto dado.
    Retorna (fecha_iso, nav) o None.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
        script = soup.find("script", id="product-block")
        if not script or not script.string:
            log.warning("Cobas: no se encontró <script id='product-block'>")
            return None

        # El script tiene forma: window.products = JSON.parse(`...`);
        # El contenido puede estar escapado con \" y otros.
        match = re.search(r"JSON\.parse\(`(.*?)`\)", script.string, re.DOTALL)
        if not match:
            log.warning("Cobas: no se pudo extraer el JSON del product-block")
            return None

        raw_json = match.group(1)
        # Limpiar escapes típicos de plantillas literales de JS
        cleaned = raw_json.replace('\\"', '"').replace('\\\\', '\\')
        data = json.loads(cleaned)
        products = data.get("data", [])

        for prod in products:
            if prod.get("key") != product_id:
                continue

            info = prod.get("product_profit_info", {})
            raw_value = (info.get("liquidative_value") or "").strip()
            raw_date  = (info.get("liquidative_date") or "").strip()
            if not raw_value or not raw_date:
                continue

            # Precio: "176,540000 €" → 176.540000
            price_str = re.sub(r"[^\d,]", "", raw_value).replace(",", ".")
            try:
                price = float(price_str)
            except ValueError:
                continue

            # Fecha: "30-4-2026" → 2026-04-30
            try:
                dt = datetime.strptime(raw_date, "%d-%m-%Y")
            except ValueError:
                # intentamos con partes por si hay días sin cero
                parts = raw_date.split("-")
                if len(parts) != 3:
                    continue
                try:
                    day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                    dt = datetime(year, month, day)
                except Exception:
                    continue

            return (dt.strftime("%Y-%m-%d"), price)

        return None
    except Exception as e:
        log.error("Cobas: error extrayendo precio actual: %s", e, exc_info=True)
        return None


# ---------------------------------------------------------------------------
def scrape_cobas_prices(
    session,
    cobas_url: str,
) -> List[Tuple[str, float]]:
    """
    Obtiene el último valor liquidativo desde la ficha de producto de Cobas AM.

    La web de Cobas requiere cookies de consentimiento para mostrar el HTML
    completo. Este scraper envía la cookie CookieConsent con todas las
    categorías aceptadas para obtener la página real.

    Dado que la API pública (api.cobasam.com) no es accesible desde fuera
    y solo devuelve el NAV más reciente, este scraper se limita al último
    precio disponible. Para histórico completo deben usarse otras fuentes
    (FT, Yahoo Finance, etc.).

    Args:
        session: requests.Session con reintentos.
        cobas_url: URL de la ficha del producto (ej. .../lux_international_eur/).

    Returns:
        Lista de tuplas (fecha ISO "YYYY-MM-DD", precio). Vacía si falla.
    """
    if not cobas_url or not cobas_url.startswith("http"):
        return []

    product_id = _extract_product_id_from_url(cobas_url)
    if not product_id:
        log.warning("Cobas: no se pudo extraer product_id de %s", cobas_url)
        return []

    # ── Obtener HTML completo enviando la cookie de consentimiento ────────
    try:
        resp = session.get(
            cobas_url,
            headers={
                "Cookie": COOKIEBOT_CONSENT_COOKIE,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            },
            timeout=25,
        )
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        log.error("Cobas: error HTTP %s: %s", cobas_url, e)
        return []

    current = _current_price_from_html(html, product_id)
    if current:
        log.info("Cobas: NAV obtenido de %s → %s = %s", cobas_url, current[0], current[1])
        return [current]

    log.warning("Cobas: no se pudo extraer el precio de %s", cobas_url)
    return []
