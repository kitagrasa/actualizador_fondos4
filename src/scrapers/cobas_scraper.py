from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import List, Tuple, Optional

from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

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
        match = re.search(r"JSON\.parse\(`(.*?)`\)", script.string, re.DOTALL)
        if not match:
            log.warning("Cobas: no se pudo extraer el JSON del product-block")
            return None

        data = json.loads(match.group(1))
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


def _historical_from_api(session, product_id: str) -> List[Tuple[str, float]]:
    """
    Intenta obtener serie histórica desde la API pública de Cobas AM.
    Se prueban varios endpoints habituales; la respuesta debe ser JSON
    con una lista de objetos {date, value/nav/price}.
    """
    candidates = [
        f"https://api.cobasam.com/graph/{product_id}",
        f"https://api.cobasam.com/product/{product_id}/prices",
        f"https://api.cobasam.com/{product_id}/historical",
        f"https://api.cobasam.com/data/{product_id}",
    ]

    for url in candidates:
        try:
            resp = session.get(url, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            prices = _parse_historical_json(data)
            if prices:
                log.info("Cobas: histórico obtenido de %s (%d puntos)", url, len(prices))
                return prices
        except Exception:
            continue
    return []


def _parse_historical_json(payload) -> List[Tuple[str, float]]:
    """Convierte la respuesta JSON de la API a [(fecha, precio), ...]."""
    points = payload.get("data", payload) if isinstance(payload, dict) else payload
    if not isinstance(points, list):
        return []

    out = []
    for item in points:
        if not isinstance(item, dict):
            continue
        # Campos posibles de fecha/nav
        date_str = item.get("date") or item.get("fecha") or item.get("x")
        value = item.get("value") or item.get("nav") or item.get("price") or item.get("y")
        if not date_str or value is None:
            continue
        try:
            price = float(str(value).replace(",", "."))
        except ValueError:
            continue
        # Intentar parsear fecha (varios formatos)
        dt = None
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y%m%d"):
            try:
                dt = datetime.strptime(str(date_str)[:10], fmt)
                break
            except ValueError:
                continue
        if dt is None:
            # parsear como timestamp Unix (segundos o milisegundos)
            try:
                ts = float(date_str)
                if ts > 1e12:   # milisegundos
                    ts /= 1000
                dt = datetime.utcfromtimestamp(ts)
            except Exception:
                continue
        out.append((dt.strftime("%Y-%m-%d"), price))

    return sorted(out, key=lambda x: x[0])


# ---------------------------------------------------------------------------
def scrape_cobas_prices(
    session,
    cobas_url: str,
) -> List[Tuple[str, float]]:
    """
    Obtiene precios desde la web / API de Cobas Asset Management.

    - Primero intenta descargar histórico a través de la API pública.
    - Si no existe o falla, extrae el último NAV desde el HTML de la página.

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

    # 1) Intentar histórico vía API
    hist = _historical_from_api(session, product_id)
    if hist:
        return hist

    # 2) Fallback: obtener el último precio desde el HTML
    try:
        resp = session.get(cobas_url, timeout=15)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        log.error("Cobas: error HTTP %s: %s", cobas_url, e)
        return []

    current = _current_price_from_html(html, product_id)
    if current:
        log.info("Cobas: obtenido solo precio más reciente de %s", cobas_url)
        return [current]

    log.warning("Cobas: no se pudo extraer ningún precio de %s", cobas_url)
    return []
