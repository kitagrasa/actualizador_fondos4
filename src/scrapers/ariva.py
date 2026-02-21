"""
scrapers/ariva.py

Scraper para páginas 'historische Kurse' de ariva.de
Columna en funds: URL completa tipo:
  https://www.ariva.de/fonds/{slug}/kurse/historische-kurse
"""

import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ─── Constantes ───────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.7",
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
}

# ─── Helpers internos ─────────────────────────────────────────────────────────
def _parse_ariva_date(raw: str) -> datetime | None:
    """Convierte 'DD.MM.YY' → datetime. Ariva usa año de 2 dígitos."""
    try:
        return datetime.strptime(raw.strip(), "%d.%m.%y")
    except ValueError:
        return None


def _parse_ariva_price(raw: str) -> float | None:
    """Convierte '87,06 €' → 87.06"""
    cleaned = re.sub(r"[^\d,]", "", raw).replace(",", ".")
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


# ─── Función principal ────────────────────────────────────────────────────────
def scrape_ariva(url: str, timeout: int = 15) -> dict | None:
    """
    Obtiene el precio de cierre y fecha más reciente de una página
    kurse/historische-kurse de ariva.de.

    Args:
        url:     URL completa de la página de precios históricos.
                 Ejemplo: https://www.ariva.de/fonds/blackrock-global-funds-
                          world-gold-fund-e2-eur/kurse/historische-kurse
        timeout: Timeout HTTP en segundos.

    Returns:
        {'date': datetime, 'price': float} o None si falla.
    """
    # 1. Descarga la página
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"[ariva] ❌ Error HTTP en {url}: {exc}")
        return None

    # 2. Parse HTML
    soup = BeautifulSoup(resp.text, "html.parser")

    # 3. Busca filas de datos (clase 'arrow0')
    rows = soup.find_all("tr", class_="arrow0")
    if not rows:
        print(f"[ariva] ❌ Sin filas de datos en {url}")
        return None

    # 4. Primera fila = precio más reciente
    cells = rows[0].find_all("td")
    if len(cells) < 4:
        print(f"[ariva] ❌ Estructura inesperada ({len(cells)} celdas) en {url}")
        return None

    # 5. Extrae fecha (celda 0) y precio cierre (celda 3)
    date = _parse_ariva_date(cells[0].get_text(strip=True))
    price = _parse_ariva_price(cells[3].get_text(strip=True))

    if date is None or price is None:
        print(
            f"[ariva] ❌ No se pudo parsear "
            f"fecha='{cells[0].get_text(strip=True)}' "
            f"precio='{cells[3].get_text(strip=True)}' en {url}"
        )
        return None

    return {"date": date, "price": price}
