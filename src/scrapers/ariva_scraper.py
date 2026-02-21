from __future__ import annotations

import logging
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

def scrape_ariva_prices(url: str, **kwargs) -> tuple[list[dict], dict | None]:
    """
    Obtiene el historial completo de precios desde una página de Ariva.
    Retorna: (lista_precios, metadatos)
    """
    if not url or not isinstance(url, str) or not url.startswith("http"):
        return [], None

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "de-DE,de;q=0.9,es;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    try:
        # Timeout de 15s, óptimo y robusto para evitar bloqueos en GitHub Actions
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        log.error("Ariva: Error HTTP al acceder a %s: %s", url, e)
        return [], None

    # Una sola pasada de BeautifulSoup para optimizar memoria
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Ariva usa las clases 'arrow0' y 'arrow1' para las filas de datos históricos
    rows = soup.find_all("tr", class_=re.compile(r"^arrow"))
    
    if not rows:
        log.warning("Ariva: No se encontraron filas de precios (clase 'arrow') en %s", url)
        return [], None

    historical_prices = []

    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        raw_date = cells[0].get_text(strip=True)
        
        # En Ariva la estructura es: Datum | Eröffnung | Hoch | Tief | Schluss | Volumen
        # Índice 4 es el cierre (Schluss). Si falla o la fila es atípica, tomamos la apertura (Índice 1)
        if len(cells) >= 5:
            raw_price = cells[4].get_text(strip=True)
            if not raw_price:
                raw_price = cells[1].get_text(strip=True)
        else:
            raw_price = cells[1].get_text(strip=True)

        # 1. Parseo estricto de fecha: formato DD.MM.YY (ej: 20.02.26) -> YYYY-MM-DD
        try:
            parsed_date = datetime.strptime(raw_date, "%d.%m.%y").strftime("%Y-%m-%d")
        except ValueError:
            continue

        # 2. Limpieza robusta del precio: elimina divisas, espacios, y pasa coma a punto
        cleaned_price = re.sub(r"[^\d,]", "", raw_price).replace(",", ".")
        try:
            parsed_price = float(cleaned_price)
        except ValueError:
            continue

        historical_prices.append({
            "date": parsed_date,
            "close": parsed_price
        })

    log.info("Ariva: Extraídos %d precios de %s", len(historical_prices), url)
    return historical_prices, None

