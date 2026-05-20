"""
Scraper para Fundsquare.
Extrae precios históricos de la tabla .tabHorizontal.
"""
from __future__ import annotations

import logging
import re
from typing import List, Tuple

from bs4 import BeautifulSoup

from ..utils import parse_float, parse_date

log = logging.getLogger("scrapers.fundsquare")


def scrape_fundsquare_prices(session, fundsquare_url: str) -> List[Tuple[str, float]]:
    """
    Acepta la URL completa de Fundsquare (por ejemplo, /security/price?idInstr=XXXXX).
    Devuelve [(YYYY-MM-DD, NAV)].
    """
    if not fundsquare_url:
        log.debug("Fundsquare: URL vacía, se omite.")
        return []

    try:
        r = session.get(fundsquare_url, timeout=25)
        if r.status_code != 200:
            log.warning("Fundsquare: status=%s url=%s", r.status_code, fundsquare_url)
            return []

        soup = BeautifulSoup(r.text, "lxml")

        # 1️⃣ Buscar la tabla principal de precios (tabla .tabHorizontal)
        table = soup.select_one("table.tabHorizontal")
        if table:
            rows = table.find_all("tr")
            if len(rows) > 1:
                # Asumir que la primera fila son encabezados
                header_row = rows[0]
                headers = [th.get_text(strip=True).lower() for th in header_row.find_all("th")]
                
                # Detectar índices de fecha y NAV
                date_idx = None
                nav_idx = None
                for i, h in enumerate(headers):
                    if "date" in h:
                        date_idx = i
                    if "nav" in h:
                        nav_idx = i
                
                # Fallback si no se encuentran: primera columna = fecha, cuarta = NAV
                if date_idx is None:
                    date_idx = 0
                if nav_idx is None:
                    nav_idx = 3  # NAV suele ser la cuarta columna

                out = []
                for row in rows[1:]:
                    cells = row.find_all("td")
                    if len(cells) > max(date_idx, nav_idx):
                        date_raw = cells[date_idx].get_text(strip=True)
                        nav_raw = cells[nav_idx].get_text(strip=True)
                        try:
                            date_str = parse_date(date_raw)
                            nav = parse_float(nav_raw)
                            out.append((date_str, nav))
                        except Exception as e:
                            log.debug("Error procesando fila: %s", e)
                            continue
                if out:
                    return out

        # 2️⃣ Si no se encontró la tabla, buscar en la sección "Latest Price"
        latest_price_div = soup.find("div", class_="bloctitle", string=re.compile(r"Latest Price"))
        if latest_price_div:
            price_table = latest_price_div.find_next("table", class_="tabHorizontal")
            if price_table:
                rows = price_table.find_all("tr")
                if len(rows) > 1:
                    data_row = rows[1]  # segunda fila contiene los datos
                    cells = data_row.find_all("td")
                    if len(cells) >= 4:
                        date_raw = cells[0].get_text(strip=True)
                        nav_raw = cells[3].get_text(strip=True)  # NAV en la cuarta columna
                        try:
                            date_str = parse_date(date_raw)
                            nav = parse_float(nav_raw)
                            return [(date_str, nav)]
                        except Exception as e:
                            log.debug("Error parseando último precio: %s", e)

        log.warning("Fundsquare: no se encontraron datos de precios en %s", fundsquare_url)
        return []

    except Exception as e:
        log.error("Error en Fundsquare url=%s: %s", fundsquare_url, e, exc_info=True)
        return []
