from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import List, Tuple, Optional

from bs4 import BeautifulSoup
from requests import Session

log = logging.getLogger(__name__)


def scrape_investing_prices(
    session: Session,
    url: str,
    cached_pair_id: Optional[str] = None,
    startdate: Optional[date] = None,
    enddate: Optional[date] = None,
    fullrefresh: bool = False
) -> Tuple[List[Tuple[str, float]], Optional[str]]:
    """
    Retorna la lista de tuplas (fecha, precio) y el pair_id.
    """
    if not url or not isinstance(url, str) or not url.startswith("http"):
        return [], cached_pair_id

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }

    try:
        resp = session.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        log.error("Investing: Error HTTP al cargar %s: %s", url, e)
        return [], cached_pair_id

    # 1. Extraer pair_id (Arreglado el SyntaxError cambiando las comillas exteriores a simples)
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

    # 2. Intento de llamada AJAX para obtener el histórico profundo
    if pair_id:
        try:
            end_dt = enddate or date.today()
            start_dt = startdate or (end_dt - timedelta(days=730))  # 2 años por defecto
            if fullrefresh:
                start_dt = date(2000, 1, 1)

            ajax_url = "https://www.investing.com/instruments/HistoricalDataAjax"
            ajax_headers = headers.copy()
            ajax_headers.update({
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": url,
            })
            
            payload = {
                "curr_id": pair_id,
                "st_date": start_dt.strftime("%m/%d/%Y"),
                "end_date": end_dt.strftime("%m/%d/%Y"),
                "interval_sec": "Daily",
                "sort_col": "date",
                "sort_ord": "DESC",
                "action": "historical_data"
            }

            ajax_resp = session.post(ajax_url, data=payload, headers=ajax_headers, timeout=15)
            if ajax_resp.ok and ajax_resp.text.strip():
                soup_ajax = BeautifulSoup(ajax_resp.text, "html.parser")
                table_ajax = soup_ajax.find("table", id="curr_table")
                if table_ajax:
                    historical_prices = _parse_investing_table(table_ajax)
                    if historical_prices:
                        log.info("Investing: Obtenidos %d precios vía AJAX", len(historical_prices))
                        return historical_prices, pair_id
        except Exception as e:
            log.warning("Investing: Fallo en llamada AJAX, probando tabla estática... (%s)", e)

    # 3. Fallback: Parsear la tabla HTML nativa visible si el AJAX falla
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"data-test": "historical-data-table"})
    if not table:
        table = soup.find("table", id="curr_table")

    if table:
        historical_prices = _parse_investing_table(table)
        log.info("Investing: Obtenidos %d precios de tabla HTML estática", len(historical_prices))
    else:
        log.warning("Investing: No se encontró tabla histórica en %s", url)

    return historical_prices, pair_id


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
        # Parseo robusto para múltiples formatos de fecha
        try:
            if "-" in raw_date:
                parsed_date = datetime.strptime(raw_date, "%Y-%m-%d").strftime("%Y-%m-%d")
            elif "." in raw_date:
                parsed_date = datetime.strptime(raw_date, "%d.%m.%Y").strftime("%Y-%m-%d")
            elif "/" in raw_date:
                parsed_date = datetime.strptime(raw_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            else:
                # Caso: Feb 20 2026
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

        # Parseo robusto de divisas y decimales
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
