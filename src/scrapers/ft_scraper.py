from __future__ import annotations

import json
import logging
import re
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from ..utils import parse_float, parse_ft_date

log = logging.getLogger("scrapers.ft")


def _find_close_col_index(table) -> Optional[int]:
    headers = [th.get_text(" ", strip=True).lower() for th in table.select("thead th")]
    for i, h in enumerate(headers):
        if h == "close":
            return i
    return None


def _extract_date_text_from_td(date_td) -> str:
    # FT suele traer 2 spans: long y short; si usamos get_text(strip=True) se concatena [file:1]
    long_span = date_td.select_one("span.mod-ui-hide-small-below")
    if long_span:
        return long_span.get_text(" ", strip=True)

    # Fallback: concat, y parse_ft_date extraerá por regex “la primera fecha larga”
    return date_td.get_text("", strip=True)


def scrape_ft_prices_and_metadata(session, ft_symbol: str) -> Tuple[List[Tuple[str, float]], Dict]:
    """
    ft_symbol típicamente "ISIN:EUR".
    Devuelve (prices, metadata).
    """
    url = f"https://markets.ft.com/data/funds/tearsheet/historical?s={ft_symbol}"
    meta: Dict = {"ft_symbol": ft_symbol, "url": url}

    try:
        r = session.get(url, timeout=25)
        meta["status_code"] = r.status_code
        if r.status_code != 200:
            log.warning("FT %s status=%s", ft_symbol, r.status_code)
            return [], meta

        soup = BeautifulSoup(r.text, "lxml")

        # Nombre (si está)
        h1 = soup.select_one("h1.mod-tearsheet-overview__header__name")
        if h1:
            meta["name"] = h1.get_text(" ", strip=True)

        # Intentar detectar divisa desde el símbolo (p. ej. ":EUR")
        m = re.search(r":([A-Z]{3})$", ft_symbol)
        if m:
            meta["currency"] = m.group(1)

        table = soup.select_one("table.mod-tearsheet-historical-pricesresults")
        if not table:
            log.warning("FT: no se encontró table.mod-tearsheet-historical-pricesresults en %s", url)
            return [], meta

        close_idx = _find_close_col_index(table)
        date_idx = 0  # normalmente es la primera columna

        out: List[Tuple[str, float]] = []
        for i, tr in enumerate(table.select("tbody tr"), start=1):
            tds = tr.find_all("td")
            if not tds:
                continue

            try:
                date_td = tds[date_idx]
                date_text = _extract_date_text_from_td(date_td)

                if close_idx is not None and close_idx < len(tds):
                    close_raw = tds[close_idx].get_text(" ", strip=True)
                else:
                    # Fallback: en FT suele ser la 5ª columna (Date, Open, High, Low, Close) [file:1]
                    close_raw = tds[4].get_text(" ", strip=True) if len(tds) > 4 else ""

                d = parse_ft_date(date_text)
                close = parse_float(close_raw)
                out.append((d, close))
            except Exception as e:
                log.debug("FT: Fila %s - No parseable (%s)", i, e)

        return out, meta

    except Exception as e:
        log.error("FT error symbol=%s: %s", ft_symbol, e, exc_info=True)
        return [], meta
