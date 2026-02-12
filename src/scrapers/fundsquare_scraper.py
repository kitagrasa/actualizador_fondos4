from __future__ import annotations

import logging
from typing import List, Tuple

from bs4 import BeautifulSoup

from ..utils import parse_float, parse_fundsquare_date_ddmmyyyy

log = logging.getLogger("scrapers.fundsquare")


def scrape_fundsquare_prices(session, id_instr: str) -> List[Tuple[str, float]]:
    """
    Devuelve lista [(YYYY-MM-DD, nav)].
    """
    url = f"https://www.fundsquare.net/security/histo-prices?idInstr={id_instr}"
    try:
        r = session.get(url, timeout=25)
        if r.status_code != 200:
            log.warning("Fundsquare %s status=%s", id_instr, r.status_code)
            return []
        soup = BeautifulSoup(r.text, "lxml")

        table = soup.select_one("table.tabHorizontal")
        if not table:
            log.warning("Fundsquare: no se encontró table.tabHorizontal en %s", url)
            return []

        # Intentar mapear columnas por cabecera
        headers = [th.get_text(" ", strip=True).lower() for th in table.select("tr th")]
        # En el HTML que adjuntaste aparece "NAV date" y "NAV" en esa tabla [file:1]
        date_idx = None
        nav_idx = None
        for i, h in enumerate(headers):
            h_norm = h.replace(" ", "")
            if "navdate" in h_norm:
                date_idx = i
            if h_norm == "nav":
                nav_idx = i

        out: List[Tuple[str, float]] = []
        for tr in table.select("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue

            if date_idx is not None and nav_idx is not None and len(tds) > max(date_idx, nav_idx):
                date_raw = tds[date_idx].get_text(" ", strip=True)
                nav_raw = tds[nav_idx].get_text(" ", strip=True)
            else:
                # Fallback: primeras 2 columnas
                if len(tds) < 2:
                    continue
                date_raw = tds[0].get_text(" ", strip=True)
                nav_raw = tds[1].get_text(" ", strip=True)

            try:
                d = parse_fundsquare_date_ddmmyyyy(date_raw)
                nav = parse_float(nav_raw)
                out.append((d, nav))
            except Exception:
                continue

        return out

    except Exception as e:
        log.error("Fundsquare error idInstr=%s: %s", id_instr, e, exc_info=True)
        return []
