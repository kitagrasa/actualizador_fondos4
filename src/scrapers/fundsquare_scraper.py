from __future__ import annotations

import logging
from typing import List, Tuple

from bs4 import BeautifulSoup

from ..utils import parse_float, parse_fundsquare_date_ddmmyyyy

log = logging.getLogger("scrapers.fundsquare")


def scrape_fundsquare_prices(session, fundsquare_url: str) -> List[Tuple[str, float]]:
    """Acepta la URL completa de Fundsquare. Devuelve [(YYYY-MM-DD, nav)]."""
    if not fundsquare_url:
        log.debug("Fundsquare: URL vacía, se omite.")
        return []
    try:
        r = session.get(fundsquare_url, timeout=25)
        if r.status_code != 200:
            log.warning("Fundsquare: status=%s url=%s", r.status_code, fundsquare_url)
            return []

        soup = BeautifulSoup(r.text, "lxml")
        table = soup.select_one("table.tabHorizontal")
        if not table:
            log.warning("Fundsquare: no se encontró table.tabHorizontal en %s", fundsquare_url)
            return []

        headers = [th.get_text(" ", strip=True).lower() for th in table.select("tr th")]
        date_idx, nav_idx = None, None
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
                if len(tds) < 2:
                    continue
                date_raw = tds[0].get_text(" ", strip=True)
                nav_raw = tds[1].get_text(" ", strip=True)
            try:
                out.append((parse_fundsquare_date_ddmmyyyy(date_raw), parse_float(nav_raw)))
            except Exception:
                continue
        return out

    except Exception as e:
        log.error("Fundsquare error url=%s: %s", fundsquare_url, e, exc_info=True)
        return []
