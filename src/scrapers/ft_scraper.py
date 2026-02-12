from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from ..utils import parse_float, parse_ft_date

log = logging.getLogger("scrapers.ft_scraper")


def _symbol_variants(ft_symbol: str) -> List[str]:
    """
    FT a veces se representa como:
    - "LU056...:EUR" (con ':')
    - "LU056...EUR" (sin ':') [file:42]
    Probamos ambos para robustez.
    """
    sym = (ft_symbol or "").strip()
    if not sym:
        return []

    variants = [sym]

    if ":" in sym:
        variants.append(sym.replace(":", ""))
    else:
        # Si termina en 3 letras (divisa), intenta insertar ':'
        m = re.match(r"^(.+?)([A-Z]{3})$", sym)
        if m:
            variants.append(f"{m.group(1)}:{m.group(2)}")

    # Dedup conservando orden
    out = []
    seen = set()
    for v in variants:
        if v not in seen:
            out.append(v)
            seen.add(v)
    return out


def _find_close_col_index(table) -> Optional[int]:
    headers = [th.get_text(" ", strip=True).lower() for th in table.select("thead th")]
    for i, h in enumerate(headers):
        if h == "close":
            return i
    return None


def _extract_date_text_from_td(date_td) -> str:
    """
    En FT la celda Date trae 2 spans: uno largo y otro corto [file:42].
    Preferimos el largo; si no, devolvemos el texto con separador para evitar “pegado”.
    """
    long_span = date_td.select_one("span.mod-ui-hide-small-below")
    if long_span:
        return long_span.get_text(" ", strip=True)

    short_span = date_td.select_one("span.mod-ui-hide-medium-above")
    if short_span:
        return short_span.get_text(" ", strip=True)

    # Fallback: con separador ' ' (no ''), para no concatenar sin espacios
    return date_td.get_text(" ", strip=True)


def scrape_ft_prices_and_metadata(session, ft_symbol: str) -> Tuple[List[Tuple[str, float]], Dict]:
    """
    Devuelve (prices, metadata).
    prices: lista [(YYYY-MM-DD, close)]
    """
    meta: Dict = {"ft_symbol_requested": ft_symbol}
    symbols_to_try = _symbol_variants(ft_symbol)

    for sym in symbols_to_try:
        url = f"https://markets.ft.com/data/funds/tearsheet/historical?s={sym}"
        meta["url"] = url
        meta["ft_symbol_used"] = sym

        try:
            r = session.get(url, timeout=25)
            meta["status_code"] = r.status_code
            meta["final_url"] = str(getattr(r, "url", url))

            log.debug("FT: GET %s status=%s final_url=%s", url, r.status_code, meta["final_url"])

            if r.status_code != 200:
                continue

            html = r.text or ""
            soup = BeautifulSoup(html, "lxml")

            # Nombre (si está)
            h1 = soup.select_one("h1.mod-tearsheet-overview__header__name, h1.mod-tearsheet-overview__header__name--large")
            if h1:
                meta["name"] = h1.get_text(" ", strip=True)

            # Divisa (si se puede deducir del símbolo)
            m = re.search(r":([A-Z]{3})$", sym)
            if m:
                meta["currency"] = m.group(1)
            else:
                m = re.search(r"([A-Z]{3})$", sym)
                if m:
                    meta["currency"] = m.group(1)

            table = soup.select_one("table.mod-tearsheet-historical-pricesresults")
            if not table:
                # Debug extra: corta HTML para entender si hay bloqueo o cambió el DOM
                sample = re.sub(r"\s+", " ", html[:600])
                log.debug("FT: NO table.mod-tearsheet-historical-pricesresults. HTML sample: %r", sample)
                continue

            close_idx = _find_close_col_index(table)
            if close_idx is None:
                close_idx = 4  # fallback típico (Date, Open, High, Low, Close, Volume) [file:42]

            out: List[Tuple[str, float]] = []
            for row_i, tr in enumerate(table.select("tbody tr"), start=1):
                tds = tr.find_all("td")
                if len(tds) <= close_idx:
                    log.debug("FT: Fila %s ignorada por columnas insuficientes (%s)", row_i, len(tds))
                    continue

                date_td = tds[0]
                date_raw = _extract_date_text_from_td(date_td)
                close_raw = tds[close_idx].get_text(" ", strip=True)

                log.debug("FT: Fila %s - Fecha raw: %r, Close raw: %r", row_i, date_raw, close_raw)

                try:
                    d = parse_ft_date(date_raw)
                    close = parse_float(close_raw)
                    out.append((d, close))
                except Exception as e:
                    log.debug("FT: No se pudo parsear fila %s (%s)", row_i, e)

            if out:
                log.debug("FT: Parsed %s filas OK para %s", len(out), sym)
                return out, meta

            # Si hay tabla pero no parseamos nada, seguimos probando variante de símbolo
            log.debug("FT: Tabla encontrada pero 0 filas parseadas para %s", sym)

        except Exception as e:
            log.error("FT error symbol=%s: %s", sym, e, exc_info=True)

    return [], meta
