"""
Persistencia de históricos como tabla HTML para Portfolio Performance.

Cada fondo se guarda en `data/prices/{ISIN}` (sin extensión) con una única
tabla de dos columnas, `Date` y `Close`, que es el formato que espera el
importador «Table on website» de Portfolio Performance:

- cabeceras `<th>Date</th>` y `<th>Close</th>`;
- fechas ISO `YYYY-MM-DD`;
- precios con punto decimal y hasta 6 decimales.

El archivo se sirve desde raw.githubusercontent.com y se importa con la URL
`.../data/prices/{ISIN}` sin extensión.
"""
from __future__ import annotations

import logging
import re
from html import escape
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List

log = logging.getLogger("portfolio")

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

HTML_HEAD = (
    "<!DOCTYPE html>\n"
    '<html lang="en">\n'
    "<head>\n"
    '<meta charset="utf-8">\n'
    "<title>{title}</title>\n"
    "</head>\n"
    "<body>\n"
    "<table>\n"
    "<thead>\n"
    "<tr><th>Date</th><th>Close</th></tr>\n"
    "</thead>\n"
    "<tbody>\n"
)
HTML_TAIL = "</tbody>\n</table>\n</body>\n</html>\n"


def format_price(value: float) -> str:
    """Formatea el precio con punto decimal y sin ceros sobrantes (máx. 6 decimales)."""
    text = f"{round(float(value), 6):.6f}".rstrip("0").rstrip(".")
    return text or "0"


def render_prices_html(prices_by_date: Dict[str, float], title: str = "Prices") -> str:
    """Genera la página HTML con la tabla de precios, fechas descendentes."""
    rows = []
    for d in sorted(prices_by_date.keys(), reverse=True):
        if not DATE_RE.match(d):
            log.warning("Fecha con formato inesperado, se omite: %r", d)
            continue
        rows.append(f"<tr><td>{d}</td><td>{format_price(prices_by_date[d])}</td></tr>\n")
    return HTML_HEAD.format(title=escape(title)) + "".join(rows) + HTML_TAIL


class _PricesTableParser(HTMLParser):
    """Extrae las celdas `<td>` de las filas del `<tbody>` de la tabla."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: List[List[str]] = []
        self._in_tbody = False
        self._in_cell = False
        self._cell: List[str] = []
        self._row: List[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag == "tbody":
            self._in_tbody = True
        elif tag == "tr" and self._in_tbody:
            self._row = []
        elif tag == "td" and self._in_tbody:
            self._in_cell = True
            self._cell = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "td" and self._in_cell:
            self._row.append("".join(self._cell).strip())
            self._in_cell = False
        elif tag == "tr" and self._in_tbody:
            if self._row:
                self.rows.append(self._row)
            self._row = []
        elif tag == "tbody":
            self._in_tbody = False

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell.append(data)


def read_prices(path: Path) -> Dict[str, float]:
    """Lee la tabla HTML de un fondo. Devuelve {} si no existe o no se puede parsear."""
    if not path.exists():
        return {}
    try:
        parser = _PricesTableParser()
        parser.feed(path.read_text(encoding="utf-8"))
        out: Dict[str, float] = {}
        for cells in parser.rows:
            if len(cells) != 2:
                continue
            date_str, price_text = cells[0], cells[1].replace("\xa0", "")
            if not DATE_RE.match(date_str):
                continue
            try:
                out[date_str] = float(price_text)
            except ValueError:
                log.warning("Precio ilegible en %s (%s): %r", path, date_str, price_text)
        return out
    except Exception as e:
        log.error("No se pudo leer %s: %s", path, e)
        return {}


def write_prices_if_changed(path: Path, prices_by_date: Dict[str, float]) -> bool:
    """Escribe la tabla HTML solo si el texto generado cambia."""
    path.parent.mkdir(parents=True, exist_ok=True)
    new_text = render_prices_html(prices_by_date, title=path.name)
    old_text = path.read_text(encoding="utf-8") if path.exists() else None
    if old_text == new_text:
        return False
    path.write_text(new_text, encoding="utf-8")
    return True
