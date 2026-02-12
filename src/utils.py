from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime

WEEKDAYS_LONG = r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)"
WEEKDAYS_SHORT = r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)"

RE_FT_LONG_DATE = re.compile(rf"({WEEKDAYS_LONG},\s+[A-Za-z]+\s+\d{{1,2}},\s+\d{{4}})")
RE_FT_SHORT_DATE = re.compile(rf"({WEEKDAYS_SHORT},\s+[A-Za-z]{{3}}\s+\d{{1,2}},\s+\d{{4}})")

RE_NUMBER = re.compile(r"[-+]?\d+(?:[.,]\d+)?")

log = logging.getLogger("utils")


def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper().strip()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
    # Evita ruido excesivo de requests/urllib3 cuando estás en DEBUG
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def parse_float(text: str) -> float:
    """
    Maneja: "32.763 EUR", "32,763", " 32.76 ", NBSP, etc.
    """
    cleaned = (text or "").replace("\xa0", " ").strip()
    m = RE_NUMBER.search(cleaned.replace(" ", ""))
    if not m:
        raise ValueError(f"No se pudo parsear número de: {text!r}")
    val = m.group(0).replace(",", ".")
    return float(val)


def parse_fundsquare_date_ddmmyyyy(text: str) -> str:
    digits = re.sub(r"\D", "", text or "")
    if len(digits) != 8:
        raise ValueError(f"Fecha Fundsquare inválida: {text!r}")
    dt = datetime.strptime(digits, "%d%m%Y").date()
    return dt.isoformat()


def parse_ft_date(date_cell_text: str) -> str:
    """
    FT puede venir:
    - "Wednesday, February 11, 2026"
    - "Wed, Feb 11, 2026"
    - o concatenado: "Wednesday, February 11, 2026Wed, Feb 11, 2026" [file:42]
    En vez de replace(), extraemos por regex (equivalente a split robusto).
    """
    s = (date_cell_text or "").replace("\xa0", " ").strip()

    m = RE_FT_LONG_DATE.search(s)
    if m:
        dt = datetime.strptime(m.group(1), "%A, %B %d, %Y").date()
        return dt.isoformat()

    m = RE_FT_SHORT_DATE.search(s)
    if m:
        dt = datetime.strptime(m.group(1), "%a, %b %d, %Y").date()
        return dt.isoformat()

    raise ValueError(f"No se pudo parsear fecha FT: {date_cell_text!r}")


def json_dumps_canonical(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
