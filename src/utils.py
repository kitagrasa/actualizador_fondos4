from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


WEEKDAYS = r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)"
RE_FT_LONG_DATE = re.compile(rf"({WEEKDAYS},\s+[A-Za-z]+\s+\d{{1,2}},\s+\d{{4}})")
RE_NUMBER = re.compile(r"[-+]?\d+(?:[.,]\d+)?")


def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper().strip()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def madrid_now_str() -> str:
    return datetime.now(ZoneInfo("Europe/Madrid")).strftime("%Y-%m-%d %H:%M:%S %Z")


def parse_float(text: str) -> float:
    # Maneja "32.763 EUR", "32,763", etc.
    m = RE_NUMBER.search(text.replace(" ", ""))
    if not m:
        raise ValueError(f"No se pudo parsear número de: {text!r}")
    val = m.group(0).replace(",", ".")
    return float(val)


def parse_fundsquare_date_ddmmyyyy(text: str) -> str:
    digits = re.sub(r"\D", "", text)
    if len(digits) != 8:
        raise ValueError(f"Fecha Fundsquare inválida: {text!r}")
    dt = datetime.strptime(digits, "%d%m%Y").date()
    return dt.isoformat()


def parse_ft_date(date_cell_text: str) -> str:
    """
    FT a veces concatena el formato largo y el corto en la misma celda.
    En vez de replace(), extraemos la primera fecha larga por regex (equivalente a split semántico).
    """
    m = RE_FT_LONG_DATE.search(date_cell_text)
    if not m:
        raise ValueError(f"No se pudo extraer fecha larga de: {date_cell_text!r}")
    dt = datetime.strptime(m.group(1), "%A, %B %d, %Y").date()
    return dt.isoformat()


def json_dumps_canonical(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
