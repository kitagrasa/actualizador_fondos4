from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any

# Expresión regular simplificada y robusta para la fecha de Financial Times
RE_FT_LONG_DATE = re.compile(r"([a-zA-Z]+,\s+[a-zA-Z]+\s+\d{1,2},\s+\d{4})")

def setup_logging() -> None:
    level = os.getenv("LOGLEVEL", "INFO").upper().strip()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def madridnow_str() -> str:
    return datetime.now(ZoneInfo("Europe/Madrid")).strftime("%Y-%m-%d %H:%M:%S %Z")

def parse_float(text: str) -> float:
    s = str(text).strip().replace("\xa0", "").replace(" ", "")
    # Quitamos letras de monedas o cualquier caracter extraño, dejamos solo dígitos, coma, punto y negativo
    s = re.sub(r"[^\d\.\,\-]", "", s)
    if not s:
        raise ValueError(f"No se pudo parsear número de {text!r}")
        
    # Detectar el formato exacto y limpiarlo a formato interno estándar (float)
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            # Formato europeo: 35.450,82 -> 35450.82
            s = s.replace(".", "").replace(",", ".")
        else:
            # Formato inglés FT: 35,450.82 -> 35450.82
            s = s.replace(",", "")
    elif "," in s:
        # Formato europeo simple: 35450,82 -> 35450.82
        s = s.replace(",", ".")
        
    return float(s)

def parse_date(date_str: str) -> str:
    """
    Parsea una fecha en formato DD/MM/YYYY, DD-MM-YYYY, DD.MM.YYYY, o DDMMYYYY.
    Devuelve YYYY-MM-DD.
    """
    # Eliminar separadores no numéricos
    cleaned = re.sub(r"[^\d]", "", date_str)
    if len(cleaned) == 8:
        # Formato DDMMYYYY
        day = cleaned[0:2]
        month = cleaned[2:4]
        year = cleaned[4:8]
        return f"{year}-{month}-{day}"
    else:
        # Intentar con separadores
        separators = re.findall(r"(\D)", date_str)
        if separators:
            sep = separators[0]
            parts = date_str.split(sep)
            if len(parts) == 3:
                day, month, year = parts
                if len(year) == 2:
                    year = f"20{year}"
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        raise ValueError(f"Formato de fecha no reconocido: {date_str}")

# Esta función se mantiene por compatibilidad con otros módulos
def parse_fundsquare_date_ddmmyyyy(text: str) -> str:
    """Wrapper de parse_date para mantener compatibilidad con otros módulos."""
    return parse_date(text)

def parse_ft_date(datecell_text: str) -> str:
    m = RE_FT_LONG_DATE.search(datecell_text)
    if not m:
        raise ValueError(f"No se pudo extraer fecha larga de {datecell_text!r}")
    dt = datetime.strptime(m.group(1), "%A, %B %d, %Y").date()
    return dt.isoformat()

def json_dumps_canonical(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)
