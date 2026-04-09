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
        raise ValueError(f"No se pudo parsear texto a número: {text!r}")
        
    # Detectar el formato exacto y limpiarlo a formato interno estándar (float)
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            # Formato europeo: 1.234,56 -> 1234.56
            s = s.replace(".", "").replace(",", ".")
        else:
            # Formato inglés FT: 1,234.56 -> 1234.56
            s = s.replace(",", "")
    elif "," in s:
        # Formato europeo simple: 1234,56 -> 1234.56
        s = s.replace(",", ".")
        
    return float(s)

def parse_fundsquare_date_ddmmyyyy(text: str) -> str:
    digits = re.sub(r"\D", "", text)
    if len(digits) != 8:
        raise ValueError(f"Fecha Fundsquare inválida: {text!r}")
    dt = datetime.strptime(digits, "%d%m%Y").date()
    return dt.isoformat()

def parse_ft_date(datecell_text: str) -> str:
    m = RE_FT_LONG_DATE.search(datecell_text)
    if not m:
        raise ValueError(f"No se pudo extraer fecha larga de {datecell_text!r}")
    dt = datetime.strptime(m.group(1), "%A, %B %d, %Y").date()
    return dt.isoformat()

def json_dumps_canonical(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)
