from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict

from .utils import json_dumps_canonical

log = logging.getLogger("portfolio")

def read_prices_json(path: Path) -> Dict[str, float]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        out = {}
        for row in data:
            d = row.get("date")
            c = row.get("close")
            if isinstance(d, str) and c is not None:
                # Soportar lectura tanto si estaba guardado de forma estándar o si era el string que guardamos antes
                if isinstance(c, (int, float)):
                    out[d] = float(c)
                elif isinstance(c, str):
                    # Transición: convierte los textos "37.501,04" otra vez a número matemático "37501.04"
                    c_clean = c.replace(".", "").replace(",", ".")
                    out[d] = float(c_clean)
        return out
    except Exception as e:
        log.error("No se pudo leer %s: %s", path, e)
        return {}

def write_prices_json_if_changed(path: Path, prices_by_date: Dict[str, float]) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    
    for d in sorted(prices_by_date.keys(), reverse=True):
        # Redondeamos a un máximo de 6 decimales para evitar problemas de coma flotante infinita.
        # Se guarda como tipo numérico (sin comillas) con punto decimal como exige el formato JSON.
        price = round(prices_by_date[d], 6)
        rows.append({"date": d, "close": price})

    # Convertimos a JSON estándar
    new_text = json_dumps_canonical(rows) + "\n"
    
    old_text = path.read_text(encoding="utf-8") if path.exists() else None
    if old_text == new_text:
        return False

    path.write_text(new_text, encoding="utf-8")
    return True
