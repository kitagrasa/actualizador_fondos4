from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict

from .utils import json_dumps_canonical

log = logging.getLogger("portfolio")

def readpricesjson(path: Path) -> Dict[str, float]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        out = {}
        for row in data:
            d = row.get("date")
            c = row.get("close")
            if isinstance(d, str):
                if isinstance(c, (int, float)):
                    out[d] = float(c)
                elif isinstance(c, str):
                    # Si el precio viene de un JSON guardado como string con coma, se pasa a punto
                    out[d] = float(c.replace(',', '.'))
        return out
    except Exception as e:
        log.error("No se pudo leer %s: %s", path, e)
        return {}

def writepricesjsonifchanged(path: Path, pricesbydate: Dict[str, float]) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    
    for d in sorted(pricesbydate.keys(), reverse=True):
        price = pricesbydate[d]
        
        # 1. Conservamos precisión máxima (6 decimales) y evitamos notación científica
        # 2. Eliminamos ceros sobrantes a la derecha (e.g. 15.400000 -> 15.4)
        # 3. Reemplazamos el punto decimal de Python por una coma
        price_str = f"{price:.6f}".rstrip("0").rstrip(".")
        if "." in price_str:
            price_str = price_str.replace(".", ",")
            
        rows.append({"date": d, "close": price_str})

    newtext = json_dumps_canonical(rows) + "\n"
    
    oldtext = path.read_text(encoding="utf-8") if path.exists() else None
    if oldtext == newtext:
        return False

    path.write_text(newtext, encoding="utf-8")
    return True
