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
            if isinstance(d, str):
                if isinstance(c, (int, float)):
                    out[d] = float(c)
                elif isinstance(c, str):
                    # Si el precio viene de un JSON guardado como string con coma, se pasa a punto internamente
                    out[d] = float(c.replace(',', '.'))
        return out
    except Exception as e:
        log.error("No se pudo leer %s: %s", path, e)
        return {}

def write_prices_json_if_changed(path: Path, prices_by_date: Dict[str, float]) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    
    for d in sorted(prices_by_date.keys(), reverse=True):
        price = prices_by_date[d]
        
        # 1. Conservamos precisión máxima y evitamos notación científica
        price_str = f"{price:.6f}"
        
        # 2. Eliminamos ceros sobrantes a la derecha y reemplazamos punto por coma
        if "." in price_str:
            price_str = price_str.rstrip("0").rstrip(".")
            price_str = price_str.replace(".", ",")
            
        rows.append({"date": d, "close": price_str})

    new_text = json_dumps_canonical(rows) + "\n"
    
    old_text = path.read_text(encoding="utf-8") if path.exists() else None
    if old_text == new_text:
        return False

    path.write_text(new_text, encoding="utf-8")
    return True
