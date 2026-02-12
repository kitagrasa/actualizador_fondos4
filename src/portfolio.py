from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple

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
            if isinstance(d, str) and (isinstance(c, int) or isinstance(c, float)):
                out[d] = float(c)
        return out
    except Exception as e:
        log.error("No se pudo leer %s: %s", path, e)
        return {}


def write_prices_json_if_changed(path: Path, prices_by_date: Dict[str, float]) -> bool:
    """
    Escribe lista [{date, close}] ordenada desc. Devuelve True si cambió el fichero.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{"date": d, "close": prices_by_date[d]} for d in sorted(prices_by_date.keys(), reverse=True)]
    new_text = json_dumps_canonical(rows)

    old_text = path.read_text(encoding="utf-8") if path.exists() else None
    if old_text == new_text:
        return False

    path.write_text(new_text, encoding="utf-8")
    return True
