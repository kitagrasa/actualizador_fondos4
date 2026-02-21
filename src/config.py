from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List
import logging
import requests

log = logging.getLogger(__name__)

@dataclass(frozen=True)
class FundConfig:
    isin: str
    ft_url: str          # Vacío → salta FT
    fundsquare_url: str  # Vacío → salta Fundsquare
    investing_url: str   # Vacío → salta Investing
    ariva_url: str       # Vacío → salta Ariva


def load_funds_csv(path_or_url: str | Path) -> List[FundConfig]:
    path_str = str(path_or_url).strip()
    lines = []

    # 1. Si es un enlace externo (Ej: Google Sheets publicado como CSV)
    if path_str.startswith("http://") or path_str.startswith("https://"):
        try:
            # Usamos requests que maneja redirecciones de Google automáticamente
            resp = requests.get(
                path_str, 
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                timeout=15
            )
            resp.raise_for_status()
            # Separamos el contenido en líneas para leerlas como CSV
            content = resp.text
            lines = content.splitlines()
        except Exception as e:
            log.error("Error descargando el CSV desde la web %s: %s", path_str, e)
            return []
            
    # 2. Fallback (por si acaso): Si es un archivo local tradicional en GitHub
    else:
        path = Path(path_or_url)
        if not path.exists():
            log.error("No existe el archivo local %s", path)
            return []
        with path.open("r", encoding="utf-8", newline="") as f:
            lines = f.readlines()

    if not lines:
        log.error("El origen de datos está vacío o no se pudo descargar.")
        return []

    funds: List[FundConfig] = []
    
    # csv.DictReader empareja automáticamente los datos usando la primera línea como cabecera
    reader = csv.DictReader(lines)
    
    if reader.fieldnames is None or "isin" not in reader.fieldnames:
        log.error("El origen de datos debe tener al menos la cabecera 'isin'.")
        return []

    for row in reader:
        isin = (row.get("isin") or "").strip()
        if not isin:
            continue
            
        funds.append(FundConfig(
            isin=isin,
            ft_url=(row.get("ft_url") or "").strip(),
            fundsquare_url=(row.get("fundsquare_url") or "").strip(),
            investing_url=(row.get("investing_url") or "").strip(),
            ariva_url=(row.get("ariva_url") or "").strip(),
        ))

    # Deduplicar por ISIN (última línea gana)
    return list({f.isin: f for f in funds}.values())
