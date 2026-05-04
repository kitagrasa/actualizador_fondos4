from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List
import requests

log = logging.getLogger(__name__)

@dataclass(frozen=True)
class FundConfig:
    isin: str
    ft_url: str          # Vacío → salta FT
    fundsquare_url: str  # Vacío → salta Fundsquare
    investing_url: str   # Vacío → salta Investing
    ariva_url: str       # Vacío → salta Ariva
    yahoo_url: str       # Vacío → salta Yahoo Finance
    cobas_url: str       # Vacío → salta Cobas AM


def load_funds_csv(path_or_url: str | Path) -> List[FundConfig]:
    path_str = str(path_or_url).strip()
    lines = []
    content = ""

    # 1. Si es un enlace externo (Google Sheets publicado como CSV)
    if path_str.startswith("http://") or path_str.startswith("https://"):
        try:
            resp = requests.get(
                path_str,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                timeout=15
            )
            resp.raise_for_status()
            content = resp.text
            # Eliminar BOM (carácter invisible que Google a veces añade)
            if content.startswith('\ufeff'):
                content = content[1:]
            lines = content.splitlines()
            log.info("CSV remoto descargado: %d líneas (sin BOM).", len(lines))
        except Exception as e:
            log.error("Error descargando el CSV desde la web %s: %s", path_str, e)
            return []
    # 2. Fallback: archivo local
    else:
        path = Path(path_or_url)
        if not path.exists():
            log.error("No existe el archivo local %s", path)
            return []
        with path.open("r", encoding="utf-8", newline="") as f:
            content = f.read()
            if content.startswith('\ufeff'):
                content = content[1:]
            lines = content.splitlines()

    if not lines:
        log.error("El origen de datos está vacío o no se pudo descargar.")
        return []

    # Usar csv.DictReader con las líneas limpias
    reader = csv.DictReader(lines)
    if reader.fieldnames is None:
        log.error("El CSV no tiene cabeceras.")
        return []
    fieldnames = [fn.strip() for fn in reader.fieldnames if fn]
    log.info("Cabeceras detectadas: %s", fieldnames)

    if "isin" not in fieldnames:
        log.error("El origen de datos debe tener al menos la cabecera 'isin'. Cabeceras reales: %s", fieldnames)
        return []

    funds: List[FundConfig] = []
    for row in reader:
        # Ignorar filas vacías completamente
        if not any(row.values()):
            continue
        isin = (row.get("isin") or "").strip()
        if not isin:
            continue

        funds.append(FundConfig(
            isin=isin,
            ft_url=(row.get("ft_url") or "").strip(),
            fundsquare_url=(row.get("fundsquare_url") or "").strip(),
            investing_url=(row.get("investing_url") or "").strip(),
            ariva_url=(row.get("ariva_url") or "").strip(),
            yahoo_url=(row.get("yahoo_url") or "").strip(),
            cobas_url=(row.get("cobas_url") or "").strip(),
        ))

    # Deduplicar por ISIN (última línea gana)
    dedup = list({f.isin: f for f in funds}.values())
    log.info("Fondos leídos: %d (tras deduplicación)", len(dedup))
    return dedup
