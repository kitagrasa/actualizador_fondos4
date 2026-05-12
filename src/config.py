"""
Módulo de configuración: carga la lista de fondos desde un CSV remoto (Google Sheets) o local.
Robustez ante: BOM, campos vacíos, mayúsculas/minúsculas en cabeceras, filas sin ISIN, duplicados.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import requests

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FundConfig:
    """Configuración de un fondo: ISIN y URLs de las distintas fuentes (FT, Fundsquare, Ariva, Yahoo, Cobas)."""
    isin: str
    ft_url: str          # Vacío → se omite Financial Times
    fundsquare_url: str  # Vacío → se omite Fundsquare
    investing_url: str   # Vacío → se omite Investing.com (aunque ya no se usa activamente)
    ariva_url: str       # Vacío → se omite Ariva
    yahoo_url: str       # Vacío → se omite Yahoo Finance
    cobas_url: str       # Vacío → se omite Cobas AM


def _normalize_fieldnames(fieldnames: List[str]) -> List[str]:
    """Convierte los nombres de columna a minúsculas y elimina espacios laterales."""
    return [fn.strip().lower() for fn in fieldnames if fn]


def _get_column_value(row: dict, key: str) -> str:
    """
    Obtiene el valor de una columna buscando por nombre normalizado (minúsculas).
    Si la columna no existe, devuelve cadena vacía.
    """
    for k, v in row.items():
        if k.strip().lower() == key:
            return (v or "").strip()
    return ""


def load_funds_csv(path_or_url: str | Path) -> List[FundConfig]:
    """
    Carga la configuración desde un CSV remoto (HTTP/HTTPS) o archivo local.
    El CSV debe contener al menos la columna 'isin'.
    Las demás columnas esperadas: ft_url, fundsquare_url, investing_url, ariva_url, yahoo_url, cobas_url.
    Retorna una lista de FundConfig (sin duplicados por ISIN, prevalece la última ocurrencia).
    """
    path_str = str(path_or_url).strip()
    content = ""

    # 1. Obtener contenido (remoto o local)
    if path_str.startswith(("http://", "https://")):
        try:
            resp = requests.get(
                path_str,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                timeout=15
            )
            resp.raise_for_status()
            content = resp.text
            # Eliminar BOM (carácter invisible que a veces añade Google Sheets)
            if content.startswith('\ufeff'):
                content = content[1:]
            log.info("CSV remoto descargado: %d caracteres", len(content))
        except Exception as e:
            log.error("Error descargando CSV desde %s: %s", path_str, e)
            return []
    else:
        path = Path(path_or_url)
        if not path.exists():
            log.error("Archivo local no existe: %s", path)
            return []
        with path.open("r", encoding="utf-8", newline="") as f:
            content = f.read()
            if content.startswith('\ufeff'):
                content = content[1:]

    if not content.strip():
        log.error("El origen de datos está vacío.")
        return []

    # 2. Parsear CSV usando csv.DictReader
    lines = content.splitlines()
    reader = csv.DictReader(lines)
    if reader.fieldnames is None:
        log.error("El CSV no tiene cabeceras.")
        return []

    normalized_headers = _normalize_fieldnames(reader.fieldnames)
    if "isin" not in normalized_headers:
        log.error("El CSV debe tener una columna 'isin'. Cabeceras detectadas: %s", reader.fieldnames)
        return []

    funds: List[FundConfig] = []
    for row_num, row in enumerate(reader, start=2):  # start=2 porque línea 1 son cabeceras
        # Ignorar filas completamente vacías
        if not any(row.values()):
            continue

        isin = _get_column_value(row, "isin")
        if not isin:
            log.debug("Fila %d sin ISIN, omitida", row_num)
            continue

        funds.append(FundConfig(
            isin=isin,
            ft_url=_get_column_value(row, "ft_url"),
            fundsquare_url=_get_column_value(row, "fundsquare_url"),
            investing_url=_get_column_value(row, "investing_url"),
            ariva_url=_get_column_value(row, "ariva_url"),
            yahoo_url=_get_column_value(row, "yahoo_url"),
            cobas_url=_get_column_value(row, "cobas_url"),
        ))

    # 3. Deduplicar por ISIN (la última ocurrencia sobrescribe a las anteriores)
    dedup_map = {}
    for fund in funds:
        dedup_map[fund.isin] = fund
    unique_funds = list(dedup_map.values())

    log.info("Fondos cargados: %d (originales %d, duplicados eliminados)", len(unique_funds), len(funds))
    return unique_funds
