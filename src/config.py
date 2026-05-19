"""
Módulo de configuración: carga la lista de fondos desde un CSV remoto
(Google Sheets) o local. Robusto ante BOM, campos vacíos,
mayúsculas/minúsculas en cabeceras, filas sin ISIN, duplicados.
"""
from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List

import requests

log = logging.getLogger(__name__)


def _normalize_field_names(fieldnames: List[str]) -> List[str]:
    """Convierte los nombres de columna a minúsculas y elimina espacios."""
    return [fn.strip().lower() for fn in fieldnames if fn]


def _get_column_value(row: dict, *keys: str) -> str:
    """
    Obtiene el valor de una columna buscando por múltiples nombres posibles
    (normalizado a minúsculas). Si ninguna coincide, devuelve cadena vacía.
    Así soporta tanto 'fturl' como 'ft_url', 'FT_URL', etc.
    """
    for k, v in row.items():
        k_norm = k.strip().lower()
        for key in keys:
            if k_norm == key.strip().lower():
                return (v or "").strip()
    return ""


@dataclass(frozen=True)
class FundConfig:
    """Configuración de un fondo: ISIN y URLs de las distintas fuentes."""
    isin: str
    fturl: str               # Financial Times
    fundsquareurl: str       # Fundsquare
    investingurl: str        # Investing.com (no activo)
    arivaurl: str            # Ariva
    yahoourl: str            # Yahoo Finance
    cobasurl: str            # Cobas AM
    genericurl: str          # Scraper genérico
    genericselector: str     # Selector CSS del precio
    genericselectorfecha: str  # Selector CSS de la fecha publicada en la web


def load_funds_csv(path_or_url: str | Path) -> List[FundConfig]:
    """
    Carga la configuración desde un CSV remoto (HTTP/HTTPS) o archivo local.
    Soporta nombres de columna con o sin guiones bajos, en cualquier capitalización.
    Retorna una lista de FundConfig sin duplicados por ISIN.
    """
    path_str = str(path_or_url).strip()
    content = ""

    # 1. Obtener contenido remoto o local
    if path_str.startswith(("http://", "https://")):
        try:
            resp = requests.get(
                path_str,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                timeout=15,
            )
            resp.raise_for_status()
            content = resp.text
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

    # 2. Eliminar BOM (carácter invisible que a veces añade Google Sheets)
    if content.startswith("\ufeff"):
        content = content[1:]

    if not content.strip():
        log.error("El origen de datos está vacío.")
        return []

    lines = content.splitlines()
    reader = csv.DictReader(lines)

    if reader.fieldnames is None:
        log.error("El CSV no tiene cabeceras.")
        return []

    normalized_headers = _normalize_field_names(reader.fieldnames)
    if "isin" not in normalized_headers:
        log.error(
            "El CSV debe tener una columna 'isin'. Cabeceras detectadas: %s",
            reader.fieldnames,
        )
        return []

    # 3. Parsear CSV — cada campo acepta nombre con o sin guión bajo
    funds: List[FundConfig] = []
    for rownum, row in enumerate(reader, start=2):
        if not any(row.values()):
            continue
        isin = _get_column_value(row, "isin")
        if not isin:
            log.debug("Fila %d sin ISIN, omitida", rownum)
            continue
        funds.append(
            FundConfig(
                isin=isin,
                fturl=_get_column_value(row, "ft_url", "fturl"),
                fundsquareurl=_get_column_value(row, "fundsquare_url", "fundsquareurl"),
                investingurl=_get_column_value(row, "investing_url", "investingurl"),
                arivaurl=_get_column_value(row, "ariva_url", "arivaurl"),
                yahoourl=_get_column_value(row, "yahoo_url", "yahoourl"),
                cobasurl=_get_column_value(row, "cobas_url", "cobasurl"),
                genericurl=_get_column_value(row, "generic_url", "genericurl"),
                genericselector=_get_column_value(row, "generic_selector", "genericselector"),
                genericselectorfecha=_get_column_value(row, "genericselectorfecha", "generic_selector_fecha"),
            )
        )

    # 4. Deduplicar por ISIN (la última ocurrencia sobrescribe anteriores)
    dedup_map = {}
    for fund in funds:
        dedup_map[fund.isin] = fund
    unique_funds = list(dedup_map.values())

    log.info(
        "Fondos cargados: %d originales, %d duplicados eliminados",
        len(unique_funds),
        len(funds) - len(unique_funds),
    )
    return unique_funds
