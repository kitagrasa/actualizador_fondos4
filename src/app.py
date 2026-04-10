from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Dict, List, Tuple

# Tus importaciones habituales
from .config import load_funds_csv
from .httpclient import build_session
from .portfolio import read_prices_json, write_prices_json_if_changed
from .utils import setup_logging, utc_now_iso, madrid_now_str

# Scrapers
from .scrapers.ftscraper import scrape_ft_prices_and_metadata
from .scrapers.fundsquarescraper import scrape_fundsquare_prices
from .scrapers.investingscraper import scrape_investing_prices
from .scrapers.arivascraper import scrape_ariva_prices
from .scrapers.yahoofinancescraper import scrape_yahoo_finance_prices

ROOT = Path(__file__).resolve().parents[1]
DATADIR = ROOT / "data"
PRICESDIR = DATADIR / "prices"
FUNDS_CSV = ROOT / "funds.csv"

log = logging.getLogger("app")

def merge_updates(existing: Dict[str, float], *new_data_lists: List[Tuple[str, float]]) -> Dict[str, float]:
    """
    Combina datos respetando el orden en que se le pasan. 
    Los últimos parámetros sobreescriben a los anteriores si comparten la misma fecha.
    """
    out = dict(existing)
    for data_list in new_data_lists:
        for d, c in data_list:
            out[d] = c
    return out

def main() -> int:
    setup_logging()
    log.info("Inicio actualización UTC=%s", utc_now_iso())
    
    # Intenta leer variable de entorno, si no asume path local
    csv_path = os.getenv("FUNDS_CSV_URL", str(FUNDS_CSV))
    funds = load_funds_csv(csv_path)
    
    if not funds:
        log.warning("No hay fondos válidos para procesar.")
        return 0

    session = build_session()
    PRICESDIR.mkdir(parents=True, exist_ok=True)
    any_changed = False

    for f in funds:
        log.info("Procesando ISIN=%s FT=%s FS=%s INV=%s AR=%s YF=%s", 
                 f.isin, bool(f.ft_url), bool(f.fundsquare_url), 
                 bool(f.investing_url), bool(f.ariva_url), bool(f.yahoo_url))

        existing_path = PRICESDIR / f"{f.isin}.json"
        existing = read_prices_json(existing_path) or {}

        # 1. Ejecución de los Scrapers
        yahoo_prices, yahoo_meta = scrape_yahoo_finance_prices(session, f.yahoo_url)
        ariva_prices, ariva_meta = scrape_ariva_prices(session, f.ariva_url)
        inv_prices, inv_meta = scrape_investing_prices(session, f.investing_url)
        fs_prices = scrape_fundsquare_prices(session, f.fundsquare_url)
        ft_prices, ft_meta = scrape_ft_prices_and_metadata(session, f.ft_url)

        # 2. Fusión jerárquica (El último parámetro tiene máxima prioridad)
        # Prioridad actual: FT > FS > Investing > Ariva > Yahoo Finance
        merged = merge_updates(
            existing, 
            yahoo_prices,   # Fallback de menor prioridad
            ariva_prices, 
            inv_prices, 
            fs_prices, 
            ft_prices       # Máxima prioridad
        )

        # 3. Guardado en disco
        if write_prices_json_if_changed(existing_path, merged):
            log.info("Actualizado %s (%s puntos)", existing_path.name, len(merged))
            any_changed = True
        else:
            log.info("Sin cambios en %s", existing_path.name)

    log.info("Fin de la ejecución. Hubo cambios: %s", any_changed)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
