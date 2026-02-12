from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Tuple

from .config import load_funds_csv
from .http_client import build_session
from .portfolio import read_prices_json, write_prices_json_if_changed
from .utils import setup_logging, utc_now_iso, json_dumps_canonical, madrid_now_str
from .scrapers.ft_scraper import scrape_ft_prices_and_metadata
from .scrapers.fundsquare_scraper import scrape_fundsquare_prices

SOURCE_PRIORITY = {"ft": 20, "fundsquare": 10}

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PRICES_DIR = DATA_DIR / "prices"
META_FILE = DATA_DIR / "funds_metadata.json"
FUNDS_CSV = ROOT / "funds.csv"

log = logging.getLogger("app")


def load_metadata() -> Dict:
    if not META_FILE.exists():
        return {"updated_at_utc": None, "updated_at_madrid": None, "funds": {}}
    try:
        return json.loads(META_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"updated_at_utc": None, "updated_at_madrid": None, "funds": {}}


def save_metadata_if_changed(meta: Dict) -> bool:
    META_FILE.parent.mkdir(parents=True, exist_ok=True)
    new_text = json_dumps_canonical(meta)
    old_text = META_FILE.read_text(encoding="utf-8") if META_FILE.exists() else None
    if old_text == new_text:
        return False
    META_FILE.write_text(new_text, encoding="utf-8")
    return True


def cleanup_removed_funds(active_isins: List[str]) -> bool:
    changed = False
    active = set(active_isins)
    PRICES_DIR.mkdir(parents=True, exist_ok=True)

    for p in PRICES_DIR.glob("*.json"):
        isin = p.stem.strip()
        if isin and isin not in active:
            log.info("Borrando histórico de fondo eliminado: %s", isin)
            try:
                p.unlink()
                changed = True
            except Exception as e:
                log.error("No se pudo borrar %s: %s", p, e)

    meta = load_metadata()
    funds_meta = meta.get("funds", {})
    removed = [isin for isin in list(funds_meta.keys()) if isin not in active]
    for isin in removed:
        funds_meta.pop(isin, None)
        changed = True

    meta["funds"] = funds_meta
    meta["updated_at_utc"] = utc_now_iso()
    meta["updated_at_madrid"] = madrid_now_str()
    if save_metadata_if_changed(meta):
        changed = True
    return changed


def merge_updates(existing: Dict[str, float], ft: List[Tuple[str, float]], fs: List[Tuple[str, float]]) -> Dict[str, float]:
    """
    Regla clave:
    - FT (prioridad 20) sobrescribe siempre el mismo día si trae un precio.
    - Fundsquare solo rellena huecos (no sobrescribe si ya existe ese día).
    """
    out = dict(existing)

    # Fundsquare primero: rellena
    for d, c in fs:
        if d not in out:
            out[d] = c

    # FT después: sobrescribe
    for d, c in ft:
        out[d] = c

    return out


def main() -> int:
    setup_logging()
    log.info("Inicio actualización (UTC=%s, Madrid=%s)", utc_now_iso(), madrid_now_str())

    funds = load_funds_csv(FUNDS_CSV)
    if not funds:
        log.warning("No hay fondos en funds.csv")
        return 0

    session = build_session()

    any_changed = cleanup_removed_funds([f.isin for f in funds])

    meta = load_metadata()
    if "funds" not in meta:
        meta["funds"] = {}

    for f in funds:
        isin = f.isin
        id_instr = f.id_instr
        ft_symbol = f.ft_symbol

        log.info("Procesando %s (Fundsquare idInstr=%s, FT=%s)", isin, id_instr, ft_symbol)

        existing_path = PRICES_DIR / f"{isin}.json"
        existing = read_prices_json(existing_path)

        ft_prices, ft_meta = scrape_ft_prices_and_metadata(session, ft_symbol)
        fs_prices = scrape_fundsquare_prices(session, id_instr)

        merged = merge_updates(existing, ft_prices, fs_prices)

        changed_prices = write_prices_json_if_changed(existing_path, merged)
        if changed_prices:
            log.info("Actualizado %s (%s puntos)", existing_path, len(merged))
            any_changed = True
        else:
            log.info("Sin cambios en %s", existing_path)

        # Guardar metadata (1 solo archivo global)
        meta["funds"].setdefault(isin, {})
        if "name" in ft_meta:
            meta["funds"][isin]["name"] = ft_meta["name"]
        if "currency" in ft_meta:
            meta["funds"][isin]["currency"] = ft_meta["currency"]
        meta["funds"][isin]["ft_symbol"] = ft_symbol
        meta["funds"][isin]["fundsquare_idInstr"] = id_instr

    meta["updated_at_utc"] = utc_now_iso()
    meta["updated_at_madrid"] = madrid_now_str()
    if save_metadata_if_changed(meta):
        any_changed = True

    log.info("Fin. Cambios=%s", any_changed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
