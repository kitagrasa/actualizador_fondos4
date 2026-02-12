from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple

from .config import load_funds_csv
from .http_client import build_session
from .portfolio import read_prices_json, write_prices_json_if_changed
from .utils import setup_logging, json_dumps_canonical
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
        return {"funds": {}}
    try:
        data = json.loads(META_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"funds": {}}
        if "funds" not in data or not isinstance(data["funds"], dict):
            data["funds"] = {}
        return data
    except Exception:
        return {"funds": {}}


def save_metadata_if_changed(meta: Dict) -> bool:
    META_FILE.parent.mkdir(parents=True, exist_ok=True)
    new_text = json_dumps_canonical(meta)
    old_text = META_FILE.read_text(encoding="utf-8") if META_FILE.exists() else None
    if old_text == new_text:
        return False
    META_FILE.write_text(new_text, encoding="utf-8")
    return True


def cleanup_removed_funds(active_isins: List[str], meta: Dict) -> bool:
    """
    Si quitas un fondo del funds.csv:
    - Borra data/prices/{ISIN}.json
    - Lo elimina de meta["funds"]
    """
    changed = False
    active = set(active_isins)

    PRICES_DIR.mkdir(parents=True, exist_ok=True)

    # Borrar históricos
    for p in PRICES_DIR.glob("*.json"):
        isin = p.stem.strip()
        if isin and isin not in active:
            log.info("Borrando histórico de fondo eliminado: %s", isin)
            try:
                p.unlink()
                changed = True
            except Exception as e:
                log.error("No se pudo borrar %s: %s", p, e)

    # Limpiar metadata
    funds_meta = meta.get("funds", {})
    removed = [isin for isin in list(funds_meta.keys()) if isin not in active]
    for isin in removed:
        log.info("Eliminando metadata de fondo eliminado: %s", isin)
        funds_meta.pop(isin, None)
        changed = True

    meta["funds"] = funds_meta
    return changed


def merge_updates(
    existing: Dict[str, float],
    ft: List[Tuple[str, float]],
    fs: List[Tuple[str, float]],
) -> Dict[str, float]:
    """
    Regla:
    - Fundsquare rellena huecos.
    - FT sobrescribe siempre mismo día (prioridad mayor).
    """
    out = dict(existing)

    for d, c in fs:
        if d not in out:
            out[d] = c

    for d, c in ft:
        out[d] = c

    return out


def main() -> int:
    setup_logging()
    log.info("Inicio actualización")

    funds = load_funds_csv(FUNDS_CSV)
    if not funds:
        log.warning("No hay fondos en funds.csv")
        return 0

    session = build_session()

    meta = load_metadata()
    any_changed = False

    # Limpieza automática si quitaste fondos
    if cleanup_removed_funds([f.isin for f in funds], meta):
        any_changed = True

    # Procesar fondos
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

        if write_prices_json_if_changed(existing_path, merged):
            log.info("Actualizado %s (%s puntos)", existing_path, len(merged))
            any_changed = True
        else:
            log.info("Sin cambios en %s", existing_path)

        # Metadata del fondo (solo se guardará si cambia realmente)
        funds_meta = meta.setdefault("funds", {})
        fmeta = funds_meta.setdefault(isin, {})

        # Siempre guardar los identificadores “fuente”
        if fmeta.get("ft_symbol") != ft_symbol:
            fmeta["ft_symbol"] = ft_symbol
            any_changed = True
        if fmeta.get("fundsquare_idInstr") != id_instr:
            fmeta["fundsquare_idInstr"] = id_instr
            any_changed = True

        # Intentar completar nombre/divisa desde FT (si FT los da)
        if "name" in ft_meta and ft_meta["name"]:
            if fmeta.get("name") != ft_meta["name"]:
                fmeta["name"] = ft_meta["name"]
                any_changed = True
        if "currency" in ft_meta and ft_meta["currency"]:
            if fmeta.get("currency") != ft_meta["currency"]:
                fmeta["currency"] = ft_meta["currency"]
                any_changed = True

    # Guardar metadata solo si cambió
    if save_metadata_if_changed(meta):
        # Ojo: any_changed puede ser False si el meta cambió pero no precios (por ejemplo, primera ejecución)
        any_changed = True

    log.info("Fin. Cambios=%s", any_changed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
