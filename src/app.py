from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .config import load_funds_csv
from .http_client import build_session
from .portfolio import read_prices_json, write_prices_json_if_changed
from .utils import setup_logging, json_dumps_canonical
from .scrapers.ft_scraper import scrape_ft_prices
from .scrapers.fundsquare_scraper import scrape_fundsquare_prices
from .scrapers.investing_scraper import scrape_investing_prices

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
    funds_meta = meta.get("funds", {})
    for isin in [k for k in list(funds_meta) if k not in active]:
        log.info("Eliminando metadata de fondo eliminado: %s", isin)
        funds_meta.pop(isin, None)
        changed = True
    meta["funds"] = funds_meta
    return changed


def merge_updates(existing: Dict[str, float], *sources: List[Tuple[str, float]]) -> Dict[str, float]:
    """
    Fusiona existing con todas las fuentes.
    Cada fuente sobrescribe la fecha si el precio es diferente
    (independientemente de qué web lo obtuvo).
    """
    out = dict(existing)
    for source in sources:
        for d, c in source:
            out[d] = c
    return out


def _max_existing_date(existing: Dict[str, float]) -> Optional[date]:
    if not existing:
        return None
    try:
        return max(datetime.strptime(d, "%Y-%m-%d").date() for d in existing)
    except Exception:
        return None


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

    if cleanup_removed_funds([f.isin for f in funds], meta):
        any_changed = True

    full_refresh = os.getenv("FULL_REFRESH", "0").strip() == "1"
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "14"))
    today = date.today()

    for f in funds:
        isin = f.isin
        log.info("Procesando %s | FT=%s | FS=%s | INV=%s",
                 isin, f.ft_url or "—", f.fundsquare_url or "—", f.investing_url or "—")

        existing_path = PRICES_DIR / f"{isin}.json"
        existing = read_prices_json(existing_path)

        last_dt = _max_existing_date(existing)
        do_full = full_refresh or (not existing)
        start = (max(date(2000, 1, 1), last_dt - timedelta(days=lookback_days))
                 if (not do_full and last_dt) else None)

        ft_prices, ft_meta = scrape_ft_prices(
            session, f.ft_url, start_date=start, end_date=today, full_refresh=do_full)
        fs_prices = scrape_fundsquare_prices(session, f.fundsquare_url)
        inv_prices = scrape_investing_prices(
            session, f.investing_url, start_date=start, end_date=today, full_refresh=do_full)

        merged = merge_updates(existing, ft_prices, fs_prices, inv_prices)

        if write_prices_json_if_changed(existing_path, merged):
            log.info("Actualizado %s (%s puntos)", isin, len(merged))
            any_changed = True
        else:
            log.info("Sin cambios en %s", isin)

        # Metadata
        fmeta = meta.setdefault("funds", {}).setdefault(isin, {})
        for key, val in [("ft_url", f.ft_url), ("fundsquare_url", f.fundsquare_url),
                         ("investing_url", f.investing_url)]:
            if val and fmeta.get(key) != val:
                fmeta[key] = val
                any_changed = True
        for key, val in [("name", ft_meta.get("name")), ("currency", ft_meta.get("currency"))]:
            if val and fmeta.get(key) != val:
                fmeta[key] = val
                any_changed = True

    if save_metadata_if_changed(meta):
        any_changed = True

    log.info("Fin. Cambios=%s", any_changed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
