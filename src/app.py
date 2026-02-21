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
from .scrapers.ariva_scraper import scrape_ariva_prices

ROOT       = Path(__file__).resolve().parents[1]
DATA_DIR   = ROOT / "data"
PRICES_DIR = DATA_DIR / "prices"
META_FILE  = DATA_DIR / "funds_metadata.json"

log = logging.getLogger("app")


# ── Metadata ─────────────────────────────────────────────────────────────────

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


# ── Limpieza fondos eliminados ────────────────────────────────────────────────

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


# ── Merge de fuentes ──────────────────────────────────────────────────────────

def merge_updates(
    existing: Dict[str, float],
    *sources: List[Tuple[str, float]],
) -> Dict[str, float]:
    """Cada fuente sobrescribe por fecha. El último que escribe gana."""
    out = dict(existing)
    for source in sources:
        for d, c in source:
            out[d] = c
    return out


def max_existing_date(existing: Dict[str, float]) -> Optional[date]:
    if not existing:
        return None
    try:
        return max(datetime.strptime(d, "%Y-%m-%d").date() for d in existing.keys())
    except Exception:
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    setup_logging()
    log.info("Inicio actualización")

    # ── NUEVO: Control estricto del Secreto ──
    funds_csv_url = os.environ.get("FUNDS_CSV_URL", "").strip()
    if not funds_csv_url.startswith("http"):
        log.error("CRÍTICO: El secreto FUNDS_CSV_URL no está configurado o no es una URL válida. Abortando.")
        return 1  # Exit code 1 forzará a GitHub Actions a fallar y avisarte
        
    funds = load_funds_csv(funds_csv_url)
    if not funds:
        log.error("CRÍTICO: La descarga devolvió 0 fondos. Verifica la URL de Google Sheets. Abortando.")
        return 1

    session      = build_session()
    meta         = load_metadata()
    any_changed  = False

    if cleanup_removed_funds([f.isin for f in funds], meta):
        any_changed = True

    fullrefresh   = os.getenv("FULLREFRESH", "0").strip() == "1"
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "14"))
    today         = date.today()

    for f in funds:
        isin = f.isin
        ariva_url = getattr(f, "ariva_url", None)
        
        log.info("Procesando %s  FT=%s  FS=%s  INV=%s  ARV=%s",
                 isin,
                 getattr(f, "ft_url", "—") or "—",
                 getattr(f, "fundsquare_url", "—") or "—",
                 getattr(f, "investing_url", "—") or "—",
                 ariva_url or "—")

        existing_path = PRICES_DIR / f"{isin}.json"
        existing      = read_prices_json(existing_path)
        last_dt       = max_existing_date(existing)
        fmeta         = meta.setdefault("funds", {}).setdefault(isin, {})

        do_full = fullrefresh or not existing
        start   = (
            max(date(2000, 1, 1), last_dt - timedelta(days=lookback_days))
            if (not do_full and last_dt)
            else None
        )

        # ── FT ───────────────────────────────────────────────────────────────
        ft_prices, ft_meta = scrape_ft_prices(
            session,
            getattr(f, "ft_url", None),
            startdate=start,
            enddate=today,
            fullrefresh=do_full,
        )

        # ── Fundsquare ────────────────────────────────────────────────────────
        fs_prices = scrape_fundsquare_prices(session, getattr(f, "fundsquare_url", None))

        # ── Investing ─────────────────────────────────────────────────────────
        cached_pair_id = fmeta.get("investing_pair_id") or None
        inv_result = scrape_investing_prices(
            session,
            getattr(f, "investing_url", None),
            cached_pair_id=cached_pair_id,
            startdate=start,
            enddate=today,
            fullrefresh=do_full,
        )
        if isinstance(inv_result, tuple) and len(inv_result) == 2:
            inv_prices, inv_pair_id = inv_result
        else:
            inv_prices, inv_pair_id = (inv_result or []), None

        if inv_pair_id and fmeta.get("investing_pair_id") != inv_pair_id:
            fmeta["investing_pair_id"] = inv_pair_id
            any_changed = True

        # ── Ariva ─────────────────────────────────────────────────────────────
        ariva_prices_tuples = []
        if ariva_url:
            ariva_result = scrape_ariva_prices(ariva_url)
            if isinstance(ariva_result, tuple) and len(ariva_result) >= 1:
                raw_ariva = ariva_result[0]
                if raw_ariva:
                    if isinstance(raw_ariva[0], dict):
                        ariva_prices_tuples = [(p["date"], p["close"]) for p in raw_ariva if "date" in p and "close" in p]
                    else:
                        ariva_prices_tuples = raw_ariva

        # ── Merge y guardado ──────────────────────────────────────────────────
        merged = merge_updates(existing, ft_prices, fs_prices, inv_prices, ariva_prices_tuples)

        if write_prices_json_if_changed(existing_path, merged):
            log.info("Actualizado %s → %s puntos", isin, len(merged))
            any_changed = True
        else:
            log.info("Sin cambios en %s", isin)

        # ── Metadata ──────────────────────────────────────────────────────────
        for key, val in [
            ("ft_url",         getattr(f, "ft_url", None)),
            ("fundsquare_url", getattr(f, "fundsquare_url", None)),
            ("investing_url",  getattr(f, "investing_url", None)),
            ("ariva_url",      ariva_url),
        ]:
            if val and fmeta.get(key) != val:
                fmeta[key] = val
                any_changed = True

        for key, val in [
            ("name",     ft_meta.get("name") if isinstance(ft_meta, dict) else None),
            ("currency", ft_meta.get("currency") if isinstance(ft_meta, dict) else None),
        ]:
            if val and fmeta.get(key) != val:
                fmeta[key] = val
                any_changed = True

    if save_metadata_if_changed(meta):
        any_changed = True

    log.info("Fin. Cambios=%s", any_changed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
