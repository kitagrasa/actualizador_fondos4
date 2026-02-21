from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from .config import load_funds_csv
from .http_client import build_session
from .portfolio import read_prices_json, write_prices_json_if_changed
from .utils import setup_logging, json_dumps_canonical

# Importa módulos (no funciones) para no romper por nombres diferentes
from .scrapers import ft_scraper, fundsquare_scraper, investing_scraper  # type: ignore

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PRICES_DIR = DATA_DIR / "prices"
META_FILE = DATA_DIR / "fundsmetadata.json"
FUNDS_CSV = ROOT / "funds.csv"

log = logging.getLogger("app")


# ── Helpers ──────────────────────────────────────────────────────────────────

def _resolve_callable(module: Any, names: List[str]) -> Callable:
    for n in names:
        fn = getattr(module, n, None)
        if callable(fn):
            return fn
    raise ImportError(f"No se encontró ninguna función válida en {module.__name__}: {names}")


def _qs_param(url: str, key: str) -> Optional[str]:
    try:
        q = parse_qs(urlparse(url).query)
        v = q.get(key, [None])[0]
        return v.strip() if isinstance(v, str) and v.strip() else None
    except Exception:
        return None


def _extract_ft_symbol_from_url(ft_url: str) -> Optional[str]:
    # FT tearsheet suele llevar ?s=LUxxxx:EUR o ?s=LUxxxxEUR, etc.
    return _qs_param(ft_url, "s")


def _extract_fundsquare_idinstr_from_url(fs_url: str) -> Optional[str]:
    return _qs_param(fs_url, "idInstr")


# ── Metadata ──────────────────────────────────────────────────────────────────

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


# ── Limpieza ─────────────────────────────────────────────────────────────────

def cleanup_removed_funds(active_isins: List[str], meta: Dict) -> bool:
    changed = False
    active = set(active_isins)
    PRICES_DIR.mkdir(parents=True, exist_ok=True)

    for p in PRICES_DIR.glob("*.json"):
        isin = p.stem.strip()
        if isin and isin not in active:
            log.info("Borrando histórico de fondo eliminado %s", isin)
            try:
                p.unlink()
                changed = True
            except Exception as e:
                log.error("No se pudo borrar %s: %s", p, e)

    funds_meta = meta.get("funds", {})
    for isin in list(funds_meta.keys()):
        if isin not in active:
            log.info("Eliminando metadata de fondo eliminado %s", isin)
            funds_meta.pop(isin, None)
            changed = True

    meta["funds"] = funds_meta
    return changed


# ── Merge ────────────────────────────────────────────────────────────────────

def merge_updates(
    existing: Dict[str, float],
    *sources: List[Tuple[str, float]],
) -> Dict[str, float]:
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


# ── Main ─────────────────────────────────────────────────────────────────────

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

    full_refresh = os.getenv("FULLREFRESH", "0").strip() == "1"
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "14"))
    today = date.today()

    # Resolver funciones existentes (soporta nombres antiguos y nuevos)
    scrape_ft = _resolve_callable(
        ft_scraper,
        [
            "scrape_ft_prices_and_metadata",
            "scrape_ft_prices",
            "scrapeftpricesandmetadata",
            "scrapeftpricesandmetadata_session",  # por si acaso
            "scrapeftprices",  # por si tu módulo expone solo prices
        ],
    )

    scrape_fs = _resolve_callable(
        fundsquare_scraper,
        [
            "scrape_fundsquare_prices",
            "scrapefundsquareprices",
        ],
    )

    scrape_inv = _resolve_callable(
        investing_scraper,
        [
            "scrape_investing_prices",
            "scrapeinvestingprices",
        ],
    )

    for f in funds:
        isin = f.isin
        log.info(
            "Procesando %s | FT=%s | FS=%s | INV=%s",
            isin,
            f.ft_url or "—",
            f.fundsquare_url or "—",
            f.investing_url or "—",
        )

        existing_path = PRICES_DIR / f"{isin}.json"
        existing = read_prices_json(existing_path)
        last_dt = max_existing_date(existing)

        fmeta = meta.setdefault("funds", {}).setdefault(isin, {})

        do_full = full_refresh or not existing
        start = (
            max(date(2000, 1, 1), last_dt - timedelta(days=lookback_days))
            if (not do_full and last_dt)
            else None
        )

        # ── FT ──────────────────────────────────────────────────────────────
        ft_prices: List[Tuple[str, float]] = []
        ft_meta: Dict[str, Any] = {}

        if f.ft_url:
            # Intento 1: API nueva (url + kwargs start_date/end_date/full_refresh)
            try:
                res = scrape_ft(
                    session,
                    f.ft_url,
                    start_date=start,
                    end_date=today,
                    full_refresh=do_full,
                )
                if isinstance(res, tuple) and len(res) == 2:
                    ft_prices, ft_meta = res  # type: ignore[misc]
                else:
                    ft_prices = res or []  # type: ignore[assignment]
            except TypeError:
                # Intento 2: API vieja (ftsymbol extraído de ?s=... + startdate/enddate/fullrefresh)
                ftsymbol = _extract_ft_symbol_from_url(f.ft_url) or f.ft_url
                try:
                    res = scrape_ft(
                        session,
                        ftsymbol,
                        startdate=start,
                        enddate=today,
                        fullrefresh=do_full,
                    )
                    if isinstance(res, tuple) and len(res) == 2:
                        ft_prices, ft_meta = res  # type: ignore[misc]
                    else:
                        ft_prices = res or []  # type: ignore[assignment]
                except Exception as e:
                    log.warning("FT falló para %s: %s", isin, e)
            except Exception as e:
                log.warning("FT falló para %s: %s", isin, e)

        # ── Fundsquare ───────────────────────────────────────────────────────
        fs_prices: List[Tuple[str, float]] = []
        if f.fundsquare_url:
            try:
                # Intento 1: espera URL completa
                fs_prices = scrape_fs(session, f.fundsquare_url) or []
            except TypeError:
                # Intento 2: espera idInstr
                idinstr = _extract_fundsquare_idinstr_from_url(f.fundsquare_url)
                if idinstr:
                    fs_prices = scrape_fs(session, idinstr) or []
            except Exception as e:
                log.warning("Fundsquare falló para %s: %s", isin, e)

        # ── Investing ────────────────────────────────────────────────────────
        inv_prices: List[Tuple[str, float]] = []
        inv_pair_id: Optional[str] = None

        if f.investing_url:
            cached_pair_id = fmeta.get("investing_pair_id") or None
            try:
                inv_res = scrape_inv(
                    session,
                    f.investing_url,
                    cached_pair_id=cached_pair_id,
                    start_date=start,
                    end_date=today,
                    full_refresh=do_full,
                )
            except TypeError:
                # API vieja
                inv_res = scrape_inv(
                    session,
                    f.investing_url,
                    startdate=start,
                    enddate=today,
                    fullrefresh=do_full,
                )

            if isinstance(inv_res, tuple) and len(inv_res) == 2:
                inv_prices, inv_pair_id = inv_res  # type: ignore[misc]
            else:
                inv_prices = inv_res or []  # type: ignore[assignment]

            if inv_pair_id and fmeta.get("investing_pair_id") != inv_pair_id:
                fmeta["investing_pair_id"] = inv_pair_id
                any_changed = True

        # ── Merge y guardado ─────────────────────────────────────────────────
        merged = merge_updates(existing, ft_prices, fs_prices, inv_prices)

        if write_prices_json_if_changed(existing_path, merged):
            log.info("Actualizado %s → %s puntos", isin, len(merged))
            any_changed = True
        else:
            log.info("Sin cambios en %s", isin)

        # ── Metadata ─────────────────────────────────────────────────────────
        for key, val in [
            ("ft_url", f.ft_url),
            ("fundsquare_url", f.fundsquare_url),
            ("investing_url", f.investing_url),
            ("name", ft_meta.get("name") if isinstance(ft_meta, dict) else None),
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
