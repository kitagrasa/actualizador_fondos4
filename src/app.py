"""
Orquestador principal: descarga precios de múltiples fuentes y fusiona
según prioridad.
Jerarquía (de menor a mayor prioridad):
  Generic → Ariva → Fundsquare → FT → Yahoo → Cobas
"""
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
from .scrapers.ariva_scraper import scrape_ariva_prices
from .scrapers.yahoofinance_scraper import scrape_yahoo_finance_prices
from .scrapers.cobas_scraper import scrape_cobas_prices
from .scrapers.generic_scraper import scrape_generic_prices

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PRICES_DIR = DATA_DIR / "prices"
META_FILE = DATA_DIR / "fundsmetadata.json"

log = logging.getLogger("app")


def load_metadata() -> Dict:
    """Carga fundsmetadata.json. Si no existe o está corrupto, retorna estructura vacía."""
    if not META_FILE.exists():
        return {"funds": {}}
    try:
        data = json.loads(META_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"funds": {}}
        if "funds" not in data or not isinstance(data["funds"], dict):
            data["funds"] = {}
        return data
    except Exception as e:
        log.warning("Error leyendo metadatos: %s", e)
        return {"funds": {}}


def save_metadata_if_changed(meta: Dict) -> bool:
    """Guarda metadatos solo si ha habido cambios (evita escrituras innecesarias)."""
    META_FILE.parent.mkdir(parents=True, exist_ok=True)
    new_text = json_dumps_canonical(meta)
    old_text = META_FILE.read_text(encoding="utf-8") if META_FILE.exists() else None
    if old_text == new_text:
        return False
    META_FILE.write_text(new_text, encoding="utf-8")
    return True


def cleanup_removed_funds(active_isins: List[str], meta: Dict) -> bool:
    """
    Borra archivos JSON de precios y entradas en metadatos de ISINs
    que ya no figuran en la configuración activa.
    """
    changed = False
    active_set = set(active_isins)
    PRICES_DIR.mkdir(parents=True, exist_ok=True)
    for p in PRICES_DIR.glob("*.json"):
        isin = p.stem.strip()
        if isin and isin not in active_set:
            log.info("Eliminando histórico de fondo eliminado: %s", isin)
            try:
                p.unlink()
                changed = True
            except Exception as e:
                log.error("No se pudo borrar %s: %s", p, e)
    funds_meta = meta.get("funds", {})
    for isin in list(funds_meta.keys()):
        if isin not in active_set:
            log.info("Eliminando metadata de fondo eliminado: %s", isin)
            funds_meta.pop(isin, None)
            changed = True
    meta["funds"] = funds_meta
    return changed


def merge_updates(
    existing: Dict[str, float],
    *sources: Optional[List[Tuple[str, float]]],
) -> Dict[str, float]:
    """
    Fusiona el histórico existente con los datos de nuevas fuentes.
    El último en la lista tiene máxima prioridad.
    Jerarquía: Generic (menor) → Ariva → Fundsquare → FT → Yahoo → Cobas (mayor).
    """
    result = dict(existing)
    for source in sources:
        if not source:
            continue
        for date_str, price in source:
            result[date_str] = price
    return result


def max_existing_date(existing: Dict[str, float]) -> Optional[date]:
    """Retorna la fecha más reciente del histórico actual, None si vacío."""
    if not existing:
        return None
    try:
        return max(datetime.strptime(d, "%Y-%m-%d").date() for d in existing.keys())
    except Exception:
        return None


def main() -> int:
    setup_logging()
    log.info("Inicio de actualización de precios")

    funds_csv_url = os.environ.get("FUNDS_CSV_URL", "").strip()
    if not funds_csv_url.startswith(("http://", "https://")):
        log.error("CRÍTICO: La variable FUNDS_CSV_URL no es una URL válida. Abortando.")
        return 1

    funds = load_funds_csv(funds_csv_url)
    if not funds:
        log.error("CRÍTICO: No se cargaron fondos desde el CSV.")
        return 1

    session = build_session()
    meta = load_metadata()
    any_changed = False

    if cleanup_removed_funds([f.isin for f in funds], meta):
        any_changed = True

    full_refresh = os.getenv("FULL_REFRESH", "0").strip() == "1"
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "14"))
    today = date.today()

    for fund in funds:
        isin = fund.isin
        ariva_url = fund.arivaurl or None
        yahoo_url = fund.yahoourl or None
        cobas_url = fund.cobasurl or None
        generic_url = fund.genericurl or None
        generic_selector = fund.genericselector or None
        # ← NUEVO: selector CSS de la fecha publicada en la web
        generic_selector_fecha = fund.genericselectorfecha or None

        log.info(
            "Procesando %s | FT=%s | FS=%s | ARIVA=%s | COBAS=%s | YAHOO=%s | GENERIC=%s",
            isin,
            fund.fturl or "—",
            fund.fundsquareurl or "—",
            ariva_url or "—",
            cobas_url or "—",
            yahoo_url or "—",
            generic_url or "—",
        )

        existing_path = PRICES_DIR / f"{isin}.json"
        existing = read_prices_json(existing_path)
        last_date = max_existing_date(existing)

        do_full = full_refresh or not existing
        start_date = (
            max(date(2000, 1, 1), last_date - timedelta(days=lookback_days))
            if not do_full and last_date
            else None
        )

        # 0. Scraper genérico (mínima prioridad)
        # ← CORRECCIÓN BUG: se pasa selector_fecha para que la web aporte su propia fecha
        generic_prices = []
        if generic_url and generic_selector:
            result = scrape_generic_prices(
                session=session,
                url=generic_url,
                selector=generic_selector,
                selector_fecha=generic_selector_fecha,  # ← NUEVO PARÁMETRO
            )
            generic_prices = result if result else []

        # 1. Yahoo Finance (siempre pide 10 años: red de seguridad)
        yf_prices, yf_meta = [], {}
        if yahoo_url:
            yf_prices, yf_meta = scrape_yahoo_finance_prices(
                session,
                yahoo_url,
                start_date=None,
                end_date=today,
                full_refresh=True,
            )

        # 2. Financial Times (incremental o completo)
        ft_prices, ft_meta = scrape_ft_prices(
            session,
            fund.fturl,
            start_date=start_date,
            end_date=today,
            full_refresh=do_full,
        )

        # 3. Fundsquare
        fs_prices = scrape_fundsquare_prices(session, fund.fundsquareurl)

        # 4. Ariva
        ariva_tuples = []
        if ariva_url:
            ariva_result = scrape_ariva_prices(ariva_url)
            if isinstance(ariva_result, tuple) and len(ariva_result) >= 1:
                raw_ariva = ariva_result[0]
                if raw_ariva:
                    if isinstance(raw_ariva[0], dict):
                        ariva_tuples = [
                            (p["date"], p["close"])
                            for p in raw_ariva
                            if "date" in p and "close" in p
                        ]
                    else:
                        ariva_tuples = raw_ariva

        # 5. Cobas AM (máxima prioridad)
        cobas_prices = scrape_cobas_prices(session, cobas_url)

        # Fusión: Generic (menor) → Ariva → Fundsquare → FT → Yahoo → Cobas (mayor)
        merged = merge_updates(
            existing,
            generic_prices,
            ariva_tuples,
            fs_prices,
            ft_prices,
            yf_prices,
            cobas_prices,
        )

        if write_prices_json_if_changed(existing_path, merged):
            log.info("Actualizado %s: %s puntos", isin, len(merged))
            any_changed = True
        else:
            log.info("Sin cambios en %s", isin)

        # Actualizar metadatos
        f_meta = meta.setdefault("funds", {}).setdefault(isin, {})
        for key, val in [
            ("fturl", fund.fturl),
            ("fundsquareurl", fund.fundsquareurl),
            ("arivaurl", ariva_url),
            ("cobasurl", cobas_url),
            ("yahoourl", yahoo_url),
            ("genericurl", generic_url),
            ("genericselector", generic_selector),
            ("genericselectorfecha", generic_selector_fecha),  # ← NUEVO
        ]:
            if val and f_meta.get(key) != val:
                f_meta[key] = val
                any_changed = True

        ft_name = ft_meta.get("name") if isinstance(ft_meta, dict) else None
        ft_curr = ft_meta.get("currency") if isinstance(ft_meta, dict) else None
        yf_curr = yf_meta.get("currency") if isinstance(yf_meta, dict) else None
        yf_sym = yf_meta.get("yahoo_symbol") if isinstance(yf_meta, dict) else None

        for key, val in [
            ("name", ft_name or (yf_meta.get("name") if isinstance(yf_meta, dict) else None)),
            ("currency", ft_curr or yf_curr),
            ("yahoo_symbol", yf_sym),
        ]:
            if val and f_meta.get(key) != val:
                f_meta[key] = val
                any_changed = True

    if save_metadata_if_changed(meta):
        any_changed = True

    log.info("Proceso finalizado. Cambios detectados: %s", any_changed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
