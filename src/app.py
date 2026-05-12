"""
Orquestador principal: descarga precios de múltiples fuentes (FT, Fundsquare, Ariva, Yahoo, Cobas),
fusiona los datos según prioridad (Ariva → Fundsquare → FT → Yahoo → Cobas) y guarda el histórico.
Incluye limpieza de fondos eliminados, gestión de metadatos y soporte para refresh completo/incremental.
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
from .scrapers.yahoo_finance_scraper import scrape_yahoo_finance_prices
from .scrapers.cobas_scraper import scrape_cobas_prices

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PRICES_DIR = DATA_DIR / "prices"
META_FILE = DATA_DIR / "funds_metadata.json"

log = logging.getLogger("app")


# ── Metadatos (nombres, divisas, URLs) ─────────────────────────────────────────

def load_metadata() -> Dict:
    """Carga funds_metadata.json. Si no existe o está corrupto, retorna estructura vacía."""
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


# ── Limpieza de fondos eliminados ─────────────────────────────────────────────

def cleanup_removed_funds(active_isins: List[str], meta: Dict) -> bool:
    """
    Borra archivos JSON de precios y entradas en metadatos correspondientes a ISINs
    que ya no figuran en la configuración activa.
    """
    changed = False
    active_set = set(active_isins)
    PRICES_DIR.mkdir(parents=True, exist_ok=True)

    # Eliminar archivos de precios huérfanos
    for p in PRICES_DIR.glob("*.json"):
        isin = p.stem.strip()
        if isin and isin not in active_set:
            log.info("Eliminando histórico de fondo eliminado: %s", isin)
            try:
                p.unlink()
                changed = True
            except Exception as e:
                log.error("No se pudo borrar %s: %s", p, e)

    # Eliminar entradas de metadatos
    funds_meta = meta.get("funds", {})
    for isin in list(funds_meta.keys()):
        if isin not in active_set:
            log.info("Eliminando metadata de fondo eliminado: %s", isin)
            funds_meta.pop(isin, None)
            changed = True
    meta["funds"] = funds_meta
    return changed


# ── Fusión de fuentes con prioridades ─────────────────────────────────────────

def merge_updates(
    existing: Dict[str, float],
    *sources: List[Tuple[str, float]],
) -> Dict[str, float]:
    """
    Fusiona el histórico existente con los datos de nuevas fuentes.
    Cada fuente sobrescribe fechas anteriores. La prioridad es el orden de los argumentos:
    el último en la lista tiene máxima prioridad (Cobas > Yahoo > FT > Fundsquare > Ariva).
    """
    result = dict(existing)  # copia
    for source in sources:
        if not source:
            continue
        for date_str, price in source:
            result[date_str] = price
    return result


def max_existing_date(existing: Dict[str, float]) -> Optional[date]:
    """Retorna la fecha más reciente presente en el histórico actual (None si vacío)."""
    if not existing:
        return None
    try:
        return max(datetime.strptime(d, "%Y-%m-%d").date() for d in existing.keys())
    except Exception:
        return None


# ── Función principal ─────────────────────────────────────────────────────────

def main() -> int:
    setup_logging()
    log.info("Inicio de actualización de precios")

    # Validación estricta del secreto FUNDS_CSV_URL
    funds_csv_url = os.environ.get("FUNDS_CSV_URL", "").strip()
    if not funds_csv_url.startswith(("http://", "https://")):
        log.error("CRÍTICO: La variable FUNDS_CSV_URL no es una URL válida. Abortando.")
        return 1

    funds = load_funds_csv(funds_csv_url)
    if not funds:
        log.error("CRÍTICO: No se cargaron fondos desde el CSV. Verifica la URL y el formato.")
        return 1

    session = build_session()
    meta = load_metadata()
    any_changed = False

    # Limpiar fondos que ya no están en el CSV
    if cleanup_removed_funds([f.isin for f in funds], meta):
        any_changed = True

    fullrefresh = os.getenv("FULLREFRESH", "0").strip() == "1"
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "14"))
    today = date.today()

    for fund in funds:
        isin = fund.isin
        ariva_url = fund.ariva_url or None
        yahoo_url = fund.yahoo_url or None
        cobas_url = fund.cobas_url or None

        log.info("Procesando %s | FT=%s | FS=%s | ARIVA=%s | COBAS=%s | YAHOO=%s",
                 isin,
                 fund.ft_url or "—",
                 fund.fundsquare_url or "—",
                 ariva_url or "—",
                 cobas_url or "—",
                 yahoo_url or "—")

        existing_path = PRICES_DIR / f"{isin}.json"
        existing = read_prices_json(existing_path)
        last_date = max_existing_date(existing)

        # Determinar rango de fechas para scrapers incrementales
        do_full = fullrefresh or not existing
        start_date = (
            max(date(2000, 1, 1), last_date - timedelta(days=lookback_days))
            if (not do_full and last_date)
            else None
        )

        # 1. Yahoo Finance: siempre pide 10 años (red de seguridad)
        yf_prices, yf_meta = [], {}
        if yahoo_url:
            yf_prices, yf_meta = scrape_yahoo_finance_prices(
                session,
                yahoo_url,
                startdate=None,      # Sin recorte inferior (obtiene desde inicio)
                enddate=today,
                full_refresh=True,   # Fuerza 10 años de datos
            )

        # 2. Financial Times (incremental o completo)
        ft_prices, ft_meta = scrape_ft_prices(
            session,
            fund.ft_url,
            startdate=start_date,
            enddate=today,
            fullrefresh=do_full,
        )

        # 3. Fundsquare
        fs_prices = scrape_fundsquare_prices(session, fund.fundsquare_url)

        # 4. Ariva
        ariva_tuples = []
        if ariva_url:
            ariva_result = scrape_ariva_prices(ariva_url)
            if isinstance(ariva_result, tuple) and len(ariva_result) >= 1:
                raw_ariva = ariva_result[0]
                if raw_ariva:
                    # Normalizar a lista de tuplas (fecha, precio)
                    if isinstance(raw_ariva[0], dict):
                        ariva_tuples = [(p["date"], p["close"]) for p in raw_ariva if "date" in p and "close" in p]
                    else:
                        ariva_tuples = raw_ariva

        # 5. Cobas AM (último valor liquidativo)
        cobas_prices = scrape_cobas_prices(session, cobas_url)

        # Fusión con prioridad: Ariva (menor) → Fundsquare → FT → Yahoo → Cobas (mayor)
        merged = merge_updates(
            existing,
            ariva_tuples,
            fs_prices,
            ft_prices,
            yf_prices,
            cobas_prices,
        )

        # Guardar si hubo cambios
        if write_prices_json_if_changed(existing_path, merged):
            log.info("Actualizado %s → %s puntos", isin, len(merged))
            any_changed = True
        else:
            log.info("Sin cambios en %s", isin)

        # Actualizar metadatos del fondo (URLs, nombre, divisa, símbolo Yahoo)
        fmeta = meta.setdefault("funds", {}).setdefault(isin, {})
        for key, val in [
            ("ft_url", fund.ft_url),
            ("fundsquare_url", fund.fundsquare_url),
            ("ariva_url", ariva_url),
            ("cobas_url", cobas_url),
            ("yahoo_url", yahoo_url),
        ]:
            if val and fmeta.get(key) != val:
                fmeta[key] = val
                any_changed = True

        # Extraer nombre y divisa desde FT o Yahoo
        ft_name = ft_meta.get("name") if isinstance(ft_meta, dict) else None
        ft_curr = ft_meta.get("currency") if isinstance(ft_meta, dict) else None
        yf_curr = yf_meta.get("currency") if isinstance(yf_meta, dict) else None
        yf_sym = yf_meta.get("yahoosymbol") if isinstance(yf_meta, dict) else None

        for key, val in [
            ("name", ft_name or yf_meta.get("name")),
            ("currency", ft_curr or yf_curr),
            ("yahoo_symbol", yf_sym),
        ]:
            if val and fmeta.get(key) != val:
                fmeta[key] = val
                any_changed = True

    # Guardar metadatos globales si se modificaron
    if save_metadata_if_changed(meta):
        any_changed = True

    log.info("Proceso finalizado. Cambios detectados: %s", any_changed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
