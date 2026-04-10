from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

log = logging.getLogger("scrapers.yahoofinance")

def _extract_symbol(yahoo_url: str) -> Optional[str]:
    if not yahoo_url:
        return None

    u = urlparse(yahoo_url.strip())
    qs = parse_qs(u.query)

    for key in ("symbols", "symbol", "s", "p"):
        vals = qs.get(key)
        if vals and vals[0].strip():
            return unquote(vals[0].strip())

    parts = [p for p in u.path.split("/") if p]

    if "quote" in parts:
        i = parts.index("quote")
        if i + 1 < len(parts) and parts[i + 1].strip():
            return unquote(parts[i + 1].strip())

    if "chart" in parts:
        i = parts.index("chart")
        if i + 1 < len(parts) and parts[i + 1].strip():
            return unquote(parts[i + 1].strip())

    return None

def scrape_yahoo_finance_prices(
    session,
    yahoo_url: str,
    startdate: Optional[date] = None,
    enddate: Optional[date] = None,
    full_refresh: bool = False,
) -> Tuple[List[Tuple[str, float]], Dict]:
    if not yahoo_url:
        return [], {}

    symbol = _extract_symbol(yahoo_url)
    meta: Dict = {"yahoo_url": yahoo_url}

    if not symbol:
        log.warning("Yahoo Finance: no se pudo extraer símbolo de %s", yahoo_url)
        return [], meta

    meta["yahoosymbol"] = symbol

    params = {
        "interval": "1d",
        "range": "10y" if full_refresh else "6mo",
        "includeAdjustedClose": "false", # Para fondos suele ser mejor el precio limpio (Close)
        "events": "capitalGain|div|split",
        "formatted": "true",
        "lang": "es-ES",
        "region": "ES",
        "symbol": symbol,
        "userYfid": "true",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Referer": yahoo_url,
    }

    try:
        r = session.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params=params,
            headers=headers,
            timeout=25,
        )

        if r.status_code != 200:
            log.warning("Yahoo Finance API devuleve %s para %s", r.status_code, symbol)
            return [], meta

        payload = r.json()
        chart = payload.get("chart") or {}
        result = (chart.get("result") or [None])[0] or {}

        if not result:
            err = chart.get("error")
            log.warning("Yahoo Finance sin resultados para %s. Error: %s", symbol, err)
            return [], meta

        ym = result.get("meta") or {}
        if ym.get("currency"):
            meta["currency"] = ym["currency"]

        timestamps = result.get("timestamp") or []
        indicators = result.get("indicators") or {}

        # Obtenemos la matriz de precios Close (Cierre normal, no alterado)
        close_prices = ((indicators.get("quote") or [{}])[0].get("close") or [])

        out: List[Tuple[str, float]] = []

        for ts, px in zip(timestamps, close_prices):
            if px is None:
                continue

            d = datetime.fromtimestamp(int(ts), tz=timezone.utc).date()

            if startdate and d < startdate:
                continue
            if enddate and d > enddate:
                continue

            out.append((d.isoformat(), float(px)))

        return sorted(out), meta

    except Exception as e:
        log.error("Error en Yahoo Finance URL=%s Detalle=%s", yahoo_url, e, exc_info=True)
        return [], meta
