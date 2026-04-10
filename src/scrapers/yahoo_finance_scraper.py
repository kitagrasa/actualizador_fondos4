from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

log = logging.getLogger("scrapers.yahoo_finance")

def _extract_symbol(yahoo_url: str) -> Optional[str]:
    if not yahoo_url:
        return None

    u = urlparse(yahoo_url.strip())
    qs = parse_qs(u.query)

    for key in ("symbols", "symbol", "s"):
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
    fullrefresh: bool = False,
) -> Tuple[List[Tuple[str, float]], Dict]:
    
    if not yahoo_url:
        return [], {}

    symbol = _extract_symbol(yahoo_url)
    meta: Dict = {"yahoo_finance_url": yahoo_url}

    if not symbol:
        log.warning("Yahoo Finance: no se pudo extraer símbolo de %s", yahoo_url)
        return [], meta

    meta["yahoo_symbol"] = symbol

    params = {
        "interval": "1d",
        "range": "10y" if fullrefresh else "6mo",
        "includeAdjustedClose": "true",
        "events": "capitalGain|div|split",
        "formatted": "true",
        "lang": "es-ES",
        "region": "ES",
        "symbol": symbol,
        "userYfid": "true",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
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
            log.warning("Yahoo Finance %s devolvió HTTP %s", symbol, r.status_code)
            return [], meta

        payload = r.json()
        chart = payload.get("chart") or {}
        result = (chart.get("result") or [None])[0] or {}

        if not result:
            log.warning("Yahoo Finance sin datos (result vacío) para %s", symbol)
            return [], meta

        ym = result.get("meta") or {}
        if ym.get("currency"):
            meta["currency"] = ym["currency"]
        if ym.get("longName"):
            meta["name"] = ym["longName"]
        elif ym.get("shortName"):
            meta["name"] = ym["shortName"]

        timestamps = result.get("timestamp") or []
        indicators = result.get("indicators") or {}

        # Priorizamos cierre ajustado si existe
        adjclose = ((indicators.get("adjclose") or [{}])[0].get("adjclose") or [])
        close = ((indicators.get("quote") or [{}])[0].get("close") or [])

        series = adjclose if any(v is not None for v in adjclose) else close
        out: List[Tuple[str, float]] = []

        for ts, px in zip(timestamps, series):
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
        log.error("Yahoo Finance error scrapeando %s: %s", yahoo_url, e)
        return [], meta
