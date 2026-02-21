from __future__ import annotations

import json
import logging
import random
import re
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from ..utils import parse_float

log = logging.getLogger("scrapers.investing")

# Endpoint TVC (TradingView-compatible) — no está bloqueado por Cloudflare
# api.investing.com usa TLS fingerprinting y siempre devuelve 403 con requests estándar
_TVC_URL = "https://tvc{n}.investing.com/{rand}/{now}/{from_ts}/{to_ts}/{id}/history"


def _find_key(obj, key: str):
    """Búsqueda recursiva de una clave en dict/list anidado."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = _find_key(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _find_key(item, key)
            if r is not None:
                return r
    return None


def _get_instrument_id(session, investing_url: str) -> Optional[str]:
    """Extrae instrument_id de la página histórica de investing.com."""
    try:
        domain = urlparse(investing_url).netloc or "www.investing.com"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": f"https://{domain}/",
        }
        r = session.get(investing_url, headers=headers, timeout=30)
        if r.status_code != 200:
            log.warning("Investing: status=%s url=%s", r.status_code, investing_url)
            return None

        # 1) Buscar en __NEXT_DATA__ (Next.js)
        soup = BeautifulSoup(r.text, "lxml")
        tag = soup.find("script", id="__NEXT_DATA__")
        if tag and tag.string:
            try:
                data = json.loads(tag.string)
                for key in ("instrument_id", "instrumentId", "pair_id"):
                    val = _find_key(data, key)
                    if val:
                        log.debug("Investing: instrument_id=%s (desde __NEXT_DATA__)", val)
                        return str(val)
            except Exception as e:
                log.debug("Investing: Error parseando __NEXT_DATA__: %s", e)

        # 2) Fallback: regex en HTML
        for pattern in (
            r'"instrument_id"\s*:\s*"?(\d+)"?',
            r'"pair_id"\s*:\s*"?(\d+)"?',
            r'data-pair-id="(\d+)"',
        ):
            m = re.search(pattern, r.text)
            if m:
                log.debug("Investing: instrument_id=%s (desde regex)", m.group(1))
                return m.group(1)

        log.warning("Investing: No se encontró instrument_id en %s", investing_url)
        return None

    except Exception as e:
        log.error("Investing: Error obteniendo instrument_id de %s: %s", investing_url, e, exc_info=True)
        return None


def _fetch_tvc(session, instrument_id: str, from_ts: int, to_ts: int) -> Optional[List[Tuple[str, float]]]:
    """
    Llama al endpoint TVC (tvc1..tvc8.investing.com) que devuelve JSON TradingView.
    No está protegido por Cloudflare a diferencia de api.investing.com.
    """
    n = random.randint(1, 8)
    rand = random.randint(100000, 999999)
    now = int(time.time())
    url = _TVC_URL.format(n=n, rand=rand, now=now, from_ts=from_ts, to_ts=to_ts, id=instrument_id)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Referer": "https://www.investing.com/",
        "Origin": "https://www.investing.com",
    }

    try:
        r = session.get(url, headers=headers, timeout=30)
        log.debug("Investing TVC: status=%s id=%s from=%s to=%s", r.status_code, instrument_id, from_ts, to_ts)

        if r.status_code != 200:
            log.warning("Investing TVC: status=%s instrument_id=%s", r.status_code, instrument_id)
            return None

        payload = r.json()

        # Formato TradingView: {"s": "ok", "t": [...timestamps], "c": [...closes]}
        if payload.get("s") != "ok":
            log.warning("Investing TVC: status=%r instrument_id=%s", payload.get("s"), instrument_id)
            return None

        timestamps = payload.get("t", [])
        closes = payload.get("c", [])

        if not timestamps or not closes or len(timestamps) != len(closes):
            log.warning("Investing TVC: Datos incompletos. id=%s", instrument_id)
            return None

        out: List[Tuple[str, float]] = []
        for ts, c in zip(timestamps, closes):
            try:
                d = datetime.utcfromtimestamp(int(ts)).date().isoformat()
                out.append((d, float(c)))
            except Exception:
                continue

        return sorted(out)

    except Exception as e:
        log.warning("Investing TVC: Error en petición: %s", e)
        return None


def scrape_investing_prices(
    session,
    investing_url: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> List[Tuple[str, float]]:
    """
    Acepta la URL de la página histórica de investing.com.
    Usa endpoint TVC (no Cloudflare) para obtener datos. Devuelve [(YYYY-MM-DD, close)].
    """
    if not investing_url:
        log.debug("Investing: URL vacía, se omite.")
        return []

    instrument_id = _get_instrument_id(session, investing_url)
    if not instrument_id:
        return []

    end = end_date or date.today()
    start = date(2000, 1, 1) if full_refresh else (start_date or (end - timedelta(days=45)))

    from_ts = int(datetime(start.year, start.month, start.day).timestamp())
    to_ts = int(datetime(end.year, end.month, end.day, 23, 59, 59).timestamp())

    # Intentar hasta 3 servidores TVC distintos
    for attempt in range(3):
        prices = _fetch_tvc(session, instrument_id, from_ts, to_ts)
        if prices is not None:
            log.debug("Investing: %s precios para instrument_id=%s", len(prices), instrument_id)
            return prices
        if attempt < 2:
            time.sleep(0.5)

    log.warning("Investing: No se pudieron obtener precios para %s (instrument_id=%s)", investing_url, instrument_id)
    return []
