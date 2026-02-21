from __future__ import annotations

import logging
import random
import re
import time
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

log = logging.getLogger("scrapers.investing")

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

# En el HTML/JS de Investing aparece una ruta tipo:
#   /<hash>/<ts>/<a>/<b>/<c>/history?symbol=<ID>&resolution=D&from=...&to=...
# (a veces viene con \\/ escapados). [file:249]
_RE_TVC_PATH = re.compile(
    r"/(?P<hash>[a-f0-9]{32})/"
    r"(?P<ts>\d{9,11})/"
    r"(?P<a>\d{1,3})/"
    r"(?P<b>\d{1,3})/"
    r"(?P<c>\d{1,3})/"
    r"history\?symbol=(?P<symbol>\d+)"
    r"(?:&|$)",
    re.IGNORECASE,
)

# Fallback: detectar pair-id principal en DOM (hay varios en “Cotizaciones recientes”). [file:249]
_RE_PAIRID_FALLBACK = re.compile(r'\bdata-pair-id\s*=\s*"?(?P<id>\d+)"?', re.IGNORECASE)


def _unix_ts(d: date, end_of_day: bool = False) -> int:
    if end_of_day:
        return int(datetime(d.year, d.month, d.day, 23, 59, 59).timestamp())
    return int(datetime(d.year, d.month, d.day, 0, 0, 0).timestamp())


def _fetch_html(session, investing_url: str) -> Optional[str]:
    domain = urlparse(investing_url).netloc or "www.investing.com"
    headers = {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"https://{domain}/",
        "Connection": "keep-alive",
    }
    try:
        r = session.get(investing_url, headers=headers, timeout=30)
        if r.status_code != 200:
            log.warning("Investing HTML: status=%s url=%s", r.status_code, investing_url)
            return None
        return r.text or ""
    except Exception as e:
        log.error("Investing HTML error url=%s: %s", investing_url, e, exc_info=True)
        return None


def _extract_tvc_template(html: str) -> Optional[dict]:
    """
    Extrae {hash, ts, a, b, c, symbol} a partir del HTML/JS de la página de históricos.
    Esto es lo más robusto porque usamos la misma plantilla que usa la web. [file:249]
    """
    if not html:
        return None

    # Normalizar \"\\/\" → "/" para poder aplicar regex sin duplicar patrones
    normalized = html.replace("\\/", "/")

    m = _RE_TVC_PATH.search(normalized)
    if m:
        return {
            "hash": m.group("hash"),
            "ts": m.group("ts"),
            "a": m.group("a"),
            "b": m.group("b"),
            "c": m.group("c"),
            "symbol": m.group("symbol"),
        }

    # Fallback: intentar sacar pair-id “principal” desde instrumentHead (si no hubiera plantilla TVC)
    # OJO: este fallback no siempre funciona si Investing no expone la ruta TVC en el HTML. [file:249]
    try:
        soup = BeautifulSoup(html, "lxml")
        node = soup.select_one("div.instrumentHead [data-pair-id]")
        if node and node.get("data-pair-id"):
            pid = str(node.get("data-pair-id")).strip()
            if pid.isdigit():
                return {
                    "hash": None,
                    "ts": str(int(time.time())),
                    "a": "56",
                    "b": "56",
                    "c": "23",
                    "symbol": pid,
                }
    except Exception:
        pass

    m2 = _RE_PAIRID_FALLBACK.search(html)
    if m2:
        pid = m2.group("id")
        return {
            "hash": None,
            "ts": str(int(time.time())),
            "a": "56",
            "b": "56",
            "c": "23",
            "symbol": pid,
        }

    return None


def _build_tvc_url(server_n: int, tpl: dict) -> str:
    """
    Construye URL TVC:
      https://tvc6.investing.com/<hash>/<ts>/<a>/<b>/<c>/history
    El 'hash' puede ser cualquiera de 32 hex; si no lo tenemos, generamos uno. [web:256]
    """
    h = tpl.get("hash")
    if not h:
        h = "".join(random.choice("0123456789abcdef") for _ in range(32))
    ts = tpl["ts"]
    a, b, c = tpl["a"], tpl["b"], tpl["c"]
    return f"https://tvc{server_n}.investing.com/{h}/{ts}/{a}/{b}/{c}/history"


def _parse_tvc_json(payload) -> List[Tuple[str, float]]:
    """
    TradingView-like:
      {"s":"ok","t":[...],"c":[...], ...}
    [file:249]
    """
    if not isinstance(payload, dict) or payload.get("s") != "ok":
        return []

    t_list = payload.get("t")
    c_list = payload.get("c")
    if not isinstance(t_list, list) or not isinstance(c_list, list) or len(t_list) != len(c_list):
        return []

    out: List[Tuple[str, float]] = []
    for ts, close in zip(t_list, c_list):
        try:
            d = datetime.utcfromtimestamp(int(ts)).date().isoformat()
            out.append((d, float(close)))
        except Exception:
            continue

    # Orden ascendente
    out.sort(key=lambda x: x[0])
    return out


def scrape_investing_prices(
    session,
    investing_url: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> List[Tuple[str, float]]:
    """
    Scraper Investing 100% práctico para tu repo:
    - Entra a la URL “historical-data” de Investing.
    - Extrae la plantilla TVC /history?symbol=... desde el HTML/JS.
    - Llama a tvc*.investing.com y parsea {"t":[...],"c":[...]}.

    Devuelve [(YYYY-MM-DD, close)].
    """
    if not investing_url:
        log.debug("Investing: URL vacía, se omite.")
        return []

    html = _fetch_html(session, investing_url)
    if not html:
        return []

    tpl = _extract_tvc_template(html)
    if not tpl or not tpl.get("symbol"):
        log.warning("Investing: no se pudo extraer plantilla TVC ni symbol de %s", investing_url)
        return []

    end = end_date or date.today()
    if full_refresh:
        start = date(2000, 1, 1)
    else:
        start = start_date or (end - timedelta(days=45))

    from_ts = _unix_ts(start, end_of_day=False)
    to_ts = _unix_ts(end, end_of_day=True)

    # Query params (los mismos conceptos que aparecen en la propia ruta de la web). [file:249]
    params = {
        "symbol": tpl["symbol"],
        "resolution": "D",
        "from": str(from_ts),
        "to": str(to_ts),
    }

    headers = {
        "User-Agent": _UA,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Origin": "https://www.investing.com",
        "Referer": investing_url,
        "Connection": "keep-alive",
    }

    # Probar varios servidores TVC (algunos días uno falla y otro funciona). [web:256]
    servers = [6, 4, 5, 7, 8, 3, 2, 1]
    random.shuffle(servers)

    for attempt, n in enumerate(servers[:4], start=1):
        url = _build_tvc_url(n, tpl)
        try:
            r = session.get(url, params=params, headers=headers, timeout=30)
            log.debug("Investing TVC: attempt=%s status=%s tvc=%s symbol=%s", attempt, r.status_code, n, tpl["symbol"])
            if r.status_code != 200:
                continue

            payload = r.json()
            prices = _parse_tvc_json(payload)
            if prices:
                log.debug("Investing: %s precios (symbol=%s)", len(prices), tpl["symbol"])
                return prices

        except Exception as e:
            log.debug("Investing TVC: error attempt=%s tvc=%s: %s", attempt, n, e)

        time.sleep(0.25)

    log.warning("Investing: no se pudieron obtener precios vía TVC para %s (symbol=%s)", investing_url, tpl["symbol"])
    return []
