from __future__ import annotations

import logging
import random
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

log = logging.getLogger("scrapers.investing")

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

# Si el HTML expone el path TVC, se ve algo tipo:
# /<hash>/<ts>/<a>/<b>/<c>/history?symbol=<ID>&resolution=...
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

# En esta página se ve instrument_id en dataLayer y pair_id en allKeyValue. [file:249]
_RE_DATALAYER_INSTRUMENT_ID = re.compile(r'"instrument_id"\s*:\s*"?(?P<id>\d+)"?', re.IGNORECASE)
_RE_ALLKEYVALUE_PAIR_ID = re.compile(r'"pair_id"\s*:\s*"?(?P<id>\d+)"?', re.IGNORECASE)

# También aparece window.histDataExcessInfo pairId 1036800 ... [file:249]
_RE_HISTDATA_PAIRID = re.compile(r"histDataExcessInfo\s+pairId\s+(?P<id>\d+)\b", re.IGNORECASE)

# Último recurso: data-pair-id genérico (puede haber muchos en sidebars). [file:249]
_RE_DATA_PAIR_ID = re.compile(r'\bdata-pair-id\s*=\s*"?(?P<id>\d+)"?', re.IGNORECASE)


def _unix_ts(d: date, end_of_day: bool = False) -> int:
    if end_of_day:
        return int(datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    return int(datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc).timestamp())


def _parse_eu_number(text: str) -> float:
    s = (text or "").strip().replace("\u00a0", "")
    if not s:
        raise ValueError("Número vacío")
    # "1.234,56" -> "1234.56"
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    return float(s)


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
    if not html:
        return None
    normalized = html.replace("\\/", "/")
    m = _RE_TVC_PATH.search(normalized)
    if not m:
        return None
    return {
        "hash": m.group("hash"),
        "ts": m.group("ts"),
        "a": m.group("a"),
        "b": m.group("b"),
        "c": m.group("c"),
        "symbol": m.group("symbol"),
    }


def _extract_symbol_from_html(html: str) -> Optional[str]:
    """
    Devuelve el 'symbol' numérico que entiende TVC (instrument_id/pair_id).
    """
    if not html:
        return None

    normalized = html.replace("\\/", "/")

    # 1) Si aparece template TVC, úsalo
    m = _RE_TVC_PATH.search(normalized)
    if m and m.group("symbol").isdigit():
        return m.group("symbol")

    # 2) instrument_id en dataLayer
    m = _RE_DATALAYER_INSTRUMENT_ID.search(html)
    if m and m.group("id").isdigit():
        return m.group("id")

    # 3) pair_id en allKeyValue
    m = _RE_ALLKEYVALUE_PAIR_ID.search(html)
    if m and m.group("id").isdigit():
        return m.group("id")

    # 4) window.histDataExcessInfo pairId ...
    m = _RE_HISTDATA_PAIRID.search(html)
    if m and m.group("id").isdigit():
        return m.group("id")

    # 5) data-pair-id genérico (puede colarse uno que no sea el principal)
    m = _RE_DATA_PAIR_ID.search(html)
    if m and m.group("id").isdigit():
        return m.group("id")

    return None


def _build_tvc_url(server_n: int, tpl: Optional[dict]) -> str:
    """
    https://tvcX.investing.com/<hash>/<ts>/<a>/<b>/<c>/history
    Si Investing no expone el hash/ts, generamos uno compatible.
    """
    tpl = tpl or {}
    h = tpl.get("hash") or "".join(random.choice("0123456789abcdef") for _ in range(32))
    ts = str(tpl.get("ts") or int(time.time()))
    a = str(tpl.get("a") or random.randint(10, 99))
    b = str(tpl.get("b") or random.randint(10, 99))
    c = str(tpl.get("c") or random.randint(10, 99))
    return f"https://tvc{server_n}.investing.com/{h}/{ts}/{a}/{b}/{c}/history"


def _parse_tvc_json(payload) -> List[Tuple[str, float]]:
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

    out.sort(key=lambda x: x[0])
    return out


def _parse_html_table_prices(html: str) -> List[Tuple[str, float]]:
    """
    Fallback: tabla ya renderizada (table#curr_table) con data-real-value. [file:249]
    Nota: solo devuelve lo que venga en el HTML (rango visible).
    """
    if not html:
        return []
    try:
        soup = BeautifulSoup(html, "lxml")
        table = soup.select_one("table#curr_table")
        if not table:
            return []

        out: List[Tuple[str, float]] = []
        for tr in table.select("tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            d_iso: Optional[str] = None
            try:
                epoch = tds[0].get("data-real-value")
                if epoch:
                    d_iso = datetime.fromtimestamp(int(epoch), tz=timezone.utc).date().isoformat()
            except Exception:
                d_iso = None

            if not d_iso:
                txt = tds[0].get_text(strip=True)
                try:
                    d_iso = datetime.strptime(txt, "%d.%m.%Y").date().isoformat()
                except Exception:
                    continue

            raw = tds[1].get("data-real-value") or tds[1].get_text(strip=True)
            try:
                last_ = _parse_eu_number(str(raw))
            except Exception:
                continue

            out.append((d_iso, float(last_)))

        out.sort(key=lambda x: x[0])
        return out
    except Exception:
        return []


def _date_chunks(start: date, end: date, years: int) -> List[Tuple[date, date]]:
    chunks: List[Tuple[date, date]] = []
    cur = start
    while cur <= end:
        # aprox: years * 365 días, suficiente para chunking (no necesitamos exactitud al día)
        nxt = min(end, cur + timedelta(days=years * 365) - timedelta(days=1))
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


def _tvc_fetch_range(
    session,
    tvc_url: str,
    symbol: str,
    from_ts: int,
    to_ts: int,
    headers: dict,
    timeout: int = 30,
) -> List[Tuple[str, float]]:
    params = {"symbol": symbol, "resolution": "D", "from": str(from_ts), "to": str(to_ts)}
    r = session.get(tvc_url, params=params, headers=headers, timeout=timeout)
    if r.status_code != 200:
        return []
    try:
        return _parse_tvc_json(r.json())
    except Exception:
        return []


def scrape_investing_prices(
    session,
    investing_url: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> List[Tuple[str, float]]:
    """
    - full_refresh=True  -> backfill total (desde 1970-01-01 hasta hoy) en chunks adaptativos.
    - full_refresh=False -> incremental: start_date (si viene) o últimos 45 días.
    Devuelve [(YYYY-MM-DD, close)] deduplicado.
    """
    if not investing_url:
        log.debug("Investing: URL vacía, se omite.")
        return []

    html = _fetch_html(session, investing_url)
    if not html:
        return []

    symbol = _extract_symbol_from_html(html)
    if not symbol:
        log.warning("Investing: no se pudo extraer symbol/instrument_id de %s", investing_url)
        return _parse_html_table_prices(html)

    tpl = _extract_tvc_template(html)  # puede ser None, y no pasa nada

    end = end_date or date.today()
    if full_refresh:
        start = date(1970, 1, 1)
    else:
        start = start_date or (end - timedelta(days=45))

    domain = urlparse(investing_url).netloc or "www.investing.com"
    headers = {
        "User-Agent": _UA,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Origin": f"https://{domain}",
        "Referer": investing_url,
        "Connection": "keep-alive",
    }

    servers = [1, 2, 3, 4, 5, 6, 7, 8]
    random.shuffle(servers)

    # 1) Elegimos 1 servidor TVC que responda (para no multiplicar requests)
    tvc_url = None
    for n in servers[:5]:
        candidate = _build_tvc_url(n, tpl)
        test = _tvc_fetch_range(
            session,
            candidate,
            symbol,
            _unix_ts(max(start, end - timedelta(days=7))),
            _unix_ts(end, end_of_day=True),
            headers,
        )
        if test or True:
            # Si responde 200 con JSON vacío también puede ser válido (por ejemplo, fondo sin datos recientes);
            # así que hacemos un ping real de status:
            try:
                r = session.get(candidate, params={"symbol": symbol, "resolution": "D", "from": "0", "to": "1"}, headers=headers, timeout=15)
                if r.status_code == 200:
                    tvc_url = candidate
                    break
            except Exception:
                continue

    if not tvc_url:
        log.warning("Investing: no hay servidor TVC operativo para %s (symbol=%s)", investing_url, symbol)
        return _parse_html_table_prices(html)

    # 2) Backfill/Incremental por chunks, con división adaptativa si el chunk viene “demasiado lleno”
    # Heurística: si una respuesta trae >= 4500 puntos, probablemente hay límite y conviene dividir.
    LIMIT_HINT = 4500

    collected: Dict[str, float] = {}

    def fetch_chunk(s: date, e: date, depth: int = 0) -> None:
        if depth > 6:
            return
        from_ts = _unix_ts(s, end_of_day=False)
        to_ts = _unix_ts(e, end_of_day=True)
        rows = _tvc_fetch_range(session, tvc_url, symbol, from_ts, to_ts, headers)
        if not rows:
            return
        for d, c in rows:
            collected[d] = c
        if len(rows) >= LIMIT_HINT and (e - s).days > 120:
            mid = s + timedelta(days=(e - s).days // 2)
            fetch_chunk(s, mid, depth + 1)
            fetch_chunk(mid + timedelta(days=1), e, depth + 1)

    # Chunks base: más grandes en full refresh (optimiza número de requests)
    base_years = 10 if full_refresh else 2
    for s, e in _date_chunks(start, end, years=base_years):
        fetch_chunk(s, e)
        time.sleep(0.15)

    if collected:
        out = sorted(collected.items(), key=lambda x: x[0])
        log.debug("Investing: %s precios (symbol=%s) vía TVC", len(out), symbol)
        return out

    # 3) Último fallback (solo lo que venga en HTML)
    fallback = _parse_html_table_prices(html)
    if fallback:
        log.warning("Investing: TVC sin datos; devolviendo tabla HTML (%s filas) para %s", len(fallback), investing_url)
    else:
        log.warning("Investing: sin datos TVC y sin tabla HTML para %s", investing_url)
    return fallback
