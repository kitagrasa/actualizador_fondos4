from __future__ import annotations

import json
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

# ── Regex extracción symbol ──────────────────────────────────────────────────

# 1) Ruta TVC expuesta en JS (no siempre presente)
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

# 2) window.histDataExcessInfo = {pairId: 1036800, ...}
_RE_HISTDATA = re.compile(
    r'histDataExcessInfo\s*=\s*\{[^}]*?pairId\s*:\s*(?P<id>\d+)',
    re.IGNORECASE | re.DOTALL,
)

# 3) instrument_id en dataLayer / __NEXT_DATA__
_RE_INSTRUMENT_ID = re.compile(
    r'instrument[_-]?id["\']?\s*:\s*["\']?(?P<id>\d+)',
    re.IGNORECASE,
)

# 4) pair_id / pairid en allKeyValue u objeto JS
_RE_ALLKV_PAIRID = re.compile(
    r'"?pair_?id"?\s*:\s*"?(?P<id>\d+)"?',
    re.IGNORECASE,
)

# 5) data-pair-id="..." en DOM
_RE_DATA_PAIR_ID = re.compile(r'\bdata-pair-id\s*=\s*"?(?P<id>\d+)"?', re.IGNORECASE)


# ── Fetch HTML (curl_cffi → requests fallback) ───────────────────────────────

def _fetch_html_curl(url: str) -> Optional[str]:
    """
    Usa curl_cffi impersonando Chrome para bypassear Cloudflare.
    curl_cffi replica el TLS/JA3 fingerprint de un browser real.
    """
    try:
        from curl_cffi.requests import Session as CurlSession  # type: ignore
        with CurlSession(impersonate="chrome120") as s:
            r = s.get(
                url,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                    "Referer": "https://es.investing.com/",
                },
                timeout=30,
            )
            if r.status_code != 200:
                log.warning("Investing curl_cffi: status=%s url=%s", r.status_code, url)
                return None
            html = r.text or ""
            log.debug("Investing: HTML vía curl_cffi (%s chars)", len(html))
            return html
    except ImportError:
        log.debug("Investing: curl_cffi no instalado, usando requests")
        return None
    except Exception as e:
        log.debug("Investing: curl_cffi error: %s", e)
        return None


def _fetch_html_requests(session, url: str) -> Optional[str]:
    """Fallback con requests normal."""
    domain = urlparse(url).netloc or "www.investing.com"
    headers = {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"https://{domain}/",
        "Connection": "keep-alive",
    }
    try:
        r = session.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            log.warning("Investing requests: status=%s url=%s", r.status_code, url)
            return None
        return r.text or ""
    except Exception as e:
        log.error("Investing requests error url=%s: %s", url, e, exc_info=True)
        return None


def _fetch_html(session, url: str) -> Optional[str]:
    """
    Intenta curl_cffi primero (bypassea Cloudflare).
    Si no está disponible o falla, cae a requests.
    Valida que el HTML recibido sea real (>5000 chars y contenga investing).
    """
    html = _fetch_html_curl(url)
    if html and len(html) > 5000 and "investing" in html.lower():
        return html

    log.debug("Investing: curl_cffi no dio HTML válido, probando requests")
    html = _fetch_html_requests(session, url)
    if html and len(html) > 5000:
        return html

    log.warning("Investing: HTML vacío o bloqueado (Cloudflare?) para %s", url)
    return None


# ── Extracción de symbol ─────────────────────────────────────────────────────

def _extract_symbol_nextdata(html: str) -> Optional[str]:
    """
    Páginas Next.js: el instrument_id está en
    <script id="__NEXT_DATA__" type="application/json">{...}</script>
    """
    try:
        m = re.search(
            r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
            html, re.DOTALL | re.IGNORECASE
        )
        if not m:
            return None
        data = json.loads(m.group(1))
        # Buscar instrument_id en cualquier nivel del JSON
        raw = json.dumps(data)
        mi = _RE_INSTRUMENT_ID.search(raw)
        if mi and mi.group("id").isdigit():
            log.debug("Investing: symbol vía __NEXT_DATA__ = %s", mi.group("id"))
            return mi.group("id")
    except Exception:
        pass
    return None


def _extract_symbol(html: str) -> Optional[str]:
    """
    Extrae pair_id / instrument_id en cascada de 6 patrones.
    Compatible con páginas legacy (jQuery) y Next.js.
    """
    if not html:
        return None

    normalized = html.replace("\\/", "/")

    # 1) Ruta TVC en JS
    m = _RE_TVC_PATH.search(normalized)
    if m:
        log.debug("Investing: symbol vía TVC path = %s", m.group("symbol"))
        return m.group("symbol")

    # 2) __NEXT_DATA__ (páginas Next.js)
    sym = _extract_symbol_nextdata(html)
    if sym:
        return sym

    # 3) window.histDataExcessInfo = {pairId: ...}
    m = _RE_HISTDATA.search(html)
    if m:
        log.debug("Investing: symbol vía histDataExcessInfo = %s", m.group("id"))
        return m.group("id")

    # 4) dataLayer.push({instrument_id: ...}) o similar
    m = _RE_INSTRUMENT_ID.search(html)
    if m:
        log.debug("Investing: symbol vía instrument_id = %s", m.group("id"))
        return m.group("id")

    # 5) allKeyValue pairid / pair_id
    m = _RE_ALLKV_PAIRID.search(html)
    if m:
        log.debug("Investing: symbol vía pair_id = %s", m.group("id"))
        return m.group("id")

    # 6) data-pair-id en DOM
    m = _RE_DATA_PAIR_ID.search(html)
    if m:
        log.debug("Investing: symbol vía data-pair-id = %s", m.group("id"))
        return m.group("id")

    log.warning("Investing: ningún patrón encontró symbol. HTML[0:300]: %r", html[:300])
    return None


def _extract_tvc_template(html: str) -> Optional[dict]:
    if not html:
        return None
    m = _RE_TVC_PATH.search(html.replace("\\/", "/"))
    if not m:
        return None
    return {k: m.group(k) for k in ("hash", "ts", "a", "b", "c", "symbol")}


# ── TVC fetch ────────────────────────────────────────────────────────────────

def _unix_ts(d: date, end_of_day: bool = False) -> int:
    if end_of_day:
        return int(datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    return int(datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc).timestamp())


def _parse_eu_number(text: str) -> float:
    s = (text or "").strip().replace("\u00a0", "").replace(" ", "")
    if not s:
        raise ValueError("número vacío")
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    return float(s)


def _build_tvc_url(server_n: int, tpl: Optional[dict]) -> str:
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
    t_list, c_list = payload.get("t"), payload.get("c")
    if not isinstance(t_list, list) or not isinstance(c_list, list) or len(t_list) != len(c_list):
        return []
    out = []
    for ts, close in zip(t_list, c_list):
        try:
            out.append((datetime.utcfromtimestamp(int(ts)).date().isoformat(), float(close)))
        except Exception:
            continue
    out.sort(key=lambda x: x[0])
    return out


def _parse_html_table(html: str) -> List[Tuple[str, float]]:
    """Fallback: tabla #curr_table / .historicalTbl renderizada en el HTML."""
    if not html:
        return []
    try:
        soup = BeautifulSoup(html, "lxml")
        table = (
            soup.select_one("table#curr_table")
            or soup.select_one("table#currtable")
            or soup.select_one("table.historicalTbl")
        )
        if not table:
            return []
        out = []
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
                pass
            if not d_iso:
                try:
                    d_iso = datetime.strptime(tds[0].get_text(strip=True), "%d.%m.%Y").date().isoformat()
                except Exception:
                    continue
            raw = tds[1].get("data-real-value") or tds[1].get_text(strip=True)
            try:
                out.append((d_iso, _parse_eu_number(str(raw))))
            except Exception:
                continue
        out.sort(key=lambda x: x[0])
        log.debug("Investing: tabla HTML → %s filas", len(out))
        return out
    except Exception as e:
        log.debug("Investing: error parseando tabla HTML: %s", e)
        return []


def _tvc_request(
    session, url: str, symbol: str,
    from_ts: int, to_ts: int, referer: str,
) -> List[Tuple[str, float]]:
    headers = {
        "User-Agent": _UA,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Origin": "https://es.investing.com",
        "Referer": referer,
        "Connection": "keep-alive",
    }
    params = {"symbol": symbol, "resolution": "D", "from": str(from_ts), "to": str(to_ts)}
    try:
        r = session.get(url, params=params, headers=headers, timeout=30)
        if r.status_code != 200:
            return []
        return _parse_tvc_json(r.json())
    except Exception:
        return []


def _date_chunks(start: date, end: date, years: int) -> List[Tuple[date, date]]:
    chunks, cur = [], start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=years * 365 - 1))
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    return chunks


# ── Entry point ──────────────────────────────────────────────────────────────

def scrape_investing_prices(
    session,
    investing_url: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> List[Tuple[str, float]]:
    """
    1. HTML via curl_cffi (bypassea Cloudflare) → requests fallback.
    2. Extrae symbol con 6 patrones en cascada (legacy + Next.js).
    3. TVC por chunks adaptativos (todos los datos si full_refresh).
    4. Fallback a tabla HTML si TVC falla.
    """
    if not investing_url:
        return []

    html = _fetch_html(session, investing_url)
    if not html:
        return []

    symbol = _extract_symbol(html)
    if not symbol:
        log.warning(
            "Investing: no se pudo extraer symbol/instrument_id de %s", investing_url
        )
        return _parse_html_table(html)

    tpl = _extract_tvc_template(html)

    end = end_date or date.today()
    start = date(1970, 1, 1) if full_refresh else (start_date or (end - timedelta(days=45)))

    # ── Elegir servidor TVC operativo ──────────────────────────────────────
    servers = list(range(1, 9))
    random.shuffle(servers)
    tvc_url: Optional[str] = None

    for n in servers[:5]:
        candidate = _build_tvc_url(n, tpl)
        try:
            r = session.get(
                candidate,
                params={"symbol": symbol, "resolution": "D", "from": "0", "to": "1"},
                headers={"User-Agent": _UA, "Referer": investing_url},
                timeout=15,
            )
            if r.status_code == 200:
                tvc_url = candidate
                log.debug("Investing: servidor tvc%s OK (symbol=%s)", n, symbol)
                break
        except Exception:
            continue

    if not tvc_url:
        log.warning("Investing: ningún servidor TVC operativo para %s", investing_url)
        return _parse_html_table(html)

    # ── Fetch por chunks con división adaptativa ───────────────────────────
    LIMIT_HINT = 4500
    collected: Dict[str, float] = {}

    def fetch_chunk(s: date, e: date, depth: int = 0) -> None:
        if depth > 6:
            return
        rows = _tvc_request(
            session, tvc_url, symbol,
            _unix_ts(s), _unix_ts(e, end_of_day=True),
            investing_url,
        )
        for d, c in rows:
            collected[d] = c
        if len(rows) >= LIMIT_HINT and (e - s).days > 180:
            mid = s + timedelta(days=(e - s).days // 2)
            fetch_chunk(s, mid, depth + 1)
            fetch_chunk(mid + timedelta(days=1), e, depth + 1)

    base_years = 10 if full_refresh else 2
    for s, e in _date_chunks(start, end, years=base_years):
        fetch_chunk(s, e)
        time.sleep(0.15)

    if collected:
        out = sorted(collected.items(), key=lambda x: x[0])
        log.info("Investing: %s precios (symbol=%s) para %s", len(out), symbol, investing_url)
        return out

    # ── Fallback final ─────────────────────────────────────────────────────
    fallback = _parse_html_table(html)
    if fallback:
        log.warning(
            "Investing: TVC sin datos → tabla HTML (%s filas) para %s",
            len(fallback), investing_url,
        )
    else:
        log.warning("Investing: sin datos TVC ni tabla HTML para %s (symbol=%s)", investing_url, symbol)
    return fallback
