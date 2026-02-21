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

# ── Regex para extraer pair_id / instrument_id ─────────────────────────────
# Patrón 1: window.histDataExcessInfo = {pairId: 1036800, ...}   [file:268]
_RE_HISTDATA = re.compile(
    r'histDataExcessInfo\s*=\s*\{[^}]*?pairId\s*:\s*(?P<id>\d+)',
    re.IGNORECASE | re.DOTALL,
)

# Patrón 2: dataLayer.push({instrument_id: 1036800})  [file:268]
_RE_INSTRUMENT_ID = re.compile(
    r'instrument[_-]?id["\']?\s*:\s*["\']?(?P<id>\d+)',
    re.IGNORECASE,
)

# Patrón 3: var allKeyValue = {..., "pair_id":"1036800", ...}  o  pairid:1036800  [file:268]
_RE_ALLKV_PAIRID = re.compile(
    r'"?pair_?id"?\s*:\s*"?(?P<id>\d+)"?',
    re.IGNORECASE,
)

# Patrón 4: Ruta TVC expuesta en JS (no siempre presente)
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

# Patrón 5: data-pair-id="1036800" en DOM (genérico, último recurso)
_RE_DATA_PAIR_ID = re.compile(r'\bdata-pair-id\s*=\s*"?(?P<id>\d+)"?', re.IGNORECASE)


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


def _prewarm_session(session) -> None:
    """
    Visita la portada de es.investing.com para obtener cookies de sesión y GDPR.
    Sin esto, muchas URLs internas devuelven HTML incompleto o redirigen. [file:268]
    """
    try:
        session.get(
            "https://es.investing.com",
            headers={
                "User-Agent": _UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            },
            timeout=20,
        )
        # Cookie de consentimiento GDPR mínima que Investing.com espera
        session.cookies.set("GDPR_CONSENT", "1", domain=".investing.com")
        session.cookies.set("PHPSESSID", "prewarm", domain=".investing.com")
    except Exception:
        pass


_SESSION_PREWARMED: set = set()


def _fetch_html(session, investing_url: str) -> Optional[str]:
    domain = urlparse(investing_url).netloc or "www.investing.com"

    # Pre-calentar la sesión una sola vez por dominio [file:268]
    if domain not in _SESSION_PREWARMED:
        _prewarm_session(session)
        _SESSION_PREWARMED.add(domain)
        time.sleep(0.5)

    headers = {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": f"https://{domain}/funds",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
    }
    try:
        r = session.get(investing_url, headers=headers, timeout=30)
        if r.status_code != 200:
            log.warning("Investing HTML: status=%s url=%s", r.status_code, investing_url)
            return None
        html = r.text or ""
        # DEBUG: muestra inicio del HTML para diagnosticar bloqueos/redirects
        log.debug("Investing HTML: primeros 300 chars: %r", html[:300])
        return html
    except Exception as e:
        log.error("Investing HTML error url=%s: %s", investing_url, e, exc_info=True)
        return None


def _extract_symbol(html: str) -> Optional[str]:
    """
    Extrae el pair_id / instrument_id numérico del HTML.
    Orden: TVC path > histDataExcessInfo > instrument_id > allKeyValue pair_id > data-pair-id
    """
    if not html:
        return None

    normalized = html.replace("\\/", "/")

    # 1) Ruta TVC completa (lo más específico)
    m = _RE_TVC_PATH.search(normalized)
    if m:
        return m.group("symbol")

    # 2) window.histDataExcessInfo = {pairId: 1036800, ...}  [file:268]
    m = _RE_HISTDATA.search(html)
    if m:
        log.debug("Investing: symbol vía histDataExcessInfo = %s", m.group("id"))
        return m.group("id")

    # 3) dataLayer.push({instrument_id: 1036800})  [file:268]
    m = _RE_INSTRUMENT_ID.search(html)
    if m:
        log.debug("Investing: symbol vía instrument_id = %s", m.group("id"))
        return m.group("id")

    # 4) allKeyValue pair_id  [file:268]
    m = _RE_ALLKV_PAIRID.search(html)
    if m:
        log.debug("Investing: symbol vía allKeyValue pair_id = %s", m.group("id"))
        return m.group("id")

    # 5) data-pair-id en DOM (puede aparecer en sidebars con IDs ajenos)
    m = _RE_DATA_PAIR_ID.search(html)
    if m:
        log.debug("Investing: symbol vía data-pair-id = %s", m.group("id"))
        return m.group("id")

    log.debug("Investing: ningún patrón matcheó. HTML sample: %r", html[:500])
    return None


def _extract_tvc_template(html: str) -> Optional[dict]:
    """Extrae el template TVC si está presente en el HTML (no siempre)."""
    if not html:
        return None
    m = _RE_TVC_PATH.search(html.replace("\\/", "/"))
    if not m:
        return None
    return {k: m.group(k) for k in ("hash", "ts", "a", "b", "c", "symbol")}


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
    """
    Fallback: tabla renderizada en el HTML.
    Acepta id='curr_table', id='currtable', o class='historicalTbl'. [file:268]
    """
    if not html:
        return []
    try:
        soup = BeautifulSoup(html, "lxml")
        # Cubrir ambas variantes del ID y el selector por clase  [file:268]
        table = (
            soup.select_one("table#curr_table")
            or soup.select_one("table#currtable")
            or soup.select_one("table.historicalTbl")
        )
        if not table:
            log.debug("Investing: table#curr_table / .historicalTbl no encontrada en HTML")
            return []

        out = []
        for tr in table.select("tbody tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            # Fecha: epoch en data-real-value  [file:268]
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
            # Precio: data-real-value del td "Último"  [file:268]
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


def _tvc_request(session, url: str, symbol: str, from_ts: int, to_ts: int, referer: str) -> List[Tuple[str, float]]:
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


def scrape_investing_prices(
    session,
    investing_url: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    full_refresh: bool = False,
) -> List[Tuple[str, float]]:
    """
    Estrategia:
      1. Descarga HTML (con pre-warmup de sesión/cookies).
      2. Extrae symbol (pair_id) con 5 patrones en cascada.
      3. Llama a TVC por chunks (todos los datos si full_refresh).
      4. Si TVC falla → tabla HTML (rango visible por defecto).
    """
    if not investing_url:
        return []

    html = _fetch_html(session, investing_url)
    if not html:
        return []

    symbol = _extract_symbol(html)
    if not symbol:
        log.warning("Investing: no se pudo extraer symbol/instrument_id de %s", investing_url)
        # Aun así intentamos la tabla HTML antes de rendirse
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
                log.debug("Investing: usando tvc%s (symbol=%s)", n, symbol)
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
        # Si viene "lleno" puede haber un límite: dividir y repetir
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
        log.debug("Investing: %s precios totales (symbol=%s)", len(out), symbol)
        return out

    # ── Fallback final: tabla HTML ─────────────────────────────────────────
    fallback = _parse_html_table(html)
    if fallback:
        log.warning(
            "Investing: TVC sin datos → fallback tabla HTML (%s filas) para %s",
            len(fallback), investing_url,
        )
    else:
        log.warning("Investing: sin datos TVC ni tabla HTML para %s (symbol=%s)", investing_url, symbol)
    return fallback
