from __future__ import annotations

import json
import logging
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

_BLOCK_MARKERS = [
    "/cdn-cgi/challenge-platform",
    "cf-turnstile",
    "challenges.cloudflare.com",
    "verify you are human",
    "attention required",
    "captcha",
]

_RE_HISTINFO  = re.compile(
    r"histDataExcessInfo.*?pairId\s*[=:]\s*(?P<pair>\d{3,10}).*?smlId\s*[=:]\s*(?P<sml>\d{3,12})",
    re.I | re.S,
)
_RE_PAIRID    = re.compile(r"\bpairId\b\s*[=:,]\s*(?P<id>\d{3,10})", re.I)
_RE_DATA_PAIR = re.compile(r'data-pair-id[=\s"\']+(?P<id>\d{3,10})', re.I)


def _blocked(html: str) -> bool:
    low = (html or "").lower()
    return any(m in low for m in _BLOCK_MARKERS)


def _build_headers(domain: str, referer: str, accept: str) -> Dict[str, str]:
    return {
        "User-Agent": _UA,
        "Accept": accept,
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Origin": f"https://{domain}",
        "Referer": referer,
        "Connection": "keep-alive",
    }


def _fetch_html(session, url: str) -> Optional[str]:
    domain = urlparse(url).netloc or "es.investing.com"
    try:
        r = session.get(url, headers=_build_headers(domain, f"https://{domain}/", "text/html,*/*;q=0.8"), timeout=25, allow_redirects=True)
        if r.status_code != 200:
            log.warning("Investing HTML: status=%s url=%s", r.status_code, url)
            return None
        html = r.text or ""
        if len(html) < 1500:
            log.warning("Investing HTML: demasiado corto (len=%s)", len(html))
            return None
        return html
    except Exception as e:
        log.error("Investing HTML error url=%s: %s", url, e)
        return None


def _extract_pair_sml(html: str) -> Tuple[Optional[str], Optional[str]]:
    m = _RE_HISTINFO.search(html)
    if m:
        return m.group("pair"), m.group("sml")
    m = _RE_PAIRID.search(html)
    if m:
        return m.group("id"), None
    m = _RE_DATA_PAIR.search(html)
    if m:
        return m.group("id"), None
    return None, None


def _parse_num(text: str) -> float:
    s = (text or "").strip().replace("\xa0", "").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
    if not m:
        raise ValueError(f"No se pudo parsear: {text!r}")
    return float(m.group(0))


def _date_from_td(td) -> Optional[str]:
    if td is None:
        return None
    epoch = (td.get("data-real-value") or "").strip()
    if epoch.isdigit():
        try:
            return datetime.fromtimestamp(int(epoch), tz=timezone.utc).date().isoformat()
        except Exception:
            pass
    s = td.get_text(" ", strip=True)
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d", "%b %d, %Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    return None


def _parse_table(table) -> List[Tuple[str, float]]:
    out: List[Tuple[str, float]] = []
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        d = _date_from_td(tds[0])
        if not d:
            continue
        raw = tds[1].get("data-real-value") or tds[1].get_text(" ", strip=True)
        try:
            out.append((d, _parse_num(str(raw))))
        except Exception:
            continue
    out.sort(key=lambda x: x[0])
    return out


def _parse_html_table(html: str) -> List[Tuple[str, float]]:
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    table = (
        soup.select_one("table#currtable")
        or soup.select_one("table.historicalTbl")
    )
    return _parse_table(table) if table else []


def _post_ajax(session, investing_url: str, pair_id: str, sml_id: Optional[str], st: date, en: date) -> Optional[str]:
    parsed = urlparse(investing_url)
    domain = parsed.netloc or "es.investing.com"
    ajax_url = f"https://{domain}/instruments/HistoricalDataAjax"
    headers = _build_headers(domain, investing_url, "application/json, */*; q=0.01")
    headers.update({
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    })
    data: Dict = {
        "curr_id":     str(pair_id),
        "st_date":     st.strftime("%m/%d/%Y"),
        "end_date":    en.strftime("%m/%d/%Y"),
        "interval_sec": "Daily",
        "sort_col":    "date",
        "sort_ord":    "DESC",
        "action":      "historical_data",
    }
    if sml_id and sml_id.isdigit():
        data["smlID"] = sml_id
    try:
        r = session.post(ajax_url, data=data, headers=headers, timeout=25)
        if r.status_code != 200:
            log.debug("Investing AJAX: status=%s", r.status_code)
            return None
        text = (r.text or "").strip()
        if not text:
            return None
        if text.startswith("{"):
            try:
                payload = json.loads(text)
                frag = payload.get("data") if isinstance(payload, dict) else None
                if isinstance(frag, str) and frag.strip():
                    return frag
            except Exception:
                pass
        return text
    except Exception as e:
        log.debug("Investing AJAX error: %s", e)
        return None


def _parse_fragment(fragment: str) -> List[Tuple[str, float]]:
    if not fragment:
        return []
    soup = BeautifulSoup(fragment, "lxml")
    table = soup.select_one("table")
    if table:
        return _parse_table(table)
    soup2 = BeautifulSoup(f"<table><tbody>{fragment}</tbody></table>", "lxml")
    t2 = soup2.select_one("table")
    return _parse_table(t2) if t2 else []


def scrape_investing_prices(
    session,
    investing_url: str,
    cached_pair_id: Optional[str] = None,
    startdate: Optional[date] = None,
    enddate: Optional[date] = None,
    fullrefresh: bool = False,
) -> Tuple[List[Tuple[str, float]], Optional[str]]:
    """
    Devuelve ([(YYYY-MM-DD, close)], pair_id_obtenido).
    pair_id_obtenido se guarda en metadata para no volver a hacer GET HTML.
    """
    if not investing_url:
        return [], None

    pair_id = cached_pair_id
    sml_id: Optional[str] = None

    # Solo hacemos GET HTML si no tenemos pair_id en caché
    if not pair_id:
        html = _fetch_html(session, investing_url)
        if not html:
            return [], None

        # Abort inmediato si está bloqueado Y no hay tabla visible
        if _blocked(html):
            log.warning("Investing: bloqueado, intentando tabla HTML inline url=%s", investing_url)
            rows = _parse_html_table(html)
            return rows, None

        pair_id, sml_id = _extract_pair_sml(html)
        if not pair_id:
            log.warning("Investing: no se pudo extraer pair_id de %s", investing_url)
            rows = _parse_html_table(html)
            return rows, None
    else:
        html = None  # no necesitamos el HTML

    end = enddate or date.today()

    # ── INCREMENTAL (modo normal, cada día) ──────────────────────────────────
    if not fullrefresh:
        st = startdate or (end - timedelta(days=45))
        frag = _post_ajax(session, investing_url, pair_id, sml_id, st, end)
        rows = _parse_fragment(frag) if frag else []
        if rows:
            return rows, pair_id
        # fallback tabla inline (solo si tenemos html)
        if html:
            return _parse_html_table(html), pair_id
        return [], pair_id

    # ── FULL REFRESH (backfill completo) ────────────────────────────────────
    # Límite real: máx 20 chunks × 3 años = 60 años de histórico, más que suficiente
    CHUNK_DAYS = 365 * 3
    MAX_CHUNKS = 20

    collected: Dict[str, float] = {}
    found_any = False
    empty_streak = 0
    cur_end = end
    min_start = date(1970, 1, 1)

    for _ in range(MAX_CHUNKS):
        if cur_end < min_start:
            break
        cur_start = max(min_start, cur_end - timedelta(days=CHUNK_DAYS - 1))
        frag = _post_ajax(session, investing_url, pair_id, sml_id, cur_start, cur_end)
        rows = _parse_fragment(frag) if frag else []

        if rows:
            found_any = True
            empty_streak = 0
            for d_iso, close in rows:
                collected[d_iso] = close
        else:
            empty_streak += 1

        # 2 chunks vacíos consecutivos tras haber encontrado datos = llegamos al inicio
        if found_any and empty_streak >= 2:
            break

        cur_end = cur_start - timedelta(days=1)
        time.sleep(0.2)

    if collected:
        out = sorted(collected.items(), key=lambda x: x[0])
        log.info("Investing: %s puntos totales (pair_id=%s)", len(out), pair_id)
        return out, pair_id

    # fallback final
    if html:
        return _parse_html_table(html), pair_id
    return [], pair_id
