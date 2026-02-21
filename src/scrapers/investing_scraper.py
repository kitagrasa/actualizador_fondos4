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

# Señales "fuertes" de challenge (best-effort).
_BLOCK_MARKERS = [
    "/cdn-cgi/challenge-platform",
    "cf-turnstile",
    "challenges.cloudflare.com",
    "cf_chl_",
    "verify you are human",
    "attention required",
    "captcha",
]

# pairId/instrumentId/smlId aparecen de varias formas en el HTML. [file:299]
_RE_HISTINFO = re.compile(
    r"histDataExcessInfo.*?pairId\s*(?:[:=]\s*)?(?P<pair>\d{3,10}).*?smlId\s*(?:[:=]\s*)?(?P<sml>\d{3,12})",
    re.I | re.S,
)
_RE_DATALAYER_INSTRUMENT = re.compile(r"dataLayer\.push\(\s*['\"]instrumentid(?P<id>\d{3,10})['\"]\s*\)", re.I)
_RE_PAIRID_GENERIC = re.compile(r"\bpairId\b\s*(?:[:=]\s*)?(?P<id>\d{3,10})", re.I)
_RE_DATA_PAIR_ID = re.compile(r"\bdata-pair-id\b\s*=\s*['\"]?(?P<id>\d{3,10})", re.I)
_RE_PID_LAST = re.compile(r"\bpid-(?P<id>\d{3,10})-last\b", re.I)


def _looks_blocked(html: str) -> bool:
    if not html:
        return True
    low = html.lower()
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


def _fetch_html(session, investing_url: str) -> Optional[str]:
    domain = urlparse(investing_url).netloc or "es.investing.com"
    headers = _build_headers(
        domain=domain,
        referer=f"https://{domain}/",
        accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    )
    try:
        r = session.get(investing_url, headers=headers, timeout=30, allow_redirects=True)
        if r.status_code != 200:
            log.warning("Investing HTML: status=%s url=%s", r.status_code, investing_url)
            return None

        html = r.text or ""
        if len(html) < 1500:
            log.warning("Investing HTML: demasiado corto (len=%s) url=%s", len(html), investing_url)
            return None

        # No “bloquees” el flujo solo por markers; a veces la página trae datos igualmente.
        if _looks_blocked(html):
            log.warning(
                "Investing HTML: parece bloqueado/challenge url=%s (len=%s)",
                investing_url,
                len(html),
            )

        return html
    except Exception as e:
        log.error("Investing HTML error url=%s: %s", investing_url, e, exc_info=True)
        return None


def _extract_pair_and_sml(html: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve (pair_id, sml_id) si se pueden extraer del HTML. [file:299]
    """
    if not html:
        return None, None

    m = _RE_HISTINFO.search(html)
    if m:
        return m.group("pair"), m.group("sml")

    m = _RE_DATALAYER_INSTRUMENT.search(html)
    if m:
        return m.group("id"), None

    m = _RE_DATA_PAIR_ID.search(html)
    if m:
        return m.group("id"), None

    m = _RE_PID_LAST.search(html)
    if m:
        return m.group("id"), None

    m = _RE_PAIRID_GENERIC.search(html)
    if m:
        return m.group("id"), None

    return None, None


def _parse_investing_number(text: str) -> float:
    """
    Convierte:
      - "88,280" -> 88.280
      - "1.234,56" -> 1234.56
      - "1234.56" -> 1234.56
    """
    s = (text or "").strip().replace("\xa0", "").replace(" ", "")
    if not s:
        raise ValueError("Número vacío")

    if "," in s and "." in s:
        # Formato EU: puntos miles, coma decimal
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")

    # Dejar solo lo "numérico" al principio si viene algo extra
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
    if not m:
        raise ValueError(f"No se pudo parsear número: {text!r}")
    return float(m.group(0))


def _date_from_td(td) -> Optional[str]:
    """
    Preferimos epoch en data-real-value cuando exista. [file:299]
    """
    if td is None:
        return None

    epoch = (td.get("data-real-value") or "").strip()
    if epoch.isdigit():
        try:
            return datetime.fromtimestamp(int(epoch), tz=timezone.utc).date().isoformat()
        except Exception:
            pass

    s = td.get_text(" ", strip=True)
    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d", "%b %d, %Y", "%d %b, %Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    return None


def _parse_prices_from_table(table) -> List[Tuple[str, float]]:
    """
    Tabla “histórica” típica: Date | Last | Open | High | Low | Change%.
    Nos quedamos con Date y Last (close).
    """
    out: List[Tuple[str, float]] = []
    for tr in table.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        d_iso = _date_from_td(tds[0])
        if not d_iso:
            continue

        raw_last = tds[1].get("data-real-value") or tds[1].get_text(" ", strip=True)
        try:
            close = _parse_investing_number(str(raw_last))
        except Exception:
            continue

        out.append((d_iso, close))

    out.sort(key=lambda x: x[0])
    return out


def _parse_html_currtable(html: str) -> List[Tuple[str, float]]:
    """
    Fallback: parsea lo que haya renderizado en la propia página (suele ser “lo visible”).
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")

    # Selectores típicos observados en Investing: #currtable y/o .historicalTbl. [file:299]
    table = (
        soup.select_one("table#currtable")
        or soup.select_one("table.historicalTbl#currtable")
        or soup.select_one("table.historicalTbl")
    )
    if not table:
        return []

    return _parse_prices_from_table(table)


def _post_historical_data_ajax(
    session,
    investing_url: str,
    pair_id: str,
    sml_id: Optional[str],
    st: date,
    en: date,
) -> Optional[str]:
    """
    Llama al endpoint interno /instruments/HistoricalDataAjax (mismo dominio).
    Devuelve HTML (a veces viene directo, a veces envuelto en JSON en 'data').
    """
    parsed = urlparse(investing_url)
    domain = parsed.netloc or "es.investing.com"

    ajax_url = f"https://{domain}/instruments/HistoricalDataAjax"
    headers = _build_headers(
        domain=domain,
        referer=investing_url,
        accept="application/json, text/javascript, */*; q=0.01",
    )
    headers.update(
        {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
        }
    )

    # Formato de fechas típico del POST de Investing (mm/dd/YYYY).
    data = {
        "curr_id": str(pair_id),
        "st_date": st.strftime("%m/%d/%Y"),
        "end_date": en.strftime("%m/%d/%Y"),
        "interval_sec": "Daily",
        "sort_col": "date",
        "sort_ord": "DESC",
        "action": "historical_data",
    }
    if sml_id and sml_id.isdigit():
        # No siempre es obligatorio, pero si existe en el HTML, ayuda en algunos instrumentos.
        data["smlID"] = sml_id
        data["smlId"] = sml_id

    try:
        r = session.post(ajax_url, data=data, headers=headers, timeout=30)
        if r.status_code != 200:
            log.warning("Investing AJAX: status=%s pair_id=%s url=%s", r.status_code, pair_id, investing_url)
            return None

        text = r.text or ""
        if not text.strip():
            return None

        # A veces devuelve JSON con clave data, a veces HTML directo.
        t = text.lstrip()
        if t.startswith("{"):
            try:
                payload = json.loads(text)
                if isinstance(payload, dict):
                    frag = payload.get("data")
                    if isinstance(frag, str) and frag.strip():
                        return frag
            except Exception:
                # Si no es JSON válido, caerá a HTML directo
                pass

        return text
    except Exception as e:
        log.debug("Investing AJAX error pair_id=%s: %s", pair_id, e)
        return None


def _parse_ajax_fragment(fragment: str) -> List[Tuple[str, float]]:
    """
    El fragmento suele contener <table ...>...</table> o solo <tr>...</tr>.
    """
    if not fragment:
        return []

    soup = BeautifulSoup(fragment, "lxml")

    table = soup.select_one("table")
    if table:
        return _parse_prices_from_table(table)

    # Si viene solo como filas <tr>, lo envolvemos.
    soup2 = BeautifulSoup(f"<table><tbody>{fragment}</tbody></table>", "lxml")
    table2 = soup2.select_one("table")
    if not table2:
        return []
    return _parse_prices_from_table(table2)


def scrape_investing_prices(
    session,
    investing_url: str,
    startdate: Optional[date] = None,
    enddate: Optional[date] = None,
    fullrefresh: bool = False,
) -> List[Tuple[str, float]]:
    """
    Devuelve lista [(YYYY-MM-DD, close)].

    Estrategia:
      1) GET HTML (para cookies + extraer pair_id).
      2) HistoricalDataAjax en rangos (esto permite traer TODO el histórico).
      3) Fallback final a parsear #currtable (solo si AJAX falla).
    """
    if not investing_url:
        return []

    html = _fetch_html(session, investing_url)
    if not html:
        return []

    pair_id, sml_id = _extract_pair_and_sml(html)
    if not pair_id:
        log.warning("Investing: no se pudo extraer pair_id de %s", investing_url)
        # Sin pair_id no hay AJAX; devolvemos lo que se pueda de la tabla visible.
        rows = _parse_html_currtable(html)
        if not rows:
            log.warning("Investing: tabla HTML vacía/no encontrada para %s", investing_url)
        return rows

    end = enddate or date.today()

    # Si no nos dan startdate o pedimos fullrefresh, hacemos barrido hacia atrás hasta el inicio real.
    if fullrefresh or startdate is None:
        # Seguridad: límite duro de iteraciones para no colgarnos si cambian el endpoint.
        chunk_days = 365 * 5  # 5 años por request
        max_loops = 200

        collected: Dict[str, float] = {}
        found_any = False
        empty_streak = 0

        cur_end = end
        min_start = date(1900, 1, 1)

        for _ in range(max_loops):
            if cur_end < min_start:
                break

            cur_start = max(min_start, cur_end - timedelta(days=chunk_days - 1))
            frag = _post_historical_data_ajax(session, investing_url, pair_id, sml_id, cur_start, cur_end)
            rows = _parse_ajax_fragment(frag) if frag else []

            if rows:
                found_any = True
                empty_streak = 0
                for d_iso, close in rows:
                    collected[d_iso] = close
            else:
                empty_streak += 1

            # Si ya encontramos datos y encadenamos vacíos, es muy probable que hayamos pasado el inception.
            if found_any and empty_streak >= 3:
                break

            cur_end = cur_start - timedelta(days=1)
            time.sleep(0.15)

        if collected:
            out = sorted(collected.items(), key=lambda x: x[0])
            log.debug("Investing: histórico completo %s puntos (pair_id=%s)", len(out), pair_id)
            return out

        # Si AJAX no dio nada, última opción: tabla visible
        rows = _parse_html_currtable(html)
        if not rows:
            log.warning("Investing: sin datos vía AJAX ni tabla HTML para %s (pair_id=%s)", investing_url, pair_id)
        return rows

    # Incremental: pedir SOLO el rango necesario
    st = startdate
    frag = _post_historical_data_ajax(session, investing_url, pair_id, sml_id, st, end)
    rows = _parse_ajax_fragment(frag) if frag else []
    if rows:
        return rows

    # Fallback: tabla visible
    rows = _parse_html_currtable(html)
    if not rows:
        log.warning("Investing: tabla HTML vacía/no encontrada para %s", investing_url)
    return rows
