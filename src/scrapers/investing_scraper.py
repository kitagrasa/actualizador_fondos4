from __future__ import annotations

import logging
import re
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from ..utils import parse_float

log = logging.getLogger("scrapers.investing")

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

# Señales fuertes de challenge (best-effort)
_BLOCK_MARKERS = [
    "/cdn-cgi/challenge-platform",
    "cf-turnstile",
    "challenges.cloudflare.com",
    "cf_chl_",
    "verify you are human",
    "attention required",
    "captcha",
]


def _looks_blocked(html: str) -> bool:
    if not html:
        return True
    low = html.lower()
    return any(m in low for m in _BLOCK_MARKERS)


def _fetch_html(session, investing_url: str) -> Optional[str]:
    domain = urlparse(investing_url).netloc or "www.investing.com"
    headers = {
        "User-Agent": _UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Referer": f"https://{domain}/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        r = session.get(investing_url, headers=headers, timeout=30, allow_redirects=True)
        if r.status_code != 200:
            log.warning("Investing HTML: status=%s url=%s", r.status_code, investing_url)
            return None

        html = r.text or ""
        if len(html) < 1500:
            log.warning("Investing HTML: demasiado corto (len=%s) url=%s", len(html), investing_url)
            return None

        # No “castigamos” si hay markers pero viene la tabla. Solo warning si parece challenge Y no hay currtable.
        has_currtable_hint = ('id="currtable"' in html) or ("id=currtable" in html)
        if _looks_blocked(html) and not has_currtable_hint:
            log.warning(
                "Investing HTML: parece bloqueado/challenge url=%s (len=%s)",
                investing_url, len(html),
            )

        return html
    except Exception as e:
        log.error("Investing HTML error url=%s: %s", investing_url, e)
        return None


# ── Extraer pair_id / sml_id desde el HTML (para cache / AJAX) ───────────────
# IMPORTANTE: regex SIN doble-escape; el HTML real incluye:
#   window.histDataExcessInfo pairId 1036800, smlId 25737662
# [file:299]

_RE_HISTINFO = re.compile(
    r"histDataExcessInfo.*?pairId\s*(?:[:=]\s*)?(?P<pair>\d{3,10}).*?smlId\s*(?:[:=]\s*)?(?P<sml>\d{3,12})",
    re.I | re.S,
)
_RE_PAIRID_GENERIC = re.compile(r"\bpairId\b\s*(?:[:=]\s*)?(\d{3,10})", re.I)
_RE_DATA_PAIR_ID = re.compile(r"\bdata-pair-id\b\s*=\s*['\"]?(\d{3,10})", re.I)


def _extract_
