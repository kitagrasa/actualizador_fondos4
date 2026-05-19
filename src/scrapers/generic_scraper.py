"""
Scraper polivalente: extrae el precio (NAV/cotización) de cualquier web
usando un selector CSS proporcionado por el usuario.
Equivale a la función "Tabla Web" de Portfolio Performance pero automático.

Estrategias en cascada (de menor a mayor coste):
  1. requests estático  → rápido, sin overhead
  2. curl_cffi (TLS fingerprint real de Chrome) → sortea detección anti-bot básica
  3. Playwright headless (opcional) → para webs que requieren JavaScript

Retorna: List[Tuple[str, float]]  →  [(fecha_ISO, precio)]
Si no puede extraer nada, retorna [] sin lanzar excepciones.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date, datetime
from typing import List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ── Cabeceras anti-bot ────────────────────────────────────────────────────────
_EXTRA_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
}

# Cookie de consentimiento genérico: compatible con CookieBot y la mayoría
# de banners GDPR estándar (evita que el banner tape el contenido).
_COOKIE_CONSENT = (
    "cookieconsent_status=allow; "
    "CookieConsent={stamp:'-1',necessary:true,preferences:true,statistics:true,marketing:true}; "
    "gdpr_consent=1; "
    "cookies_accepted=1; "
    "cookie_consent=accepted"
)

# Timeouts en segundos
_TIMEOUT_CONNECT = 10
_TIMEOUT_READ = 25

# Formatos de fecha que se intentan autodetectar (de más a menos específico)
_DATE_FORMATS = [
    "%Y-%m-%d",        # 2026-04-30
    "%d/%m/%Y",        # 30/04/2026
    "%d-%m-%Y",        # 30-04-2026
    "%d.%m.%Y",        # 30.04.2026
    "%d/%m/%y",        # 30/04/26
    "%d-%m-%y",        # 30-04-26
    "%B %d, %Y",       # April 30, 2026  (inglés)
    "%b %d, %Y",       # Apr 30, 2026    (inglés abreviado)
    "%d de %B de %Y",  # 30 de abril de 2026  (español largo)
    "%d %B %Y",        # 30 abril 2026
]


# ─────────────────────────────────────────────────────────────────────────────
# Funciones privadas de extracción
# ─────────────────────────────────────────────────────────────────────────────

def _extract_price_from_text(text: str) -> Optional[float]:
    """
    Convierte un texto de precio a float soportando formatos europeo y anglosajón.
    Ejemplos:
        "176,540000 €"  →  176.54
        "1.234,56"      →  1234.56
        "1,234.56"      →  1234.56
        "176.54"        →  176.54
    """
    if not text:
        return None

    # Eliminar monedas, símbolos y espacios
    cleaned = re.sub(r"[€$£%\s\xa0]", "", text).strip()
    # Dejar solo dígitos, coma y punto
    cleaned = re.sub(r"[^\d,.]", "", cleaned)

    if not cleaned:
        return None

    try:
        if "," in cleaned and "." in cleaned:
            # Si la coma va DESPUÉS del último punto → formato europeo: 1.234,56
            if cleaned.rfind(",") > cleaned.rfind("."):
                cleaned = cleaned.replace(".", "").replace(",", ".")
            else:
                # Formato anglosajón: 1,234.56
                cleaned = cleaned.replace(",", "")
        elif "," in cleaned:
            # Solo coma → decimal europeo: 176,54
            cleaned = cleaned.replace(",", ".")
        # Solo punto o ninguno → ya es formato estándar

        price = float(cleaned)

        # Validación de rango: un precio válido es > 0 y < 10 millones
        if price <= 0 or price >= 10_000_000:
            log.debug("Generic: precio %s fuera de rango válido, descartado", price)
            return None

        return price

    except (ValueError, TypeError):
        log.debug("Generic: no se pudo convertir '%s' a float", cleaned)
        return None


def _extract_date_from_text(text: str, date_format: str = "") -> str:
    """
    Intenta extraer una fecha de un texto y la devuelve en formato ISO YYYY-MM-DD.
    Si no puede parsear la fecha, devuelve la fecha de hoy.
    """
    if not text:
        return date.today().isoformat()

    text_stripped = text.strip()

    # Si el usuario proporcionó un formato explícito, usarlo primero
    if date_format:
        try:
            return datetime.strptime(text_stripped, date_format).strftime("%Y-%m-%d")
        except ValueError:
            log.debug("Generic: formato explícito '%s' no coincide con '%s'", date_format, text_stripped)

    # Autodetección: probar los formatos más comunes
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text_stripped, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Si ningún formato funcionó, usar la fecha de hoy como fallback
    log.debug("Generic: no se pudo parsear fecha '%s', usando fecha de hoy", text_stripped)
    return date.today().isoformat()


def _get_element_value(element, attribute: str) -> str:
    """
    Extrae el valor de un elemento BeautifulSoup.
    attribute="text" → texto visible del elemento.
    Otro valor → atributo HTML (ej: "data-value", "content").
    """
    if element is None:
        return ""
    if attribute == "text":
        return element.get_text(strip=True)
    return element.get(attribute, "") or element.get_text(strip=True)


def _parse_html(
    html: str,
    selector: str,
    date_selector: str,
    price_attribute: str,
    date_attribute: str,
    date_format: str,
) -> Optional[Tuple[str, float]]:
    """
    Parsea el HTML ya descargado buscando precio y fecha con selectores CSS.
    Retorna (fecha_ISO, precio) o None si no encuentra los datos.
    """
    soup = BeautifulSoup(html, "lxml")

    # Buscar el elemento de precio
    price_elem = soup.select_one(selector)
    if not price_elem:
        log.debug("Generic: selector '%s' no encontró ningún elemento", selector)
        return None

    raw_price_text = _get_element_value(price_elem, price_attribute)
    log.debug("Generic: texto raw del precio → '%s'", raw_price_text)

    price = _extract_price_from_text(raw_price_text)
    if price is None:
        log.warning("Generic: no se pudo extraer precio del texto '%s'", raw_price_text)
        return None

    # Buscar el elemento de fecha (si se proporcionó selector)
    fecha_iso = date.today().isoformat()
    if date_selector:
        date_elem = soup.select_one(date_selector)
        if date_elem:
            raw_date_text = _get_element_value(date_elem, date_attribute)
            log.debug("Generic: texto raw de la fecha → '%s'", raw_date_text)
            fecha_iso = _extract_date_from_text(raw_date_text, date_format)
        else:
            log.debug("Generic: selector de fecha '%s' no encontró elemento, usando hoy", date_selector)

    log.info("Generic: extraído precio=%.6f fecha=%s", price, fecha_iso)
    return (fecha_iso, price)


# ─────────────────────────────────────────────────────────────────────────────
# Estrategias de descarga
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_with_requests(session: requests.Session, url: str) -> Optional[str]:
    """
    Estrategia 1: descarga estática con requests.
    Usa la sesión del proyecto (ya tiene reintentos y headers base).
    """
    headers = {**_EXTRA_HEADERS, "Cookie": _COOKIE_CONSENT}
    backoffs = [1.0, 2.0, 4.0]

    for intento, espera in enumerate(backoffs, start=1):
        try:
            log.debug("Generic [requests] intento %d → %s", intento, url)
            resp = session.get(
                url,
                headers=headers,
                timeout=(_TIMEOUT_CONNECT, _TIMEOUT_READ),
            )

            if resp.status_code == 403:
                log.warning("Generic [requests]: 403 bloqueado, pasando a siguiente estrategia")
                return None  # No reintentar

            if resp.status_code == 429:
                log.warning("Generic [requests]: 429 rate limit, esperando 10s")
                time.sleep(10)
                continue

            if resp.status_code == 200:
                return resp.text

            log.warning("Generic [requests]: status %s", resp.status_code)

        except Exception as e:
            log.warning("Generic [requests] intento %d error: %s", intento, e)

        if intento < len(backoffs):
            time.sleep(espera)

    return None


def _fetch_with_curl_cffi(url: str) -> Optional[str]:
    """
    Estrategia 2: curl_cffi con TLS fingerprint real de Chrome.
    Sortea la mayoría de sistemas anti-bot basados en TLS/JA3.
    curl_cffi ya está en requirements.txt del proyecto.
    """
    try:
        from curl_cffi import requests as cffi_requests  # type: ignore
    except ImportError:
        log.debug("Generic: curl_cffi no disponible, saltando estrategia 2")
        return None

    backoffs = [1.0, 2.0, 4.0]

    for intento, espera in enumerate(backoffs, start=1):
        try:
            log.debug("Generic [curl_cffi] intento %d → %s", intento, url)
            with cffi_requests.Session(impersonate="chrome120") as s:
                resp = s.get(
                    url,
                    headers={**_EXTRA_HEADERS, "Cookie": _COOKIE_CONSENT},
                    timeout=_TIMEOUT_READ,
                )

            if resp.status_code == 403:
                log.warning("Generic [curl_cffi]: 403 bloqueado, pasando a siguiente estrategia")
                return None

            if resp.status_code == 429:
                log.warning("Generic [curl_cffi]: 429 rate limit, esperando 10s")
                time.sleep(10)
                continue

            if resp.status_code == 200:
                return resp.text

        except Exception as e:
            log.warning("Generic [curl_cffi] intento %d error: %s", intento, e)

        if intento < len(backoffs):
            time.sleep(espera)

    return None


def _fetch_with_playwright(url: str) -> Optional[str]:
    """
    Estrategia 3 (opcional): Playwright headless con Chromium.
    Para webs que requieren JavaScript para mostrar el precio.
    Para activar: pip install playwright && playwright install chromium
    """
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except ImportError:
        log.debug("Generic: playwright no instalado, saltando estrategia 3")
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                locale="es-ES",
                extra_http_headers=_EXTRA_HEADERS,
            )

            # Añadir cookies de consentimiento antes de navegar
            dominio = url.split("/")[2]
            context.add_cookies([
                {"name": "cookieconsent_status", "value": "allow",   "domain": dominio, "path": "/"},
                {"name": "cookies_accepted",     "value": "1",       "domain": dominio, "path": "/"},
                {"name": "gdpr_consent",         "value": "1",       "domain": dominio, "path": "/"},
            ])

            page = context.new_page()
            page.goto(url, wait_until="networkidle", timeout=30_000)

            # Intentar cerrar el banner de cookies si aparece
            _selectores_cookies = [
                "#accept-cookies",
                ".cookie-accept",
                "#onetrust-accept-btn-handler",
                ".cc-accept",
                "[id*='accept'][id*='cookie']",
                "[class*='accept'][class*='cookie']",
                "button[aria-label*='accept' i]",
                "button[aria-label*='aceptar' i]",
                "button[aria-label*='acepto' i]",
            ]
            for sel in _selectores_cookies:
                try:
                    btn = page.query_selector(sel)
                    if btn and btn.is_visible():
                        btn.click()
                        page.wait_for_timeout(500)
                        log.debug("Generic [playwright]: banner cerrado con '%s'", sel)
                        break
                except Exception:
                    continue

            html = page.content()
            browser.close()
            return html

    except Exception as e:
        log.error("Generic [playwright] error: %s", e)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Función pública principal
# ─────────────────────────────────────────────────────────────────────────────

def scrape_generic_prices(
    session: requests.Session,
    url: str,
    selector: str,
    date_selector: str = "",
    price_attribute: str = "text",
    date_attribute: str = "text",
    date_format: str = "",
    use_playwright: bool = False,
) -> List[Tuple[str, float]]:
    """
    Extrae el precio de cualquier URL usando un selector CSS.

    Parámetros:
        session:          Sesión requests del proyecto (con reintentos).
        url:              URL de la página donde está el precio.
        selector:         Selector CSS del elemento con el precio.
                          Ej: "div.each-data p.number", ".nav-value", "#price span"
        date_selector:    (Opcional) Selector CSS del elemento con la fecha.
                          Si está vacío, se usa la fecha de hoy.
        price_attribute:  Atributo HTML a leer del elemento de precio.
                          "text" = texto visible (por defecto).
                          Otros: "data-value", "value", "content", etc.
        date_attribute:   Igual que price_attribute pero para la fecha.
        date_format:      Formato strptime de la fecha (ej: "%d/%m/%Y").
                          Si está vacío, se autodetecta.
        use_playwright:   Si True, usa Playwright como último recurso.
                          Requiere: pip install playwright && playwright install chromium

    Retorna:
        Lista con una tupla [(fecha_ISO, precio)] o [] si falla todo.
    """
    if not url or not url.startswith("http"):
        log.debug("Generic: URL vacía o inválida, omitiendo")
        return []

    if not selector or not selector.strip():
        log.warning("Generic: selector CSS vacío para %s, omitiendo", url)
        return []

    log.info("Generic: iniciando extracción de %s con selector '%s'", url, selector)

    html: Optional[str] = None

    # ── Estrategia 1: requests estático ──────────────────────────────────────
    html = _fetch_with_requests(session, url)
    if html:
        resultado = _parse_html(html, selector, date_selector, price_attribute, date_attribute, date_format)
        if resultado:
            log.info("Generic [requests]: éxito → %s = %.6f", resultado[0], resultado[1])
            return [resultado]
        log.debug("Generic [requests]: HTML descargado pero selector sin datos")

    # ── Estrategia 2: curl_cffi (TLS fingerprint Chrome) ─────────────────────
    log.info("Generic: intentando estrategia 2 (curl_cffi) para %s", url)
    html = _fetch_with_curl_cffi(url)
    if html:
        resultado = _parse_html(html, selector, date_selector, price_attribute, date_attribute, date_format)
        if resultado:
            log.info("Generic [curl_cffi]: éxito → %s = %.6f", resultado[0], resultado[1])
            return [resultado]
        log.debug("Generic [curl_cffi]: HTML descargado pero selector sin datos")

    # ── Estrategia 3: Playwright headless (solo si use_playwright=True) ───────
    if use_playwright:
        log.info("Generic: intentando estrategia 3 (playwright) para %s", url)
        html = _fetch_with_playwright(url)
        if html:
            resultado = _parse_html(html, selector, date_selector, price_attribute, date_attribute, date_format)
            if resultado:
                log.info("Generic [playwright]: éxito → %s = %.6f", resultado[0], resultado[1])
                return [resultado]
            log.warning("Generic [playwright]: HTML obtenido pero selector sin datos")

    log.warning("Generic: todas las estrategias fallaron para %s", url)
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Tests (ejecutar con: python -m src.scrapers.generic_scraper)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(message)s")
    errores = 0

    print("\n=== Test 1: Normalización de precios ===")
    casos_precio = [
        ("176,540000 €", 176.54),
        ("1.234,56",     1234.56),
        ("1,234.56",     1234.56),
        ("176.54",       176.54),
        ("  32,76 €  ",  32.76),
        ("0,0001",       0.0001),
    ]
    for texto, esperado in casos_precio:
        resultado = _extract_price_from_text(texto)
        ok = resultado is not None and abs(resultado - esperado) < 0.001
        print(f"  {'✓' if ok else '✗'} '{texto}' → {resultado} (esperado {esperado})")
        if not ok:
            errores += 1

    print("\n=== Test 2: Autodetección de fechas ===")
    casos_fecha = [
        ("30/04/2026", "",          "2026-04-30"),
        ("30-04-2026", "",          "2026-04-30"),
        ("2026-04-30", "",          "2026-04-30"),
        ("30.04.2026", "",          "2026-04-30"),
        ("April 30, 2026", "",      "2026-04-30"),
        ("30/04/2026", "%d/%m/%Y",  "2026-04-30"),
    ]
    for texto, fmt, esperado in casos_fecha:
        resultado = _extract_date_from_text(texto, fmt)
        ok = resultado == esperado
        print(f"  {'✓' if ok else '✗'} '{texto}' → {resultado} (esperado {esperado})")
        if not ok:
            errores += 1

    print("\n=== Test 3: URL inválida devuelve [] ===")
    s = requests.Session()
    r = scrape_generic_prices(s, "https://url-inexistente-12345.xyz", "div.precio")
    ok = r == []
    print(f"  {'✓' if ok else '✗'} URL inválida → {r}")
    if not ok:
        errores += 1

    print(f"\n{'='*40}")
    print(f"{'✓ TODOS LOS TESTS PASARON' if errores == 0 else f'✗ {errores} TESTS FALLARON'}")
    sys.exit(errores)
