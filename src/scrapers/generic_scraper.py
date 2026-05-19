"""
Scraper polivalente - equivalente a la función "tabla web" de Portfolio Performance.
Estrategias en cascada:
  1. requests + BeautifulSoup (estático, rápido)
  2. curl_cffi (bypass TLS fingerprint / anti-bot básico)
  3. Playwright headless (JavaScript obligatorio)
"""

import re
import time
import logging
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ── Meses en español para parseo de fechas ──────────────────────────────────
MESES_ES = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}

HEADERS_BASE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


# ── Utilidades ───────────────────────────────────────────────────────────────

def normalizar_precio(texto: str) -> Optional[float]:
    """
    Convierte precio en cualquier formato europeo/anglosajón a float.
    Ejemplos:
      '363,20 €'    -> 363.20
      '1.234,56 €'  -> 1234.56
      '1,234.56'    -> 1234.56
      '176.54'      -> 176.54
    """
    if not texto:
        return None
    texto = texto.strip()
    # Eliminar símbolos de moneda y espacios
    texto = re.sub(r'[€$£\s%]', '', texto)
    # Determinar formato: si hay coma Y punto, el último separador es el decimal
    if ',' in texto and '.' in texto:
        if texto.rfind(',') > texto.rfind('.'):
            # Formato europeo: 1.234,56
            texto = texto.replace('.', '').replace(',', '.')
        else:
            # Formato anglosajón: 1,234.56
            texto = texto.replace(',', '')
    elif ',' in texto:
        # Solo coma: puede ser separador decimal (363,20) o miles (1,234)
        partes = texto.split(',')
        if len(partes) == 2 and len(partes[1]) <= 4:
            texto = texto.replace(',', '.')
        else:
            texto = texto.replace(',', '')
    try:
        return float(texto)
    except ValueError:
        return None


def normalizar_fecha(texto: str) -> Optional[str]:
    """
    Intenta parsear fechas en múltiples formatos y devuelve 'YYYY-MM-DD'.
    Formatos soportados:
      DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD,
      DD de mes YYYY (español), DD mes YYYY (inglés/español),
      Mon DD, YYYY
    """
    if not texto:
        return None
    texto = texto.strip()

    # Normalizar espacios múltiples
    texto = re.sub(r'\s+', ' ', texto)

    formatos = [
        ("%d/%m/%Y", r"\d{2}/\d{2}/\d{4}"),
        ("%d-%m-%Y", r"\d{2}-\d{2}-\d{4}"),
        ("%Y-%m-%d", r"\d{4}-\d{2}-\d{2}"),
        ("%d.%m.%Y", r"\d{2}\.\d{2}\.\d{4}"),
    ]

    for fmt, pattern in formatos:
        match = re.search(pattern, texto)
        if match:
            try:
                return datetime.strptime(match.group(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

    # Español: "13 de mayo de 2026" o "13 mayo 2026"
    match = re.search(r'(\d{1,2})\s+(?:de\s+)?(\w+)\s+(?:de\s+)?(\d{4})', texto, re.IGNORECASE)
    if match:
        dia, mes_str, anyo = match.groups()
        mes_str = mes_str.lower()
        if mes_str in MESES_ES:
            try:
                return f"{anyo}-{MESES_ES[mes_str]}-{dia.zfill(2)}"
            except Exception:
                pass

    return None


def extraer_con_selector(soup: BeautifulSoup, selector: str) -> Optional[str]:
    """Extrae texto limpio usando un selector CSS."""
    try:
        elem = soup.select_one(selector)
        if elem:
            return elem.get_text(separator=' ', strip=True)
    except Exception:
        pass
    return None


def extraer_con_regex(html: str, patron: str) -> Optional[str]:
    """Extrae el primer grupo capturado de un regex sobre el HTML crudo."""
    try:
        match = re.search(patron, html, re.DOTALL)
        if match:
            return match.group(1).strip()
    except Exception:
        pass
    return None


# ── Estrategia 1: requests estático ─────────────────────────────────────────

def _fetch_static(url: str, extra_headers: dict = None, cookies: dict = None):
    """Descarga el HTML con requests + headers realistas."""
    import requests
    headers = {**HEADERS_BASE}
    if extra_headers:
        headers.update(extra_headers)

    session = requests.Session()

    # Primera visita para obtener cookies del servidor (consent, session, etc.)
    try:
        session.get(url, headers=headers, timeout=15, allow_redirects=True)
    except Exception:
        pass

    if cookies:
        session.cookies.update(cookies)

    resp = session.get(url, headers=headers, timeout=20, allow_redirects=True)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding
    return resp.text


# ── Estrategia 2: curl_cffi (bypass anti-bot TLS) ───────────────────────────

def _fetch_cffi(url: str, extra_headers: dict = None, cookies: dict = None):
    """Descarga con curl_cffi que imita el fingerprint TLS de Chrome."""
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        raise ImportError("curl_cffi no disponible")

    headers = {**HEADERS_BASE}
    if extra_headers:
        headers.update(extra_headers)

    session = cffi_requests.Session(impersonate="chrome110")
    # Primera visita para recoger cookies
    try:
        session.get(url, headers=headers, timeout=15)
    except Exception:
        pass

    if cookies:
        session.cookies.update(cookies)

    resp = session.get(url, headers=headers, timeout=25)
    resp.raise_for_status()
    return resp.text


# ── Estrategia 3: Playwright headless ───────────────────────────────────────

def _fetch_playwright(url: str, wait_selector: str = None, extra_headers: dict = None):
    """
    Descarga con Playwright. Acepta cookies automáticamente buscando
    botones comunes de consent y espera al selector indicado.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        raise ImportError("playwright no disponible")

    # Patrones comunes de botones de aceptar cookies
    COOKIE_BUTTONS = [
        "button:has-text('Aceptar')",
        "button:has-text('Accept')",
        "button:has-text('Acepto')",
        "button:has-text('Accept all')",
        "button:has-text('Aceptar todas')",
        "button:has-text('Allow all')",
        "#onetrust-accept-btn-handler",
        ".cky-btn-accept",
        "[data-testid='cookie-accept']",
        ".js-accept-cookies",
        "#acceptAllButton",
        "a:has-text('Aceptar')",
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="es-ES",
            extra_http_headers=extra_headers or {},
        )
        page = context.new_page()
        page.goto(url, wait_until="networkidle", timeout=40000)

        # Intentar aceptar banner de cookies
        for btn_selector in COOKIE_BUTTONS:
            try:
                btn = page.locator(btn_selector).first
                if btn.is_visible(timeout=2000):
                    btn.click()
                    page.wait_for_timeout(1000)
                    break
            except PWTimeout:
                continue
            except Exception:
                continue

        # Esperar al selector objetivo si se especifica
        if wait_selector:
            try:
                page.wait_for_selector(wait_selector, timeout=15000)
            except PWTimeout:
                logger.warning("wait_selector '%s' no apareció en el tiempo límite", wait_selector)

        html = page.content()
        browser.close()
        return html


# ── Motor principal ──────────────────────────────────────────────────────────

def scrape_precio(
    url: str,
    selector_precio: str,
    selector_fecha: str = None,
    regex_precio: str = None,
    regex_fecha: str = None,
    extra_headers: dict = None,
    cookies: dict = None,
    wait_selector: str = None,
    forzar_playwright: bool = False,
    max_reintentos: int = 2,
) -> dict:
    """
    Obtiene precio (y opcionalmente fecha) de cualquier web financiera.

    Parámetros:
      url               : URL de la página
      selector_precio   : Selector CSS del elemento con el precio
      selector_fecha    : Selector CSS del elemento con la fecha (opcional)
      regex_precio      : Regex alternativo al selector (grupo 1 = precio)
      regex_fecha       : Regex alternativo al selector (grupo 1 = fecha)
      extra_headers     : Headers HTTP adicionales
      cookies           : Cookies manuales a inyectar
      wait_selector     : Selector CSS a esperar en Playwright
      forzar_playwright : Saltar directamente a estrategia 3
      max_reintentos    : Reintentos por estrategia ante errores de red

    Retorna:
      {
        "precio": float o None,
        "fecha": str "YYYY-MM-DD" o None,
        "estrategia": "static" | "cffi" | "playwright",
        "error": str o None
      }
    """
    resultado = {"precio": None, "fecha": None, "estrategia": None, "error": None}

    estrategias = []
    if not forzar_playwright:
        estrategias = [
            ("static", _fetch_static),
            ("cffi", _fetch_cffi),
        ]
    estrategias.append(("playwright", lambda u, **kw: _fetch_playwright(
        u,
        wait_selector=wait_selector,
        extra_headers=extra_headers
    )))

    for nombre, fetch_fn in estrategias:
        for intento in range(1, max_reintentos + 1):
            try:
                logger.info("Scraper [%s] intento %d: %s", nombre, intento, url)

                if nombre in ("static", "cffi"):
                    html = fetch_fn(url, extra_headers=extra_headers, cookies=cookies)
                else:
                    html = fetch_fn(url)

                soup = BeautifulSoup(html, "html.parser")

                # ── Extraer precio ──────────────────────────────────────────
                precio_raw = None
                if selector_precio:
                    precio_raw = extraer_con_selector(soup, selector_precio)
                if not precio_raw and regex_precio:
                    precio_raw = extraer_con_regex(html, regex_precio)

                precio = normalizar_precio(precio_raw) if precio_raw else None

                # ── Extraer fecha ───────────────────────────────────────────
                fecha = None
                if selector_fecha or regex_fecha:
                    fecha_raw = None
                    if selector_fecha:
                        fecha_raw = extraer_con_selector(soup, selector_fecha)
                    if not fecha_raw and regex_fecha:
                        fecha_raw = extraer_con_regex(html, regex_fecha)
                    fecha = normalizar_fecha(fecha_raw) if fecha_raw else None

                if precio is not None:
                    resultado.update({
                        "precio": precio,
                        "fecha": fecha,
                        "estrategia": nombre,
                        "error": None,
                    })
                    logger.info(
                        "OK [%s] precio=%.4f fecha=%s url=%s",
                        nombre, precio, fecha, url
                    )
                    return resultado
                else:
                    logger.warning(
                        "[%s] intento %d: precio no encontrado (selector='%s', raw='%s')",
                        nombre, intento, selector_precio, precio_raw
                    )

            except ImportError as e:
                logger.info("Estrategia [%s] no disponible: %s", nombre, e)
                break  # pasar a la siguiente estrategia

            except Exception as e:
                code = getattr(getattr(e, 'response', None), 'status_code', None)
                if code == 403:
                    logger.warning("[%s] HTTP 403 Forbidden, cambio de estrategia", nombre)
                    break  # no reintentar, cambiar estrategia
                if code == 429:
                    logger.warning("[%s] HTTP 429 Too Many Requests, esperando 10s...", nombre)
                    time.sleep(10)
                logger.warning("[%s] intento %d error: %s", nombre, intento, e)
                if intento < max_reintentos:
                    time.sleep(2 * intento)
                continue

    resultado["error"] = "No se pudo obtener el precio con ninguna estrategia"
    logger.error("FALLO total para: %s", url)
    return resultado


# ── Test de funcionamiento ───────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    casos_test = [
        {
            "nombre": "Azvalor Internacional",
            "url": "https://www.azvalor.com/fondos-de-inversion/azvalor-internacional/",
            "selector_precio": ".elementor-icon-box-description",
            "selector_fecha": ".jet-listing-dynamic-field__content",
            "regex_fecha": r"Datos\s+a\s+(\d{2}/\d{2}/\d{4})",
        },
        {
            "nombre": "Normalizar precio europeo",
            "url": None,  # solo test de utilidad
        },
    ]

    # Test de normalización de precios
    print("=== Test normalizar_precio ===")
    tests_precio = [
        ("363,20 €", 363.20),
        ("1.234,56 €", 1234.56),
        ("1,234.56", 1234.56),
        ("176.54", 176.54),
        ("100,5000 €", 100.5),
        ("  363.2 €  ", 363.2),
    ]
    for entrada, esperado in tests_precio:
        resultado_p = normalizar_precio(entrada)
        ok = "✓" if abs((resultado_p or 0) - esperado) < 0.01 else "✗"
        print(f"  {ok} '{entrada}' -> {resultado_p} (esperado {esperado})")

    # Test de normalización de fechas
    print("\n=== Test normalizar_fecha ===")
    tests_fecha = [
        ("13/05/2026", "2026-05-13"),
        ("2026-05-13", "2026-05-13"),
        ("13 de mayo de 2026", "2026-05-13"),
        ("Datos a  13/05/2026", "2026-05-13"),
        ("13.05.2026", "2026-05-13"),
    ]
    for entrada, esperado in tests_fecha:
        resultado_f = normalizar_fecha(entrada)
        ok = "✓" if resultado_f == esperado else "✗"
        print(f"  {ok} '{entrada}' -> {resultado_f} (esperado {esperado})")

    # Test real Azvalor
    print("\n=== Test scraping Azvalor Internacional ===")
    r = scrape_precio(
        url="https://www.azvalor.com/fondos-de-inversion/azvalor-internacional/",
        selector_precio=".elementor-icon-box-description",
        selector_fecha=".jet-listing-dynamic-field__content",
        regex_fecha=r"Datos\s+a\s+(\d{2}/\d{2}/\d{4})",
    )
    print(f"  Precio   : {r['precio']}")
    print(f"  Fecha    : {r['fecha']}")
    print(f"  Estrategia: {r['estrategia']}")
    if r['error']:
        print(f"  ERROR    : {r['error']}")
