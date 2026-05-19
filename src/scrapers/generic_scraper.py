"""
Scraper polivalente — equivalente a la función "tabla web" de Portfolio Performance.
Estrategias en cascada:
  1. requests + BeautifulSoup (estático, rápido)
  2. curl_cffi (bypass TLS fingerprint anti-bot básico)
  3. Playwright headless (JavaScript obligatorio)

BUG CRÍTICO CORREGIDO:
  - scrape_generic_prices() es la función que llama app.py.
  - Si la web no publica una fecha junto al precio, el dato se DESCARTA.
  - Nunca se usa date.today() como fecha del precio.
"""

import re
import time
import logging
from datetime import datetime
from typing import Optional, List, Tuple

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ── Cabeceras HTTP realistas ──────────────────────────────────────────────────
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

# Meses en español para parseo de fechas
MESES_ES = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


# ── Estrategias de descarga ───────────────────────────────────────────────────

def fetch_static(url: str, extra_headers: dict = None, cookies: dict = None):
    """Estrategia 1: requests estático."""
    headers = {**HEADERS_BASE}
    if extra_headers:
        headers.update(extra_headers)
    session = requests.Session()
    if cookies:
        session.cookies.update(cookies)
    resp = session.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    return resp.text


def fetch_cffi(url: str, extra_headers: dict = None, cookies: dict = None):
    """Estrategia 2: curl_cffi (TLS fingerprint). Requiere curl_cffi instalado."""
    from curl_cffi import requests as cffi_requests
    headers = {**HEADERS_BASE}
    if extra_headers:
        headers.update(extra_headers)
    resp = cffi_requests.get(url, headers=headers, impersonate="chrome120", timeout=20)
    resp.raise_for_status()
    return resp.text


def fetch_playwright(url: str, wait_selector: str = None, extra_headers: dict = None):
    """Estrategia 3: Playwright headless (JavaScript crítico)."""
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            extra_http_headers=extra_headers or {},
        )
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        if wait_selector:
            try:
                page.wait_for_selector(wait_selector, timeout=15000)
            except PWTimeout:
                logger.warning("wait_selector '%s' no apareció en el tiempo límite", wait_selector)
        html = page.content()
        browser.close()
    return html


# ── Utilidades de extracción ──────────────────────────────────────────────────

def extraer_con_selector(soup: BeautifulSoup, selector: str) -> Optional[str]:
    """Extrae texto limpio usando un selector CSS."""
    try:
        elem = soup.select_one(selector)
        if elem:
            return elem.get_text(separator=" ", strip=True)
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


def normalizar_precio(texto: str) -> Optional[float]:
    """
    Convierte precio en cualquier formato europeo/anglosajón a float.
    Ejemplos: '363,20' → 363.20 | '1.234,56' → 1234.56 | '1,234.56' → 1234.56
    """
    if not texto:
        return None
    texto = texto.strip()
    # Eliminar símbolos de moneda y espacios
    texto = re.sub(r"[€$£%\s]", "", texto)
    # Eliminar caracteres no numéricos salvo coma, punto y signo
    texto = re.sub(r"[^\d.,\-]", "", texto)
    if not texto:
        return None

    # Detectar formato europeo (punto=millar, coma=decimal): ej. 1.234,56
    if re.search(r"\d{1,3}(\.\d{3})+(,\d+)?$", texto):
        texto = texto.replace(".", "").replace(",", ".")
    # Detectar formato anglosajón (coma=millar, punto=decimal): ej. 1,234.56
    elif re.search(r"\d{1,3}(,\d{3})+(\.\d+)?$", texto):
        texto = texto.replace(",", "")
    # Coma simple como decimal: ej. 363,20
    else:
        texto = texto.replace(",", ".")

    # Eliminar puntos redundantes (más de uno)
    partes = texto.split(".")
    if len(partes) > 2:
        texto = "".join(partes[:-1]) + "." + partes[-1]

    try:
        return float(texto)
    except ValueError:
        return None


def normalizar_fecha(texto: str) -> Optional[str]:
    """
    Intenta parsear una fecha en múltiples formatos habituales en webs financieras.
    Devuelve 'YYYY-MM-DD' o None si no reconoce el formato.
    NUNCA devuelve date.today() como fallback.
    """
    if not texto:
        return None
    texto = re.sub(r"\s+", " ", texto).strip()

    # Formato español largo: "13 de mayo de 2026" o "13 mayo 2026"
    match = re.search(r"(\d{1,2})\s+(?:de\s+)?(\w+)\s+(?:de\s+)?(\d{4})", texto, re.IGNORECASE)
    if match:
        dia, mes_str, anyo = match.groups()
        mes_str = mes_str.lower()
        if mes_str in MESES_ES:
            try:
                return f"{anyo}-{MESES_ES[mes_str]}-{dia.zfill(2)}"
            except Exception:
                pass

    formatos = [
        ("%d/%m/%Y", r"\d{2}/\d{2}/\d{4}"),
        ("%d/%m/%y", r"\d{2}/\d{2}/\d{2}"),
        ("%d-%m-%Y", r"\d{1,2}-\d{1,2}-\d{4}"),
        ("%Y-%m-%d", r"\d{4}-\d{2}-\d{2}"),
        ("%d.%m.%Y", r"\d{2}\.\d{2}\.\d{4}"),
        ("%d.%m.%y", r"\d{2}\.\d{2}\.\d{2}"),
    ]
    for fmt, pattern in formatos:
        match = re.search(pattern, texto)
        if match:
            try:
                return datetime.strptime(match.group(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

    return None


# ── Función principal de scraping ─────────────────────────────────────────────

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
    Obtiene precio y opcionalmente fecha de cualquier web financiera.

    Parámetros:
      url              URL de la página
      selector_precio  Selector CSS del elemento con el precio
      selector_fecha   Selector CSS del elemento con la fecha (opcional)
      regex_precio     Regex alternativo al selector (grupo 1 = precio)
      regex_fecha      Regex alternativo al selector (grupo 1 = fecha)
      extra_headers    Headers HTTP adicionales
      cookies          Cookies manuales a inyectar
      wait_selector    Selector CSS a esperar en Playwright
      forzar_playwright Saltar directamente a estrategia 3
      max_reintentos   Reintentos por estrategia ante errores de red

    Retorna dict con claves:
      precio    float o None
      fecha     str 'YYYY-MM-DD' o None  ← NUNCA usa date.today() como fallback
      estrategia str o None
      error     str o None
    """
    resultado = {"precio": None, "fecha": None, "estrategia": None, "error": None}

    estrategias = []
    if not forzar_playwright:
        estrategias += [
            ("static", lambda u, kw: fetch_static(u, **kw)),
            ("cffi",   lambda u, kw: fetch_cffi(u, **kw)),
        ]
    estrategias.append(
        ("playwright", lambda u, kw: fetch_playwright(u, wait_selector=wait_selector, extra_headers=extra_headers))
    )

    for nombre, fetch_fn in estrategias:
        for intento in range(1, max_reintentos + 1):
            try:
                logger.info("Scraper '%s' intento %d → %s", nombre, intento, url)
                kw = {"extra_headers": extra_headers, "cookies": cookies}
                html = fetch_fn(url, kw)
                soup = BeautifulSoup(html, "html.parser")

                # — Extraer precio —
                precio_raw = None
                if selector_precio:
                    precio_raw = extraer_con_selector(soup, selector_precio)
                if not precio_raw and regex_precio:
                    precio_raw = extraer_con_regex(html, regex_precio)
                precio = normalizar_precio(precio_raw) if precio_raw else None

                # — Extraer fecha —
                fecha_raw = None
                if selector_fecha:
                    fecha_raw = extraer_con_selector(soup, selector_fecha)
                if not fecha_raw and regex_fecha:
                    fecha_raw = extraer_con_regex(html, regex_fecha)
                fecha = normalizar_fecha(fecha_raw) if fecha_raw else None

                if precio is not None:
                    resultado.update({
                        "precio": precio,
                        "fecha": fecha,   # puede ser None si la web no expone fecha
                        "estrategia": nombre,
                        "error": None,
                    })
                    logger.info(
                        "OK '%s' precio=%.4f fecha=%s → %s",
                        nombre, precio, fecha, url
                    )
                    return resultado
                else:
                    logger.warning(
                        "'%s' intento %d: precio no encontrado (selector=%r, raw=%r)",
                        nombre, intento, selector_precio, precio_raw
                    )

            except ImportError as e:
                logger.info("Estrategia '%s' no disponible: %s", nombre, e)
                break  # pasar a la siguiente estrategia

            except Exception as e:
                code = getattr(getattr(e, "response", None), "status_code", None)
                if code == 403:
                    logger.warning("'%s' HTTP 403 Forbidden → cambio de estrategia", nombre)
                    break  # no reintentar, cambiar estrategia
                if code == 429:
                    logger.warning("'%s' HTTP 429 Too Many Requests → esperando 10s...", nombre)
                    time.sleep(10)
                logger.warning("'%s' intento %d error: %s", nombre, intento, e)
                if intento < max_reintentos:
                    time.sleep(2 * intento)
                continue

    resultado["error"] = "No se pudo obtener el precio con ninguna estrategia"
    logger.error("FALLO total para %s", url)
    return resultado


# ── Función pública llamada desde app.py ──────────────────────────────────────

def scrape_generic_prices(
    session,
    url: str,
    selector: str,
    selector_fecha: str = None,
    regex_fecha: str = None,
) -> List[Tuple[str, float]]:
    """
    Interfaz compatible con app.py.

    Llama a scrape_precio() y devuelve una lista con UNA sola tupla (fecha, precio)
    si y solo si:
      - Se obtuvo un precio válido, Y
      - Se obtuvo una fecha válida extraída de la propia web.

    Si la web no publica una fecha junto al precio, el dato se DESCARTA
    y se devuelve lista vacía [].
    NUNCA se usa date.today() como fecha del precio.

    Parámetros:
      session          requests.Session (ignorado internamente, reservado para compatibilidad)
      url              URL de la página
      selector         Selector CSS del precio
      selector_fecha   Selector CSS de la fecha (opcional, recomendado)
      regex_fecha      Regex alternativo para la fecha (opcional)

    Retorna:
      [(fecha_iso, precio_float)]  si precio Y fecha encontrados
      []                           si falta precio o falta fecha
    """
    if not url or not selector:
        return []

    resultado = scrape_precio(
        url=url,
        selector_precio=selector,
        selector_fecha=selector_fecha,
        regex_fecha=regex_fecha,
    )

    precio = resultado.get("precio")
    fecha  = resultado.get("fecha")

    # — Validación estricta: solo guardamos si AMBOS campos están presentes —
    if precio is None:
        logger.warning("Generic scraper: precio no encontrado → descartado (%s)", url)
        return []

    if fecha is None:
        # BUG CRÍTICO CORREGIDO: si no hay fecha publicada en la web,
        # NO usamos date.today() como fallback. Se descarta el dato.
        logger.warning(
            "Generic scraper: precio encontrado (%.4f) pero SIN FECHA en la web → "
            "dato descartado para no contaminar el histórico con fecha incorrecta (%s)",
            precio, url
        )
        return []

    logger.info("Generic scraper: guardando precio=%.4f fecha=%s (%s)", precio, fecha, url)
    return [(fecha, precio)]


# ── Test manual (ejecutar directamente) ──────────────────────────────────────

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    # Test normalización de precios
    casos_precio = [
        ("363,20",    363.20),
        ("1.234,56",  1234.56),
        ("1,234.56",  1234.56),
        ("176.54",    176.54),
        ("100,5000",  100.5),
        ("363.2",     363.2),
    ]
    print("\n── Test normalizar_precio ──")
    for entrada, esperado in casos_precio:
        resultado_p = normalizar_precio(entrada)
        ok = "✓" if abs((resultado_p or 0) - esperado) < 0.01 else "✗"
        print(f"  {ok}  '{entrada}' → {resultado_p}  (esperado {esperado})")

    # Test normalización de fechas
    casos_fecha = [
        ("30-4-2026",             "2026-04-30"),
        ("13 de mayo de 2026",    "2026-05-13"),
        ("13 mayo 2026",          "2026-05-13"),
        ("19/05/2026",            "2026-05-19"),
        ("2026-05-19",            "2026-05-19"),
        ("texto sin fecha",       None),
    ]
    print("\n── Test normalizar_fecha ──")
    for entrada, esperado in casos_fecha:
        resultado_f = normalizar_fecha(entrada)
        ok = "✓" if resultado_f == esperado else "✗"
        print(f"  {ok}  '{entrada}' → {resultado_f}  (esperado {esperado})")

    # Test real Azvalor Internacional
    print("\n── Test real Azvalor Internacional ──")
    r = scrape_precio(
        url="https://www.azvalor.com/fondos-de-inversion/azvalor-internacional/",
        selector_precio=".elementor-icon-box-description",
        selector_fecha=".jet-listing-dynamic-field__content",
        regex_fecha=r"Datos a (\d{2}/\d{2}/\d{4})",
    )
    print(f"  Precio:    {r['precio']}")
    print(f"  Fecha:     {r['fecha']}")
    print(f"  Estrategia:{r['estrategia']}")
    if r["error"]:
        print(f"  ERROR:     {r['error']}")
