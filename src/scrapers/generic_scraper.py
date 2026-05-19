"""
Scraper polivalente para fondos de inversión.

Estrategias en cascada:
  1. requests + BeautifulSoup (estático)
  2. curl_cffi (bypass TLS fingerprint)
  3. Playwright headless (JavaScript)

CORRECCIÓN BUG CRÍTICO DE FECHA:
  - La fecha se extrae de la web usando selector_fecha.
  - Si no se encuentra fecha, el dato se DESCARTA.
  - NUNCA se usa date.today() como fecha del precio.
  - Para Azvalor: el selector_fecha apunta al párrafo con
    "Desde origen hasta DDMMYYYY" (bloque elementor-element-b1d58c6).
"""
from __future__ import annotations

import re
import time
import logging
from datetime import datetime
from typing import Optional, List, Tuple

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS_BASE = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}

MESES_ES = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


# ─────────────────────────── ESTRATEGIAS DE DESCARGA ──────────────────────────

def fetch_static(url: str, extra_headers: dict = None, cookies: dict = None) -> str:
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


def fetch_cffi(url: str, extra_headers: dict = None, cookies: dict = None) -> str:
    """Estrategia 2: curl_cffi (TLS fingerprint anti-bot)."""
    from curl_cffi import requests as cffi_requests
    headers = {**HEADERS_BASE}
    if extra_headers:
        headers.update(extra_headers)
    resp = cffi_requests.get(url, headers=headers, impersonate="chrome120", timeout=20)
    resp.raise_for_status()
    return resp.text


def fetch_playwright(url: str, wait_selector: str = None, extra_headers: dict = None) -> str:
    """Estrategia 3: Playwright headless (para webs con JavaScript obligatorio)."""
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


# ─────────────────────────── UTILIDADES DE EXTRACCIÓN ─────────────────────────

def _extraer_con_selector(soup: BeautifulSoup, selector: str) -> Optional[str]:
    """Extrae texto limpio usando un selector CSS."""
    try:
        elem = soup.select_one(selector)
        if elem:
            return elem.get_text(separator=" ", strip=True)
    except Exception:
        pass
    return None


def _extraer_con_regex(html: str, patron: str) -> Optional[str]:
    """Extrae el primer grupo capturado de un regex sobre el HTML crudo."""
    try:
        match = re.search(patron, html, re.DOTALL)
        if match:
            return match.group(1).strip()
    except Exception:
        pass
    return None


def _normalizar_precio(texto: str) -> Optional[float]:
    """
    Convierte precio en cualquier formato europeo/anglosajón a float.
    Ejemplos: '363,20' → 363.20 | '1.234,56' → 1234.56 | '1,234.56' → 1234.56
    """
    if not texto:
        return None
    texto = texto.strip()
    texto = re.sub(r"[€$£%\s]", "", texto)
    texto = re.sub(r"[^\d.,\-]", "", texto)
    if not texto:
        return None

    # Formato europeo: 1.234,56
    if re.search(r"\d{1,3}(\.\d{3})+(,\d+)?$", texto):
        texto = texto.replace(".", "").replace(",", ".")
    # Formato anglosajón: 1,234.56
    elif re.search(r"\d{1,3}(,\d{3})+(\.\d+)?$", texto):
        texto = texto.replace(",", "")
    # Coma simple como decimal: 363,20
    else:
        texto = texto.replace(",", ".")

    # Eliminar puntos redundantes
    partes = texto.split(".")
    if len(partes) > 2:
        texto = "".join(partes[:-1]) + "." + partes[-1]

    try:
        return float(texto)
    except ValueError:
        return None


def _normalizar_fecha(texto: str) -> Optional[str]:
    """
    Parsea fechas en múltiples formatos y devuelve 'YYYY-MM-DD'.
    NUNCA devuelve date.today() como fallback.

    Soporta entre otros:
      'Desde origen hasta 18052026'  → 2026-05-18
      'Datos a 13052026'             → 2026-05-13
      '18/05/2026', '18-05-2026', '18.05.2026'
      '2026-05-18' (ISO)
      '13 de mayo de 2026', '13 mayo 2026'
    """
    if not texto:
        return None
    texto_norm = re.sub(r"\s+", " ", texto).strip()

    # Formato compacto DDMMYYYY (usado por Azvalor): "hasta 18052026"
    for m in re.finditer(r"\b(\d{8})\b", texto_norm):
        candidato = m.group(1)
        # Intentar DDMMYYYY
        try:
            d = datetime.strptime(candidato, "%d%m%Y")
            if 2000 <= d.year <= 2100:
                return d.strftime("%Y-%m-%d")
        except ValueError:
            pass
        # Intentar YYYYMMDD
        try:
            d = datetime.strptime(candidato, "%Y%m%d")
            if 2000 <= d.year <= 2100:
                return d.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Formato español largo: "13 de mayo de 2026" o "13 mayo 2026"
    match = re.search(
        r"(\d{1,2})\s+(?:de\s+)?(\w+)\s+(?:de\s+)?(\d{4})",
        texto_norm, re.IGNORECASE,
    )
    if match:
        dia, mes_str, anyo = match.groups()
        if mes_str.lower() in MESES_ES:
            try:
                return f"{anyo}-{MESES_ES[mes_str.lower()]}-{dia.zfill(2)}"
            except Exception:
                pass

    # Formatos con separadores estándar
    formatos = [
        ("%d/%m/%Y", r"\b\d{2}/\d{2}/\d{4}\b"),
        ("%d/%m/%y", r"\b\d{2}/\d{2}/\d{2}\b"),
        ("%d-%m-%Y", r"\b\d{1,2}-\d{1,2}-\d{4}\b"),
        ("%Y-%m-%d", r"\b\d{4}-\d{2}-\d{2}\b"),
        ("%d.%m.%Y", r"\b\d{2}\.\d{2}\.\d{4}\b"),
        ("%d.%m.%y", r"\b\d{2}\.\d{2}\.\d{2}\b"),
    ]
    for fmt, pattern in formatos:
        m = re.search(pattern, texto_norm)
        if m:
            try:
                return datetime.strptime(m.group(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

    return None


# ─────────────────────────── FUNCIÓN PRINCIPAL ────────────────────────────────

def scrape_generic_prices(
    session,
    url: str,
    selector: str,
    selector_fecha: str = None,
    regex_precio: str = None,
    regex_fecha: str = None,
    extra_headers: dict = None,
    cookies: dict = None,
    wait_selector: str = None,
    forzar_playwright: bool = False,
    max_reintentos: int = 2,
) -> List[Tuple[str, float]]:
    """
    Obtiene precio y fecha de cualquier web financiera.
    Devuelve lista de tuplas [(fecha_str, precio_float)] o [] si falla.

    Parámetros:
      session          Sesión requests (de build_session)
      url              URL de la página
      selector         Selector CSS del elemento con el precio
      selector_fecha   Selector CSS del elemento con la fecha ← CLAVE para el bug
      regex_precio     Regex alternativo al selector (grupo 1 = precio)
      regex_fecha      Regex alternativo al selector (grupo 1 = fecha)
      extra_headers    Headers HTTP adicionales
      cookies          Cookies manuales a inyectar
      wait_selector    Selector a esperar en Playwright
      forzar_playwright Saltar directamente a estrategia 3
      max_reintentos   Reintentos por estrategia

    IMPORTANTE: Si precio existe pero fecha = None → dato DESCARTADO.
    NUNCA se usa date.today() como fecha del precio.
    """
    estrategias = []
    if not forzar_playwright:
        estrategias += [
            ("static",    lambda u, kw: fetch_static(u, **kw)),
            ("cffi",      lambda u, kw: fetch_cffi(u, **kw)),
        ]
    estrategias.append(
        ("playwright", lambda u, kw: fetch_playwright(
            u, wait_selector=wait_selector, extra_headers=extra_headers,
        ))
    )

    for nombre, fetch_fn in estrategias:
        for intento in range(1, max_reintentos + 1):
            try:
                logger.info("Scraper '%s' intento %d → %s", nombre, intento, url)
                kw = {"extra_headers": extra_headers, "cookies": cookies}
                html = fetch_fn(url, kw)
                soup = BeautifulSoup(html, "html.parser")

                # ── Extraer precio ──────────────────────────────────────────
                precio_raw = None
                if selector:
                    precio_raw = _extraer_con_selector(soup, selector)
                if not precio_raw and regex_precio:
                    precio_raw = _extraer_con_regex(html, regex_precio)
                precio = _normalizar_precio(precio_raw) if precio_raw else None

                if precio is None:
                    logger.warning(
                        "'%s' intento %d: precio no encontrado (selector='%s', raw=%s)",
                        nombre, intento, selector, precio_raw,
                    )
                    continue

                # ── Extraer fecha de la web ─────────────────────────────────
                fecha = None
                fecha_raw = None

                if selector_fecha:
                    fecha_raw = _extraer_con_selector(soup, selector_fecha)
                    fecha = _normalizar_fecha(fecha_raw) if fecha_raw else None

                if fecha is None and regex_fecha:
                    fecha_raw = _extraer_con_regex(html, regex_fecha)
                    fecha = _normalizar_fecha(fecha_raw) if fecha_raw else None

                # ── Validación crítica: sin fecha → DESCARTAR ───────────────
                if fecha is None:
                    logger.warning(
                        "Generic scraper: precio encontrado (%.4f) pero SIN FECHA "
                        "en la web → dato descartado para no contaminar el histórico "
                        "con fecha incorrecta (%s)",
                        precio, url,
                    )
                    # Intentar siguiente estrategia por si carga más contenido
                    continue

                logger.info(
                    "OK '%s' precio=%.4f fecha=%s → %s",
                    nombre, precio, fecha, url,
                )
                return [(fecha, precio)]

            except ImportError as e:
                logger.info("Estrategia '%s' no disponible: %s", nombre, e)
                break  # Pasar a la siguiente estrategia
            except Exception as e:
                code = getattr(getattr(e, "response", None), "status_code", None)
                if code == 403:
                    logger.warning("'%s' HTTP 403 Forbidden, cambio de estrategia", nombre)
                    break
                if code == 429:
                    logger.warning("'%s' HTTP 429 Too Many Requests, esperando 10s...", nombre)
                    time.sleep(10)
                logger.warning("'%s' intento %d error: %s", nombre, intento, e)
                if intento < max_reintentos:
                    time.sleep(2 * intento)

    logger.error("FALLO total para %s", url)
    return []
