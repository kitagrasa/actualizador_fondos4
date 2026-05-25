"""
Scraper polivalente para fondos de inversión.

Estrategias en cascada:
  1. requests + BeautifulSoup (estático) con rotación de User-Agent y reintentos ante 403.
  2. curl_cffi (bypass TLS fingerprint)
  3. Playwright headless (JavaScript) — opcional, no disponible en GitHub Actions.

MEJORAS IMPLEMENTADAS (2026-05-25):
  - Diagnóstico automático cuando el selector CSS falla:
      * Detecta si la página parece requerir JS (poco texto visible).
      * Busca datos JSON embebidos en etiquetas <script>.
      * Prueba selectores CSS alternativos genéricos (price, last, nav, etc.)
  - Extracción de JSON embebido (JSON-LD, variables JS, data-props).
  - Manejo inteligente de 403: rotación de User-Agent, Referer, delay aleatorio.
  - Módulo especializado para MarketScreener:
      * Detecta dominio marketscreener.com.
      * Extrae ID numérico de la URL y construye la página principal (no la de gráficos).
      * Usa selectores específicos y JSON embebido para obtener precio+fecha.
  - Logging mejorado con diagnóstico detallado (códigos HTTP, longitud HTML, sugerencias).

REGLAS CRÍTICAS:
  - Si se encuentra precio pero NO fecha → dato DESCARTADO.
  - NUNCA se usa date.today() como fecha del precio.
"""
from __future__ import annotations

import re
import time
import random
import json
import logging
from datetime import datetime
from typing import Optional, List, Tuple, Dict, Any
from urllib.parse import urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ─────────────────────────── CONFIGURACIÓN DE USER-AGENTS ──────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

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

# Selectores CSS genéricos alternativos para búsqueda automática
SELECTORES_ALTERNATIVOS = [
    "[class*='price']", "[class*='precio']", "[class*='last']",
    "[class*='nav']", "[class*='value']", "[class*='cotiz']",
    "[id*='price']", "[id*='last']", "[id*='nav']",
    "span.last", "td.last", "div.cotation span", ".price-last",
    "[class*='txt-s7']", "[class*='realtime-last']",
]


# ─────────────────────────── ESTRATEGIAS DE DESCARGA ──────────────────────────

def fetch_static(url: str, extra_headers: dict = None, cookies: dict = None) -> str:
    """
    Estrategia 1: requests estático con rotación de User-Agent y reintentos ante 403.
    """
    headers = {**HEADERS_BASE}
    if extra_headers:
        headers.update(extra_headers)
    
    # Probar varios User-Agents si el primero falla con 403
    for ua in USER_AGENTS:
        headers["User-Agent"] = ua
        # Añadir Referer realista basado en la URL base
        parsed = urlparse(url)
        referer = f"{parsed.scheme}://{parsed.netloc}/"
        headers["Referer"] = referer
        
        session = requests.Session()
        if cookies:
            session.cookies.update(cookies)
        
        try:
            # Delay aleatorio para evitar detección
            time.sleep(random.uniform(1.0, 3.0))
            resp = session.get(url, headers=headers, timeout=20)
            if resp.status_code == 200:
                return resp.text
            elif resp.status_code == 403:
                logger.debug("static: HTTP 403 con User-Agent %s, probando siguiente...", ua[:50])
                continue
            else:
                resp.raise_for_status()
        except Exception as e:
            logger.debug("static: error con User-Agent %s: %s", ua[:50], e)
            continue
    
    raise Exception(f"Todos los User-Agents fallaron para {url}")


def fetch_cffi(url: str, extra_headers: dict = None, cookies: dict = None) -> str:
    """Estrategia 2: curl_cffi (TLS fingerprint anti-bot)."""
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        raise Exception("curl_cffi no disponible")
    
    headers = {**HEADERS_BASE}
    if extra_headers:
        headers.update(extra_headers)
    
    # Usar un User-Agent rotado también para cffi
    headers["User-Agent"] = random.choice(USER_AGENTS)
    parsed = urlparse(url)
    headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"
    
    resp = cffi_requests.get(url, headers=headers, impersonate="chrome120", timeout=20)
    resp.raise_for_status()
    return resp.text


def fetch_playwright(url: str, wait_selector: str = None, extra_headers: dict = None) -> str:
    """Estrategia 3: Playwright headless (para webs con JavaScript obligatorio)."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        raise Exception("playwright no instalado")
    
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
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
        match = re.search(patron, html, re.DOTALL | re.IGNORECASE)
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
    """
    if not texto:
        return None
    texto_norm = re.sub(r"\s+", " ", texto).strip()

    # Formato compacto DDMMYYYY (Azvalor): "hasta 18052026"
    for m in re.finditer(r"\b(\d{8})\b", texto_norm):
        candidato = m.group(1)
        try:
            d = datetime.strptime(candidato, "%d%m%Y")
            if 2000 <= d.year <= 2100:
                return d.strftime("%Y-%m-%d")
        except ValueError:
            pass
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


def _texto_visible(soup: BeautifulSoup) -> int:
    """Calcula caracteres de texto visible (útil para detectar páginas con poco contenido estático)."""
    # Eliminar scripts y estilos
    for script in soup(["script", "style"]):
        script.decompose()
    texto = soup.get_text(separator=" ", strip=True)
    return len(texto)


# ─────────────────────────── EXTRACCIÓN DE JSON EMBEBIDO ─────────────────────────

def extraer_json_embebido(html: str, url: str) -> Optional[Tuple[str, float]]:
    """
    Busca en bloques <script> del HTML datos estructurados que puedan contener
    precio y fecha. Soporta:
      - JSON-LD (application/ld+json)
      - Variables JS (var data = {...})
      - Atributos data-props
    Devuelve (fecha_iso, precio) si encuentra ambos, o None.
    """
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script")
    
    # Patrones para buscar precio y fecha en JSON
    patron_precio = r'"(?:nav|price|last|close|valor|cotiz)[_\w]*"\s*:\s*([\d.,]+)'
    patron_fecha = r'"(?:date|fecha|time|timestamp)[_\w]*"\s*:\s*"([^"]+)"'
    
    for script in scripts:
        if not script.string:
            continue
        contenido = script.string
        # Intentar parsear como JSON si es application/ld+json
        if script.get("type") == "application/ld+json":
            try:
                data = json.loads(contenido)
                # Recorrer recursivamente buscando precio y fecha
                precio, fecha = _buscar_precio_fecha_en_json(data)
                if precio is not None and fecha is not None:
                    fecha_norm = _normalizar_fecha(fecha)
                    if fecha_norm:
                        return (fecha_norm, precio)
            except json.JSONDecodeError:
                pass
        
        # Buscar patrones con regex en el contenido del script
        match_precio = re.search(patron_precio, contenido, re.IGNORECASE)
        match_fecha = re.search(patron_fecha, contenido, re.IGNORECASE)
        if match_precio and match_fecha:
            precio_raw = match_precio.group(1)
            fecha_raw = match_fecha.group(1)
            precio = _normalizar_precio(precio_raw)
            fecha = _normalizar_fecha(fecha_raw)
            if precio is not None and fecha is not None:
                return (fecha, precio)
    
    # Buscar en atributos data-* de cualquier elemento
    for elem in soup.find_all(attrs={"data-props": True}):
        try:
            props = json.loads(elem["data-props"])
            precio, fecha = _buscar_precio_fecha_en_json(props)
            if precio is not None and fecha is not None:
                fecha_norm = _normalizar_fecha(fecha)
                if fecha_norm:
                    return (fecha_norm, precio)
        except (json.JSONDecodeError, TypeError):
            pass
    
    return None


def _buscar_precio_fecha_en_json(obj: Any, depth: int = 0) -> Tuple[Optional[float], Optional[str]]:
    """Recursivamente busca campos 'price', 'nav', 'last', 'date' en un objeto JSON."""
    if depth > 10:
        return None, None
    if isinstance(obj, dict):
        precio = None
        fecha = None
        # Buscar precio
        for key in ["price", "nav", "last", "close", "valor", "cotiz"]:
            if key in obj:
                val = obj[key]
                if isinstance(val, (int, float)):
                    precio = float(val)
                    break
                elif isinstance(val, str):
                    precio = _normalizar_precio(val)
                    if precio is not None:
                        break
        # Buscar fecha
        for key in ["date", "fecha", "time", "timestamp", "datetime"]:
            if key in obj:
                val = obj[key]
                if isinstance(val, str):
                    fecha = val
                    break
                elif isinstance(val, (int, float)):
                    # timestamp UNIX (segundos)
                    try:
                        dt = datetime.fromtimestamp(val)
                        fecha = dt.strftime("%Y-%m-%d")
                        break
                    except:
                        pass
        if precio is not None and fecha is not None:
            return precio, fecha
        # Recursión en valores
        for v in obj.values():
            p, f = _buscar_precio_fecha_en_json(v, depth+1)
            if p is not None and f is not None:
                return p, f
    elif isinstance(obj, list):
        for item in obj:
            p, f = _buscar_precio_fecha_en_json(item, depth+1)
            if p is not None and f is not None:
                return p, f
    return None, None


# ─────────────────────────── MÓDULO ESPECIALIZADO: MARKETSCREENER ─────────────────────────

def _es_marketscreener(url: str) -> bool:
    """Detecta si la URL pertenece a marketscreener.com o sus variantes regionales."""
    dominio = urlparse(url).netloc.lower()
    return "marketscreener.com" in dominio or "zonebourse.com" in dominio


def _extraer_id_marketscreener(url: str) -> Optional[str]:
    """
    Extrae el ID numérico de la URL de MarketScreener.
    Ejemplo: /POLAR-CAPITAL-SMART-ENERG-197153219/ → 197153219
    """
    match = re.search(r'-(\d{6,12})/', url)
    if match:
        return match.group(1)
    return None


def _construir_url_principal_marketscreener(url: str, id_num: str) -> str:
    """
    Construye la URL de la página principal (resumen) a partir de una URL de gráficos.
    Ejemplo:
      Input:  https://es.marketscreener.com/cotizacion/fondos/POLAR-CAPITAL-SMART-ENERG-197153219/graficos-comparacion/
      Output: https://es.marketscreener.com/cotizacion/fondos/POLAR-CAPITAL-SMART-ENERG-197153219/
    """
    parsed = urlparse(url)
    # Dividir path en segmentos
    segments = [s for s in parsed.path.split('/') if s]
    # Buscar el segmento que contiene el ID
    for i, seg in enumerate(segments):
        if id_num in seg:
            # Quitar todo después de este segmento (incluyéndolo)
            new_path = '/' + '/'.join(segments[:i+1]) + '/'
            break
    else:
        # Fallback: eliminar el último segmento si parece ser "graficos-*"
        if segments and ('graficos' in segments[-1] or 'graphics' in segments[-1]):
            new_path = '/' + '/'.join(segments[:-1]) + '/'
        else:
            new_path = parsed.path
    # Reconstruir URL sin query ni fragment
    return urlunparse((parsed.scheme, parsed.netloc, new_path, '', '', ''))


def _scrape_marketscreener(session, url: str, selector_original: str) -> Optional[Tuple[str, float]]:
    """
    Scraper especializado para MarketScreener.
    Detecta si la URL es de gráficos (contiene "/graficos-") y redirige a la página principal.
    Luego extrae precio y fecha usando selectores específicos y JSON embebido.
    """
    logger.info("MarketScreener: detectado, aplicando lógica especializada para %s", url)
    
    # Extraer ID numérico
    id_num = _extraer_id_marketscreener(url)
    if not id_num:
        logger.warning("MarketScreener: no se pudo extraer ID de la URL, usando método genérico")
        return None
    
    # Construir URL principal si la actual parece ser de gráficos
    if "/graficos" in url or "/graphics" in url:
        url_principal = _construir_url_principal_marketscreener(url, id_num)
        logger.info("MarketScreener: redirigiendo de página de gráficos a página principal: %s", url_principal)
    else:
        url_principal = url
    
    # Descargar página principal con curl_cffi (por el TLS fingerprint)
    try:
        html = fetch_cffi(url_principal)
    except Exception as e:
        logger.error("MarketScreener: error descargando %s con cffi: %s", url_principal, e)
        return None
    
    soup = BeautifulSoup(html, "html.parser")
    
    # 1. Buscar precio con selectores específicos
    selectores_precio = [
        "span.last", "td.last",
        "[class*='txt-s7']", "[class*='realtime-last']",
        "div.cotation span", ".price-last",
        selector_original  # también probar el selector original por si acaso
    ]
    precio = None
    for sel in selectores_precio:
        raw = _extraer_con_selector(soup, sel)
        if raw:
            precio = _normalizar_precio(raw)
            if precio is not None:
                logger.debug("MarketScreener: precio encontrado con selector %s = %s", sel, precio)
                break
    
    if precio is None:
        # Intentar extracción desde JSON embebido
        json_result = extraer_json_embebido(html, url_principal)
        if json_result:
            fecha, precio = json_result
            logger.info("MarketScreener: precio+fecha extraídos de JSON embebido: %s = %.4f", fecha, precio)
            return (fecha, precio)
        logger.warning("MarketScreener: no se pudo extraer precio de %s", url_principal)
        return None
    
    # 2. Buscar fecha cercana al precio (elementos hermanos o padres)
    fecha = None
    # Buscar cualquier elemento que contenga una fecha cerca del elemento de precio
    # Estrategia simple: buscar en todo el HTML con regex
    match_fecha = re.search(r'(\d{2}/\d{2}/\d{4})', html)
    if match_fecha:
        fecha = _normalizar_fecha(match_fecha.group(1))
    if not fecha:
        match_fecha = re.search(r'(\d{2}\.\d{2}\.\d{4})', html)
        if match_fecha:
            fecha = _normalizar_fecha(match_fecha.group(1))
    if not fecha:
        # Buscar meta tag
        meta_date = soup.find("meta", attrs={"name": "date"})
        if meta_date and meta_date.get("content"):
            fecha = _normalizar_fecha(meta_date["content"])
    
    if fecha is None:
        logger.warning("MarketScreener: precio encontrado (%.4f) pero sin fecha, descartando", precio)
        return None
    
    logger.info("MarketScreener: extracción exitosa -> fecha=%s, precio=%.4f", fecha, precio)
    return (fecha, precio)


# ─────────────────────────── DIAGNÓSTICO Y SELECCIÓN AUTOMÁTICA ─────────────────────────

def _diagnosticar_fallo_selector(html: str, url: str, selector: str, soup: BeautifulSoup) -> None:
    """
    Loguea información de diagnóstico cuando el selector CSS no encuentra nada.
    """
    logger.warning("DIAGNÓSTICO: el selector '%s' no encontró nada en %s", selector, url)
    
    # Tamaño del texto visible
    texto_len = _texto_visible(soup)
    logger.info("DIAGNÓSTICO: texto visible en página: %d caracteres", texto_len)
    if texto_len < 2000:
        logger.warning("DIAGNÓSTICO: página parece requerir JavaScript (poco texto visible).")
    
    # Buscar selectores alternativos genéricos
    logger.info("DIAGNÓSTICO: probando selectores alternativos genéricos...")
    for sel_alt in SELECTORES_ALTERNATIVOS:
        raw = _extraer_con_selector(soup, sel_alt)
        if raw:
            precio = _normalizar_precio(raw)
            if precio is not None:
                logger.warning("SUGERENCIA: selector alternativo '%s' produce precio válido: %.4f. Considera actualizar el CSV.", sel_alt, precio)
                break
    
    # Detectar si hay JSON embebido
    if extraer_json_embebido(html, url):
        logger.info("DIAGNÓSTICO: se encontraron datos JSON embebidos que contienen precio/fecha.")


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
      session          Sesión requests (de build_session) - se usa solo para estrategias que lo necesitan.
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
    # ── MÓDULO ESPECIALIZADO: MarketScreener ──────────────────────────────────
    if _es_marketscreener(url):
        resultado = _scrape_marketscreener(session, url, selector)
        if resultado:
            return [resultado]
        # Si falla, continuar con estrategias normales como fallback
        logger.info("MarketScreener: especializado falló, continuando con estrategias genéricas")
    
    estrategias = []
    if not forzar_playwright:
        estrategias += [
            ("static",    lambda u, kw: fetch_static(u, **kw)),
            ("cffi",      lambda u, kw: fetch_cffi(u, **kw)),
        ]
    try:
        # Verificar si playwright está disponible
        import playwright
        estrategias.append(
            ("playwright", lambda u, kw: fetch_playwright(
                u, wait_selector=wait_selector, extra_headers=extra_headers,
            ))
        )
    except ImportError:
        logger.debug("Playwright no disponible, omitiendo estrategia")

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
                
                # Si no se encontró precio, hacer diagnóstico
                if precio is None:
                    _diagnosticar_fallo_selector(html, url, selector, soup)
                    # Intentar extracción desde JSON embebido
                    json_result = extraer_json_embebido(html, url)
                    if json_result:
                        fecha, precio = json_result
                        logger.info("Extracción exitosa desde JSON embebido: %s = %.4f", fecha, precio)
                        return [(fecha, precio)]
                    continue  # Probar siguiente estrategia/reintento
                
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
                    # Intentar extraer fecha desde JSON embebido como último recurso
                    json_result = extraer_json_embebido(html, url)
                    if json_result:
                        fecha, precio = json_result
                        logger.info("Extracción desde JSON embebido (fallback fecha): %s = %.4f", fecha, precio)
                        return [(fecha, precio)]
                    continue  # Probar siguiente estrategia
                
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
    
    logger.error("FALLO total para %s (después de todas las estrategias)", url)
    # Log adicional de diagnóstico final
    logger.info("DIAGNÓSTICO FINAL: No se pudo extraer datos de %s. Verifica que la web no requiera JavaScript o que los selectores sean correctos.", url)
    return []
