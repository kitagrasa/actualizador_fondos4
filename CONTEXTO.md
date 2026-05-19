1. El proyecto es un orquestador automatizado en Python para extraer precios históricos de fondos y ETFs.
2. Su núcleo es `src/app.py`, el script principal que coordina la ejecución secuencial de todo el sistema.
3. `src/config.py` lee y parsea las URLs de cada activo desde un CSV remoto o una hoja de Google Sheets.
4. `src/http_client.py` establece una sesión de `requests` con reintentos y cabeceras para evitar bloqueos.
5. Los módulos dentro de `src/scrapers/` encapsulan la lógica específica de extracción para cada plataforma: `ft_scraper.py`, `fundsquare_scraper.py`, `ariva_scraper.py`, `yahoo_finance_scraper.py`, **`cobas_scraper.py`** y **`generic_scraper.py`**.
6. **`generic_scraper.py`** es un scraper polivalente que puede extraer el precio de CUALQUIER web usando un selector CSS proporcionado en el CSV. Funciona como la "Tabla Web" de Portfolio Performance pero automático. Usa tres estrategias en cascada: requests estático → curl_cffi (TLS fingerprint) → Playwright headless (opcional). Gestiona automáticamente cookies, rate limiting, reintentos y formatos de precio europeo/anglosajón.
7. Scrapers como `ft_scraper.py` analizan el HTML financiero apoyándose en `beautifulsoup4` y `lxml`.
8. Otros como `yahoo_finance_scraper.py` atacan directamente APIs JSON internas para mayor eficiencia.
9. `src/utils.py` provee herramientas transversales para el formateo de fechas, logs y serialización JSON.
10. Una vez descargados los datos, `app.py` los unifica utilizando la función central `merge_updates()`.
11. Esta fusión aplica un sistema de prioridades donde el último en escribir gana. La jerarquía actual (de menor a mayor) es: **Generic → Ariva → Fundsquare → FT → Yahoo → Cobas**.
12. Yahoo Finance actúa como red de seguridad solicitando 10 años de historia para tapar huecos antiguos.
13. **El scraper de Investing.com ha sido eliminado del proyecto** por inestabilidad (bloqueos 403) y no se utiliza en ningún proceso.
14. `src/portfolio.py` gestiona la persistencia, comparando y guardando las series temporales en disco.
15. El almacenamiento genera un archivo `.json` independiente por cada ISIN dentro de la carpeta `data/prices/`.
16. Paralelamente, se mantiene un diccionario global con nombres y divisas en `funds_metadata.json`.
17. El flujo limpia automáticamente la carpeta de datos si detecta que un ISIN fue retirado de la configuración.
18. La automatización se delega en GitHub Actions, ejecutando el flujo completo mediante trabajos programados.
19. La configuración inicial inyecta datos al sistema mediante la variable de entorno protegida `FUNDS_CSV_URL`.
20. Las dependencias externas clave son `requests` (red), `beautifulsoup4` (parsing), `lxml` (motor de velocidad) y `curl_cffi` (TLS fingerprint anti-bot).
21. Todo el código aprovecha librerías estándar de Python (`dataclasses`, `pathlib`, `json`) minimizando carga.
22. El output resultante produce datos estructurados y limpios listos para utilizarse en Portfolio Performance.
23. Para añadir un fondo con el scraper genérico, basta con añadir en el CSV las columnas `generic_url` (URL de la web) y `generic_selector` (selector CSS del precio). No es necesario escribir código nuevo.

Quiero que optimices siempre los procesos, que sean robustos y comprueba que funcionen correctamente. Cuando realices algún cambio en el proyecto que afecte al contexto, actualiza el contexto también. Eres un experto en Python y en scraping/extracción de datos financieros. No se programar. Proporciona el código completo de cada archivo, listo para usar. Explica brevemente cada sección del código con comentarios en español. El código debe ser robusto ante datos sucios o inesperados en el Spreadsheet.
