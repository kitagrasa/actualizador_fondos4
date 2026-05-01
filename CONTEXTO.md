1. El proyecto es un orquestador automatizado en Python para extraer precios históricos de fondos y ETFs.
2. Su núcleo es `src/app.py`, el script principal que coordina la ejecución secuencial de todo el sistema.
3. `src/config.py` lee y parsea las URLs de cada activo desde un CSV remoto o una hoja de Google Sheets.
4. `src/http_client.py` establece una sesión de `requests` con reintentos y cabeceras para evitar bloqueos.
5. Los módulos dentro de `src/scrapers/` encapsulan la lógica específica de extracción para cada plataforma.
6. Scrapers como `ft_scraper.py` analizan el HTML financiero apoyándose en `beautifulsoup4` y `lxml`.
7. Otros como `yahoo_finance_scraper.py` atacan directamente APIs JSON internas para mayor eficiencia.
8. `src/utils.py` provee herramientas transversales para el formateo de fechas, logs y serialización JSON.
9. Una vez descargados los datos, `app.py` los unifica utilizando la función central `merge_updates()`.
10. Esta fusión aplica un sistema de prioridades donde Financial Times sobrescribe a las fuentes de respaldo.
11. Yahoo Finance actúa como red de seguridad solicitando 10 años de historia para tapar huecos antiguos.
12. `src/portfolio.py` gestiona la persistencia, comparando y guardando las series temporales en disco.
13. El almacenamiento genera un archivo `.json` independiente por cada ISIN dentro de la carpeta `data/prices/`.
14. Paralelamente, se mantiene un diccionario global con nombres y divisas en `funds_metadata.json`.
15. El flujo limpia automáticamente la carpeta de datos si detecta que un ISIN fue retirado de la configuración.
16. La automatización se delega en GitHub Actions, ejecutando el flujo completo mediante trabajos programados.
17. La configuración inicial inyecta datos al sistema mediante la variable de entorno protegida `FUNDS_CSV_URL`.
18. Las dependencias externas clave son `requests` (red), `beautifulsoup4` (parsing) y `lxml` (motor de velocidad).
19. Todo el código aprovecha librerías estándar de Python (`dataclasses`, `pathlib`, `json`) minimizando carga.
20. El output resultante produce datos estructurados y limpios listos para utilizarse en Portfolio Performance.
