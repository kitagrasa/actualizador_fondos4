# actualizador_fondos4
Actualizacion automatica de precios de fondos


# Fund NAV updater (Portfolio Performance)

- Output: `data/prices/{ISIN}.json` en formato:
  [
    {"date": "2026-02-11", "close": 32.76},
    {"date": "2026-02-10", "close": 32.83}
  ]

Edita solo `funds.csv` para añadir/quitar fondos.

## Columnas del CSV (soporte para dos scraper genéricos)

- **Obligatoria:** `isin`
- **Fuentes normales:** `ft_url`, `fundsquare_url`, `ariva_url`, `yahoo_url`, `cobas_url`
- **Scraper genérico fuente 1:** `gen_url1`, `gen_selec_imp1`, `gen_selec_fecha1`
- **Scraper genérico fuente 2 (prioridad mayor que fuente 1):** `gen_url2`, `gen_selec_imp2`, `gen_selec_fecha2`
- **Compatibilidad:** Las antiguas columnas `generic_url`, `generic_selector`, `genericselectorfecha` se siguen aceptando (se asignan a la fuente 1).

  

Eres un experto en Python y scraping de datos financieros. Ten en cuenta que no sé programar. Esto es un proyecto github online, no está ejecutado localmente y tampoco en mi PC. Dame siempre el código COMPLETO de cada archivo a modificar, listo para usar, con comentarios en español. Repito, importante: dame SIEMPRE el código COMPLETO de cada archivo a modificar, quiero los códigos completos, no solo parciales. El código debe ser robusto y optimizado. Verifica profundamente que funcione antes de darme la respuesta
