"""Tests de carga del CSV de fondos y de la ocultación de la URL secreta."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.config import _redact_url, load_funds_csv

CSV_BASE = (
    "ISIN,ft_url,gen_url1,gen_selec_imp1,gen_selec_fecha1\n"
    "LU0000000001,https://ft/1,https://gen1,.precio,.fecha\n"
    "LU0000000002,,https://gen2,.otro,\n"
)


class LoadFundsCsvTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)

    def _escribir(self, texto: str, nombre: str = "funds.csv") -> Path:
        ruta = self.dir / nombre
        ruta.write_text(texto, encoding="utf-8")
        return ruta

    def test_columnas_gen_y_ft(self):
        fondos = load_funds_csv(self._escribir(CSV_BASE))
        self.assertEqual([f.isin for f in fondos], ["LU0000000001", "LU0000000002"])
        self.assertEqual(fondos[0].fturl, "https://ft/1")
        self.assertEqual(fondos[0].gen_selector1, ".precio")
        self.assertEqual(fondos[0].gen_fecha1, ".fecha")
        self.assertEqual(fondos[1].gen_url1, "https://gen2")

    def test_bom_y_cabeceras_en_mayusculas(self):
        texto = "\ufeff" + CSV_BASE.replace("ISIN", "Isin").replace("ft_url", "FT_URL")
        fondos = load_funds_csv(self._escribir(texto))
        self.assertEqual(len(fondos), 2)
        self.assertEqual(fondos[0].fturl, "https://ft/1")

    def test_columnas_antiguas_se_asignan_a_la_fuente_1(self):
        texto = "isin,generic_url,generic_selector,genericselectorfecha\n" \
                "LU0000000003,https://viejo,.sel,.fec\n"
        fondos = load_funds_csv(self._escribir(texto))
        self.assertEqual(fondos[0].gen_url1, "https://viejo")
        self.assertEqual(fondos[0].gen_selector1, ".sel")
        self.assertEqual(fondos[0].gen_fecha1, ".fec")

    def test_duplicados_gana_la_ultima_fila(self):
        texto = CSV_BASE + "LU0000000001,https://ft/duplicado,,\n"
        fondos = load_funds_csv(self._escribir(texto))
        self.assertEqual(len(fondos), 2)
        self.assertEqual(fondos[0].fturl, "https://ft/duplicado")

    def test_fila_sin_isin_se_omite(self):
        texto = CSV_BASE + ",https://sin-isin,,\n"
        self.assertEqual(len(load_funds_csv(self._escribir(texto))), 2)

    def test_sin_columna_isin_devuelve_vacio(self):
        texto = "ft_url,gen_url1\nhttps://ft/1,\n"
        self.assertEqual(load_funds_csv(self._escribir(texto)), [])

    def test_archivo_inexistente_devuelve_vacio(self):
        self.assertEqual(load_funds_csv(self.dir / "no-existe.csv"), [])


class RedactUrlTests(unittest.TestCase):
    def test_oculta_ruta_y_parametros(self):
        url = "https://docs.google.com/spreadsheets/d/ID_SECRETO/pub?gid=0&output=csv"
        self.assertEqual(_redact_url(url), "https://docs.google.com")

    def test_texto_sin_esquema(self):
        self.assertEqual(_redact_url("no-es-una-url"), "origen remoto")


if __name__ == "__main__":
    unittest.main()
