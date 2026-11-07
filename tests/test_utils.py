"""Tests de las utilidades transversales (sin red)."""
from __future__ import annotations

import logging
import os
import unittest
from unittest import mock

from src.utils import (
    json_dumps_canonical,
    parse_date,
    parse_float,
    parse_fundsquare_date_ddmmyyyy,
    setup_logging,
)


class ParseFloatTests(unittest.TestCase):
    def test_formato_europeo_con_miles(self):
        self.assertAlmostEqual(parse_float("1.234,56"), 1234.56)

    def test_formato_anglosajon(self):
        self.assertAlmostEqual(parse_float("35,450.82"), 35450.82)

    def test_coma_decimal_simple(self):
        self.assertAlmostEqual(parse_float("363,20"), 363.20)

    def test_simbolos_espacios_y_nbsp(self):
        self.assertAlmostEqual(parse_float(" 176,540000 €\xa0"), 176.54)

    def test_texto_no_numerico_falla(self):
        with self.assertRaises(ValueError):
            parse_float("—")


class ParseDateTests(unittest.TestCase):
    def test_barra(self):
        self.assertEqual(parse_date("31/12/2025"), "2025-12-31")

    def test_guion(self):
        self.assertEqual(parse_date("05-11-2026"), "2026-11-05")

    def test_punto(self):
        self.assertEqual(parse_date("20.02.2026"), "2026-02-20")

    def test_ocho_digitos(self):
        self.assertEqual(parse_date("05112026"), "2026-11-05")

    def test_sin_ceros_a_la_izquierda(self):
        self.assertEqual(parse_date("5-4-2026"), "2026-04-05")

    def test_wrapper_fundsquare(self):
        self.assertEqual(parse_fundsquare_date_ddmmyyyy("01/02/2026"), "2026-02-01")

    def test_formato_desconocido_falla(self):
        with self.assertRaises(ValueError):
            parse_date("sin fecha")


class JsonCanonicalTests(unittest.TestCase):
    def test_claves_ordenadas_y_sin_escapar_acentos(self):
        texto = json_dumps_canonical({"b": 1, "a": "ñ"})
        self.assertLess(texto.index('"a"'), texto.index('"b"'))
        self.assertIn("ñ", texto)


class SetupLoggingTests(unittest.TestCase):
    def test_http_loggers_no_bajan_de_warning_ni_con_debug(self):
        root = logging.getLogger()
        niveles = {n: logging.getLogger(n).level for n in ("urllib3", "requests")}
        self.addCleanup(lambda: [logging.getLogger(n).setLevel(v) for n, v in niveles.items()])
        self.addCleanup(root.setLevel, root.level)

        with mock.patch.dict(os.environ, {"LOGLEVEL": "DEBUG"}):
            setup_logging()

        # El escenario peligroso está activo: el log raíz sí es DEBUG...
        self.assertEqual(root.level, logging.DEBUG)
        # ...pero las librerías HTTP no llegan a registrar la URL de la petición.
        self.assertEqual(logging.getLogger("urllib3").level, logging.WARNING)
        self.assertEqual(logging.getLogger("requests").level, logging.WARNING)


if __name__ == "__main__":
    unittest.main()
