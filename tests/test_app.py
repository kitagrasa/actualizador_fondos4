"""Tests del orquestador: fusión de series y salvaguarda de borrado."""
from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from unittest import mock

import src.app as app


class MergeUpdatesTests(unittest.TestCase):
    def test_prioridad_gana_la_ultima_fuente(self):
        existente = {"2026-01-01": 1.0}
        resultado = app.merge_updates(
            existente,
            [("2026-01-01", 2.0)],
            [{"date": "2026-01-02", "close": 3.0}],
        )
        self.assertEqual(resultado, {"2026-01-01": 2.0, "2026-01-02": 3.0})

    def test_ignora_fuentes_vacias_y_precios_nulos(self):
        resultado = app.merge_updates(
            {},
            None,
            [],
            [("2026-01-03", None)],
            [("2026-01-04", 4.5)],
        )
        self.assertEqual(resultado, {"2026-01-04": 4.5})

    def test_no_modifica_el_diccionario_original(self):
        existente = {"2026-01-01": 1.0}
        app.merge_updates(existente, [("2026-01-01", 9.0)])
        self.assertEqual(existente, {"2026-01-01": 1.0})

    def test_fecha_maxima(self):
        self.assertEqual(
            app.max_existing_date({"2026-01-01": 1.0, "2026-03-01": 2.0}),
            date(2026, 3, 1),
        )
        self.assertIsNone(app.max_existing_date({}))


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.prices = Path(self._tmp.name) / "prices"
        self.prices.mkdir(parents=True)
        parche = mock.patch.object(app, "PRICES_DIR", self.prices)
        parche.start()
        self.addCleanup(parche.stop)

    def _crear(self, cuantos: int) -> list:
        isines = [f"ISIN{i:03d}" for i in range(cuantos)]
        for isin in isines:
            (self.prices / isin).write_text("<table></table>", encoding="utf-8")
        return isines

    def _meta(self, isines) -> dict:
        return {"funds": {isin: {"name": isin} for isin in isines}}

    def test_borrado_normal(self):
        isines = self._crear(10)
        meta = self._meta(isines)
        self.assertTrue(app.cleanup_removed_funds(isines[:9], meta))
        self.assertEqual(len(list(self.prices.iterdir())), 9)
        self.assertEqual(list(meta["funds"]), isines[:9])

    def test_aborta_si_el_csv_parece_incompleto(self):
        isines = self._crear(10)
        meta = self._meta(isines)
        with self.assertRaises(app.CleanupAborted):
            app.cleanup_removed_funds(isines[:3], meta)
        self.assertEqual(len(list(self.prices.iterdir())), 10)
        self.assertEqual(len(meta["funds"]), 10)

    def test_allow_mass_delete_fuerza_la_limpieza(self):
        isines = self._crear(10)
        meta = self._meta(isines)
        self.assertTrue(app.cleanup_removed_funds(isines[:3], meta, allow_mass_delete=True))
        self.assertEqual(len(list(self.prices.iterdir())), 3)

    def test_sin_cambios_devuelve_false(self):
        isines = self._crear(4)
        self.assertFalse(app.cleanup_removed_funds(isines, self._meta(isines)))

    def test_sin_historicos_no_borra_ni_aborta(self):
        self.assertFalse(app.cleanup_removed_funds(["ISIN000"], {"funds": {}}))


if __name__ == "__main__":
    unittest.main()
