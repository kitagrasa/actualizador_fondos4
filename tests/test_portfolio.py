"""Tests de persistencia de precios en tabla HTML (sin red)."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.portfolio import read_prices, write_prices_if_changed


class PortfolioTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = Path(self._tmp.name) / "prices" / "TEST"

    def test_ida_y_vuelta(self):
        datos = {"2026-11-05": 14.24, "2026-11-04": 14.2}
        self.assertTrue(write_prices_if_changed(self.path, datos))
        self.assertEqual(read_prices(self.path), datos)

    def test_formato_del_archivo(self):
        write_prices_if_changed(self.path, {"2026-11-05": 14.2400004, "2026-11-04": 14.2})
        texto = self.path.read_text(encoding="utf-8")
        self.assertIn("<th>Date</th><th>Close</th>", texto)
        # Fechas descendentes, ISO, punto decimal, sin ceros sobrantes.
        self.assertLess(texto.index("2026-11-05"), texto.index("2026-11-04"))
        self.assertIn("<td>2026-11-05</td><td>14.24</td>", texto)
        self.assertIn("<td>2026-11-04</td><td>14.2</td>", texto)

    def test_tabla_valida_para_portfolio_performance(self):
        write_prices_if_changed(self.path, {"2026-11-05": 14.24})
        texto = self.path.read_text(encoding="utf-8")
        self.assertIn("<thead>", texto)
        self.assertIn("<tbody>", texto)
        self.assertEqual(texto.count("<th>"), 2)
        self.assertIn('lang="en"', texto)

    def test_sin_cambios_no_reescribe(self):
        datos = {"2026-11-05": 14.24}
        write_prices_if_changed(self.path, datos)
        self.assertFalse(write_prices_if_changed(self.path, datos))

    def test_ignora_filas_malformadas(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            "<table><thead><tr><th>Date</th><th>Close</th></tr></thead><tbody>"
            "<tr><td>2026-01-02</td><td>37.50</td></tr>"
            "<tr><td>sin fecha</td><td>1</td></tr>"
            "<tr><td>2026-01-03</td><td>no es un número</td></tr>"
            "<tr><td>2026-01-04</td></tr>"
            "</tbody></table>",
            encoding="utf-8",
        )
        self.assertEqual(read_prices(self.path), {"2026-01-02": 37.50})

    def test_archivo_inexistente_devuelve_vacio(self):
        self.assertEqual(read_prices(self.path), {})


if __name__ == "__main__":
    unittest.main()
