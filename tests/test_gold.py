import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.gold.aggregation import GoldLayerAggregation
from src.utils.database import DatabaseManager


class TestGoldLayerAggregation(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_manager = DatabaseManager("sqlite:///:memory:")
        self.db_manager.create_all_tables()
        self.gold = GoldLayerAggregation(self.db_manager)
        self.gold.gold_path = Path(self.temp_dir)

    def tearDown(self):
        self.db_manager.close()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_create_dimensions(self):
        clientes = pd.DataFrame(
            [{"id_cliente": "1", "nome_cliente": "Ana", "pais_cliente": "Brasil"}]
        )
        categorias = pd.DataFrame(
            [{"codigo_categoria": "1.1.01", "nome_categoria": "Venda", "tipo_categoria": "receita"}]
        )
        taxas = pd.DataFrame(
            [{"currency": "USD", "date": "2025-01-01", "buy_rate": 5.0, "sell_rate": 5.1, "average_rate": 5.05}]
        )

        mappings = self.gold.create_dimensions(clientes, categorias, taxas)

        self.assertIn("1", mappings["customer"])
        self.assertIn("1.1.01", mappings["category"])
        self.assertIn("BRL", mappings["currency"])

    def test_create_aggregated_views(self):
        transacoes = pd.DataFrame(
            [
                {
                    "id_conta": "1",
                    "tipo_conta": "receber",
                    "id_cliente": "1",
                    "nome_cliente": "Ana",
                    "pais_cliente": "BRASIL",
                    "codigo_categoria": "1.1.01",
                    "nome_categoria": "Venda",
                    "status_conta": "pago",
                    "currency": "BRL",
                    "taxa_cambio": 1.0,
                    "valor_original": 100.0,
                    "valor_brl": 100.0,
                    "data_referencia": "2025-01-01",
                }
            ]
        )

        self.gold.create_aggregated_views(transacoes)

        self.assertTrue((self.gold.gold_path / "customer_summary.parquet").exists())
        self.assertTrue((self.gold.gold_path / "category_summary.parquet").exists())
        self.assertTrue((self.gold.gold_path / "monthly_summary.parquet").exists())


if __name__ == "__main__":
    unittest.main()
