import importlib.util
import json
import os
import shutil
import tempfile
import unittest

import pandas as pd


PYSPARK_DISPONIVEL = importlib.util.find_spec("pyspark") is not None


@unittest.skipUnless(PYSPARK_DISPONIVEL, "pyspark nao esta disponivel no ambiente atual")
class TestBronzeLayerIngestion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.bronze.ingestion import SparkSessionManager

        cls.spark = SparkSessionManager.get_spark()

    @classmethod
    def tearDownClass(cls):
        from src.bronze.ingestion import SparkSessionManager

        SparkSessionManager.stop_spark()

    def setUp(self):
        from src.bronze.ingestion import BronzeLayerIngestion

        self.bronze = BronzeLayerIngestion()
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_ingest_csv(self):
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "valor": [10.0, 20.0, 30.0],
                "moeda": ["USD", "EUR", "GBP"],
            }
        )
        caminho = os.path.join(self.temp_dir, "teste.csv")
        df.to_csv(caminho, index=False)

        resultado = self.bronze.ingest_csv(caminho, "teste_csv")

        self.assertEqual(resultado.count(), 3)
        self.assertIn("_ingestion_date", resultado.columns)
        self.assertIn("_source", resultado.columns)
        self.assertIn("_raw_file_path", resultado.columns)

    def test_ingest_json(self):
        caminho = os.path.join(self.temp_dir, "teste.json")
        with open(caminho, "w", encoding="utf-8") as arquivo:
            json.dump(
                [
                    {"id_cliente": 1, "nome_cliente": "Cliente 1"},
                    {"id_cliente": 2, "nome_cliente": "Cliente 2"},
                ],
                arquivo,
                ensure_ascii=False,
            )

        resultado = self.bronze.ingest_json(caminho, "teste_json")

        self.assertEqual(resultado.count(), 2)
        self.assertIn("_source", resultado.columns)


if __name__ == "__main__":
    unittest.main()
