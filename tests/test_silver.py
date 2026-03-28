import importlib.util
import unittest

import pandas as pd


PYSPARK_DISPONIVEL = importlib.util.find_spec("pyspark") is not None


@unittest.skipUnless(PYSPARK_DISPONIVEL, "pyspark nao esta disponivel no ambiente atual")
class TestSilverLayerTransformation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.bronze.ingestion import SparkSessionManager

        cls.spark = SparkSessionManager.get_spark()

    @classmethod
    def tearDownClass(cls):
        from src.bronze.ingestion import SparkSessionManager

        SparkSessionManager.stop_spark()

    def setUp(self):
        from src.silver.transformation import SilverLayerTransformation

        self.silver = SilverLayerTransformation()

    def test_transform_categoria(self):
        df = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "codigo_categoria": ["1.1.01", "1.1.01", "2.2.01"],
                    "nome_categoria": [" Venda ", " Venda ", "Servico"],
                    "tipo_categoria": ["RECEITA", "RECEITA", "DESPESA"],
                }
            )
        )
        resultado = self.silver.transform_categoria(df).toPandas()

        self.assertEqual(len(resultado), 2)
        self.assertEqual(resultado.loc[0, "nome_categoria"], "Venda")
        self.assertEqual(resultado.loc[0, "tipo_categoria"], "receita")

    def test_transform_conta_receber(self):
        df = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "id_conta": [1],
                    "descricao_conta": ["Receita recorrente"],
                    "id_cliente": [10],
                    "codigo_categoria": ["1.1.01"],
                    "observacao_conta": ["Teste"],
                    "valor_conta": [1500.5],
                    "data_vencimento": ["2025-01-01"],
                    "data_pagamento": ["2025-01-02"],
                    "status": ["Pago"],
                }
            )
        )
        resultado = self.silver.transform_conta_receber(df).toPandas()

        self.assertEqual(resultado.loc[0, "tipo_conta"], "receber")
        self.assertEqual(resultado.loc[0, "status_conta"], "pago")
        self.assertEqual(float(resultado.loc[0, "valor_conta"]), 1500.5)


if __name__ == "__main__":
    unittest.main()
