import unittest

import pandas as pd

from src.ml.clustering import CustomerSegmentation


class TestCustomerSegmentation(unittest.TestCase):
    def setUp(self):
        self.segmentacao = CustomerSegmentation(n_clusters=0)

    def test_prepare_features(self):
        base = pd.DataFrame(
            [
                {
                    "id_cliente": "1",
                    "nome_cliente": "Ana",
                    "pais_cliente": "BRASIL",
                    "currency": "BRL",
                    "total_receita_brl": 1000.0,
                    "ticket_medio_brl": 100.0,
                    "quantidade_transacoes": 10,
                    "desvio_padrao_brl": 15.0,
                    "percentual_atraso": 10.0,
                    "percentual_pago": 90.0,
                    "dias_ativos": 120,
                }
            ]
        )

        resultado = self.segmentacao.prepare_features(base)

        self.assertIn("total_receita_brl", resultado.columns)
        self.assertIn("id_cliente", resultado.columns)

    def test_run_segmentation(self):
        base = pd.DataFrame(
            [
                {
                    "id_cliente": "1",
                    "nome_cliente": "Ana",
                    "pais_cliente": "BRASIL",
                    "currency": "BRL",
                    "total_receita_brl": 1000.0,
                    "ticket_medio_brl": 100.0,
                    "quantidade_transacoes": 10,
                    "desvio_padrao_brl": 15.0,
                    "percentual_atraso": 10.0,
                    "percentual_pago": 90.0,
                    "dias_ativos": 120,
                },
                {
                    "id_cliente": "2",
                    "nome_cliente": "Bruno",
                    "pais_cliente": "ESTADOS UNIDOS",
                    "currency": "USD",
                    "total_receita_brl": 5000.0,
                    "ticket_medio_brl": 450.0,
                    "quantidade_transacoes": 12,
                    "desvio_padrao_brl": 50.0,
                    "percentual_atraso": 5.0,
                    "percentual_pago": 95.0,
                    "dias_ativos": 200,
                },
                {
                    "id_cliente": "3",
                    "nome_cliente": "Carla",
                    "pais_cliente": "PORTUGAL",
                    "currency": "EUR",
                    "total_receita_brl": 3000.0,
                    "ticket_medio_brl": 300.0,
                    "quantidade_transacoes": 9,
                    "desvio_padrao_brl": 20.0,
                    "percentual_atraso": 12.0,
                    "percentual_pago": 88.0,
                    "dias_ativos": 180,
                },
            ]
        )

        resultado = self.segmentacao.run_segmentation(base)

        self.assertIn("cluster", resultado.columns)
        self.assertEqual(len(resultado), 3)


if __name__ == "__main__":
    unittest.main()
