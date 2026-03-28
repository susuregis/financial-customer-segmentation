import sys
from pathlib import Path
from typing import Dict

import pandas as pd

from src.bronze.ingestion import BronzeLayerIngestion, SparkSessionManager
from src.config.config import Config
from src.gold.aggregation import GoldLayerAggregation
from src.ml.clustering import CustomerSegmentation
from src.silver.transformation import SilverLayerTransformation
from src.utils.database import DatabaseManager
from src.utils.logger import LoggerConfig


logger = LoggerConfig.setup_logging(__name__)


class MedallionPipeline:
    def __init__(self, input_data_path: str = None):
        self.config = Config
        self.input_path = Path(input_data_path or Config.DATA_RAW_PATH)
        self.db_manager = DatabaseManager()
        self.bronze_layer = BronzeLayerIngestion()
        self.silver_layer = SilverLayerTransformation()
        self.gold_layer = GoldLayerAggregation(self.db_manager)
        self.spark = SparkSessionManager.get_spark()

    def executar_bronze(self) -> Dict:
        logger.info(f"Iniciando camada bronze com arquivos em {self.input_path}")
        arquivos = {
            "meta": "PS_Meta2025.csv",
            "categoria": "PS_Categoria.csv",
            "conta_pagar": "PS_Conta_Pagar.xlsx",
            "conta_receber": "PS_Conta_Receber.xlsx",
            "cliente": "PS_Cliente.json",
        }
        faltantes = [
            nome_arquivo
            for nome_arquivo in arquivos.values()
            if not (self.input_path / nome_arquivo).exists()
        ]
        if faltantes:
            raise FileNotFoundError(f"Arquivos obrigatorios ausentes: {', '.join(faltantes)}")

        raw_data = {
            "meta": self.bronze_layer.ingest_csv(str(self.input_path / arquivos["meta"]), "meta"),
            "categoria": self.bronze_layer.ingest_csv(str(self.input_path / arquivos["categoria"]), "categoria"),
            "conta_pagar": self.bronze_layer.ingest_excel(
                str(self.input_path / arquivos["conta_pagar"]),
                "conta_pagar",
            ),
            "conta_receber": self.bronze_layer.ingest_excel(
                str(self.input_path / arquivos["conta_receber"]),
                "conta_receber",
            ),
            "cliente": self.bronze_layer.ingest_json(
                str(self.input_path / arquivos["cliente"]),
                "cliente",
            ),
            "exchange_rates": self.bronze_layer.ingest_bcb_api(),
        }
        logger.info("Camada bronze concluida")
        return raw_data

    def executar_silver(self, raw_data: Dict = None) -> Dict:
        logger.info("Iniciando camada silver")
        dados = raw_data or self._carregar_bronze()
        transformados = {
            "meta": self.silver_layer.transform_meta(dados["meta"]),
            "categoria": self.silver_layer.transform_categoria(dados["categoria"]),
            "conta_pagar": self.silver_layer.transform_conta_pagar(dados["conta_pagar"]),
            "conta_receber": self.silver_layer.transform_conta_receber(dados["conta_receber"]),
            "cliente": self.silver_layer.transform_cliente(dados["cliente"]),
            "exchange_rates": self.silver_layer.transform_exchange_rates(dados["exchange_rates"]),
        }
        logger.info("Camada silver concluida")
        return transformados

    def executar_gold(self, transformed_data: Dict = None) -> Dict:
        logger.info("Iniciando camada gold")
        dados = transformed_data or self._carregar_silver()
        self.db_manager.create_all_tables()
        inicio, fim = self._obter_intervalo_datas(dados)
        self.gold_layer.create_date_dimension(inicio, fim)
        mappings = self.gold_layer.create_dimensions(
            dados["cliente"],
            dados["categoria"],
            dados["exchange_rates"],
        )
        transacoes = self.gold_layer.load_transactions_to_fact_table(
            dados["conta_pagar"],
            dados["conta_receber"],
            mappings,
            dados["exchange_rates"],
            dados["cliente"],
            dados["categoria"],
        )
        self.gold_layer.create_aggregated_views(transacoes)
        logger.info("Camada gold concluida")
        return {"mappings": mappings, "transactions": transacoes}

    def executar_ml(self, customer_summary_df: pd.DataFrame = None) -> pd.DataFrame:
        logger.info("Iniciando etapa de machine learning")
        if customer_summary_df is None:
            caminho = Path(Config.DATA_GOLD_PATH) / "customer_summary.parquet"
            if not caminho.exists():
                raise FileNotFoundError("Arquivo customer_summary.parquet nao encontrado na camada gold")
            customer_summary_df = pd.read_parquet(caminho)

        segmentacao = CustomerSegmentation(n_clusters=Config.N_CLUSTERS)
        segmentado = segmentacao.run_segmentation(customer_summary_df)
        if not segmentado.empty:
            self.gold_layer.update_customer_clusters(segmentado)
        logger.info("Etapa de machine learning concluida")
        return segmentado

    def validar_saidas(self):
        arquivos_esperados = [
            Path(Config.DATA_BRONZE_PATH) / "meta_raw.parquet",
            Path(Config.DATA_BRONZE_PATH) / "categoria_raw.parquet",
            Path(Config.DATA_BRONZE_PATH) / "conta_pagar_raw.parquet",
            Path(Config.DATA_BRONZE_PATH) / "conta_receber_raw.parquet",
            Path(Config.DATA_BRONZE_PATH) / "cliente_raw.parquet",
            Path(Config.DATA_BRONZE_PATH) / "bcb_exchange_rates_raw.parquet",
            Path(Config.DATA_SILVER_PATH) / "meta_transformed.parquet",
            Path(Config.DATA_SILVER_PATH) / "categoria_transformed.parquet",
            Path(Config.DATA_SILVER_PATH) / "conta_pagar_transformed.parquet",
            Path(Config.DATA_SILVER_PATH) / "conta_receber_transformed.parquet",
            Path(Config.DATA_SILVER_PATH) / "cliente_transformed.parquet",
            Path(Config.DATA_SILVER_PATH) / "exchange_rates_transformed.parquet",
            Path(Config.DATA_GOLD_PATH) / "transactions_enriched.parquet",
            Path(Config.DATA_GOLD_PATH) / "customer_summary.parquet",
            Path(Config.DATA_GOLD_PATH) / "category_summary.parquet",
            Path(Config.DATA_GOLD_PATH) / "monthly_summary.parquet",
            Path(Config.DATA_GOLD_PATH) / "ml_results" / "customer_segments.parquet",
        ]
        faltantes = [str(caminho) for caminho in arquivos_esperados if not caminho.exists()]
        if faltantes:
            raise FileNotFoundError(f"Saidas obrigatorias ausentes: {', '.join(faltantes)}")
        logger.info("Todas as saidas obrigatorias foram geradas")

    def run(self):
        raw_data = self.executar_bronze()
        silver_data = self.executar_silver(raw_data)
        self.executar_gold(silver_data)
        customer_summary = pd.read_parquet(Path(Config.DATA_GOLD_PATH) / "customer_summary.parquet")
        self.executar_ml(customer_summary)
        self.validar_saidas()
        logger.info("Pipeline completo finalizado com sucesso")

    def _carregar_bronze(self) -> Dict:
        return {
            "meta": self.spark.read.parquet(str(Path(Config.DATA_BRONZE_PATH) / "meta_raw.parquet")),
            "categoria": self.spark.read.parquet(str(Path(Config.DATA_BRONZE_PATH) / "categoria_raw.parquet")),
            "conta_pagar": self.spark.read.parquet(str(Path(Config.DATA_BRONZE_PATH) / "conta_pagar_raw.parquet")),
            "conta_receber": self.spark.read.parquet(str(Path(Config.DATA_BRONZE_PATH) / "conta_receber_raw.parquet")),
            "cliente": self.spark.read.parquet(str(Path(Config.DATA_BRONZE_PATH) / "cliente_raw.parquet")),
            "exchange_rates": self.spark.read.parquet(
                str(Path(Config.DATA_BRONZE_PATH) / "bcb_exchange_rates_raw.parquet")
            ),
        }

    def _carregar_silver(self) -> Dict:
        return {
            "meta": self.spark.read.parquet(str(Path(Config.DATA_SILVER_PATH) / "meta_transformed.parquet")),
            "categoria": self.spark.read.parquet(str(Path(Config.DATA_SILVER_PATH) / "categoria_transformed.parquet")),
            "conta_pagar": self.spark.read.parquet(
                str(Path(Config.DATA_SILVER_PATH) / "conta_pagar_transformed.parquet")
            ),
            "conta_receber": self.spark.read.parquet(
                str(Path(Config.DATA_SILVER_PATH) / "conta_receber_transformed.parquet")
            ),
            "cliente": self.spark.read.parquet(str(Path(Config.DATA_SILVER_PATH) / "cliente_transformed.parquet")),
            "exchange_rates": self.spark.read.parquet(
                str(Path(Config.DATA_SILVER_PATH) / "exchange_rates_transformed.parquet")
            ),
        }

    def _obter_intervalo_datas(self, dados: Dict) -> tuple[str, str]:
        pagar = self._to_pandas_safe(dados["conta_pagar"])
        receber = self._to_pandas_safe(dados["conta_receber"])
        datas = pd.concat(
            [
                pd.to_datetime(pagar["data_vencimento"], errors="coerce"),
                pd.to_datetime(pagar["data_pagamento"], errors="coerce"),
                pd.to_datetime(receber["data_vencimento"], errors="coerce"),
                pd.to_datetime(receber["data_pagamento"], errors="coerce"),
            ],
            ignore_index=True,
        ).dropna()
        if datas.empty:
            hoje = pd.Timestamp.today().date().isoformat()
            return hoje, hoje
        return datas.min().date().isoformat(), datas.max().date().isoformat()

    def _to_pandas_safe(self, df) -> pd.DataFrame:
        if hasattr(df, "toPandas"):
            expressoes = []
            for nome_coluna, tipo_coluna in df.dtypes:
                if tipo_coluna.startswith("date") or tipo_coluna.startswith("timestamp"):
                    expressoes.append(f"CAST({nome_coluna} AS STRING) AS {nome_coluna}")
                else:
                    expressoes.append(nome_coluna)
            return df.selectExpr(*expressoes).toPandas()
        if isinstance(df, pd.DataFrame):
            return df.copy()
        return pd.DataFrame(df)


def main():
    pipeline = MedallionPipeline()
    try:
        pipeline.run()
        return 0
    except Exception as erro:
        logger.error(f"Falha na execucao do pipeline: {erro}")
        return 1
    finally:
        SparkSessionManager.stop_spark()


if __name__ == "__main__":
    sys.exit(main())
