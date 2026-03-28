import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit
    from pyspark.sql.types import TimestampType

    PYSPARK_DISPONIVEL = True
except ImportError:
    SparkSession = None
    lit = None
    TimestampType = None
    PYSPARK_DISPONIVEL = False

from src.config.config import Config
from src.utils.logger import LoggerConfig


logger = LoggerConfig.setup_logging(__name__)


class SparkSessionManager:
    _session = None

    @classmethod
    def get_spark(cls):
        if not PYSPARK_DISPONIVEL:
            logger.info("PySpark nao esta disponivel. O pipeline usara modo pandas")
            return None
        if cls._session is None:
            cls._session = (
                SparkSession.builder.appName("PipelineMedallion")
                .master(Config.SPARK_MASTER)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate()
            )
            logger.info("SparkSession iniciada")
        return cls._session

    @classmethod
    def stop_spark(cls):
        if cls._session is not None:
            cls._session.stop()
            cls._session = None
            logger.info("SparkSession encerrada")


class BronzeLayerIngestion:
    def __init__(self):
        self.bronze_path = Path(Config.DATA_BRONZE_PATH)
        self.bronze_path.mkdir(parents=True, exist_ok=True)
        self.spark = SparkSessionManager.get_spark()
        logger.info(f"Camada bronze pronta em {self.bronze_path}")

    def ingest_csv(self, file_path: str, source_name: str):
        logger.info(f"Iniciando ingestao do arquivo CSV {file_path}")
        if self.spark is not None:
            df = self.spark.read.csv(file_path, header=True, inferSchema=True)
        else:
            df = pd.read_csv(file_path)
        return self._adicionar_metadados_e_salvar(df, source_name, file_path)

    def ingest_excel(self, file_path: str, source_name: str, sheet_name=0):
        logger.info(f"Iniciando ingestao do arquivo Excel {file_path}")
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        if self.spark is not None:
            df = self.spark.createDataFrame(df)
        return self._adicionar_metadados_e_salvar(df, source_name, file_path)

    def ingest_json(self, file_path: str, source_name: str):
        logger.info(f"Iniciando ingestao do arquivo JSON {file_path}")
        with open(file_path, "r", encoding="utf-8") as arquivo:
            conteudo = json.load(arquivo)
        if not isinstance(conteudo, list):
            conteudo = [conteudo]
        if self.spark is not None:
            df = self.spark.createDataFrame(conteudo)
        else:
            df = pd.DataFrame(conteudo)
        return self._adicionar_metadados_e_salvar(df, source_name, file_path)

    def ingest_bcb_api(self, currencies=None):
        moedas = currencies or Config.BCB_MOEDAS
        registros = []
        for moeda in moedas:
            cotacao = self._buscar_cotacao_mais_recente(moeda)
            if cotacao is not None:
                registros.append(cotacao)
            else:
                logger.warning(f"Nao foi encontrada cotacao recente para {moeda}")

        if not registros:
            cache = self._carregar_cache_bcb()
            if cache is not None and not cache.empty:
                logger.warning("Usando arquivo de cotacao ja existente na camada bronze")
                return cache
            raise RuntimeError("Nao foi possivel obter cotacoes validas do Banco Central")

        df = pd.DataFrame(registros)
        if self.spark is not None:
            df = self.spark.createDataFrame(df)
        output_path = self.bronze_path / "bcb_exchange_rates_raw.parquet"
        self._salvar_parquet(df, output_path)
        logger.info(f"Cotacoes do Banco Central salvas em {output_path}")
        return df

    def _buscar_cotacao_mais_recente(self, moeda: str) -> Optional[Dict]:
        for deslocamento in range(Config.BCB_LOOKBACK_DAYS):
            data_referencia = datetime.now().date() - timedelta(days=deslocamento)
            data_bcb = data_referencia.strftime("%m-%d-%Y")
            url = Config.BCB_COTACAO_URL.format(moeda=moeda, data=data_bcb)
            try:
                resposta = requests.get(url, timeout=30)
                resposta.raise_for_status()
                payload = resposta.json()
                valores = payload.get("value", [])
                if not valores:
                    continue
                ultimo_registro = valores[-1]
                compra = float(ultimo_registro["cotacaoCompra"])
                venda = float(ultimo_registro["cotacaoVenda"])
                return {
                    "currency": moeda,
                    "date": data_referencia.isoformat(),
                    "buy_rate": compra,
                    "sell_rate": venda,
                    "average_rate": round((compra + venda) / 2, 4),
                    "_ingestion_date": datetime.now().isoformat(),
                    "_source": "bcb_ptax",
                }
            except requests.RequestException as erro:
                logger.warning(f"Falha ao consultar o Banco Central para {moeda}: {erro}")
        return None

    def _carregar_cache_bcb(self):
        caminho = self.bronze_path / "bcb_exchange_rates_raw.parquet"
        if not caminho.exists():
            return None
        if self.spark is not None:
            return self.spark.read.parquet(str(caminho))
        return pd.read_parquet(caminho)

    def _adicionar_metadados_e_salvar(self, df, source_name: str, file_path: str):
        output_path = self.bronze_path / f"{source_name}_raw.parquet"
        if self.spark is not None and hasattr(df, "withColumn"):
            df = (
                df.withColumn("_ingestion_date", lit(datetime.now()).cast(TimestampType()))
                .withColumn("_source", lit(source_name))
                .withColumn("_raw_file_path", lit(str(file_path)))
            )
        else:
            df = df.copy()
            df["_ingestion_date"] = datetime.now()
            df["_source"] = source_name
            df["_raw_file_path"] = str(file_path)
        self._salvar_parquet(df, output_path)
        logger.info(f"Arquivo bruto salvo em {output_path}")
        return df

    def _salvar_parquet(self, df, output_path: Path):
        if self.spark is not None and hasattr(df, "write"):
            df.write.mode("overwrite").parquet(str(output_path))
        else:
            df.to_parquet(output_path, index=False)
