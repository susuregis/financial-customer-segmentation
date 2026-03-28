import os
from datetime import datetime

from pyspark.sql.functions import (
    col,
    count,
    lit,
    lower,
    regexp_replace,
    round as spark_round,
    to_date,
    trim,
    upper,
    when,
)

from src.bronze.ingestion import SparkSessionManager
from src.config.config import Config
from src.utils.logger import LoggerConfig


logger = LoggerConfig.setup_logging(__name__)


class SilverLayerTransformation:
    def __init__(self):
        self.silver_path = Config.DATA_SILVER_PATH
        os.makedirs(self.silver_path, exist_ok=True)
        self.spark = SparkSessionManager.get_spark()
        logger.info(f"Camada silver pronta em {self.silver_path}")

    def transform_meta(self, df):
        spark_df = self._garantir_dataframe(df)
        spark_df = self._normalizar_colunas(spark_df)
        spark_df = spark_df.dropDuplicates(["mes_meta"])
        spark_df = spark_df.withColumn("mes_meta", trim(col("mes_meta")))
        spark_df = spark_df.withColumn("meta", trim(col("meta")))
        spark_df = spark_df.withColumn(
            "meta_valor",
            regexp_replace(lower(col("meta")), "k", "000").cast("double"),
        )
        return self._finalizar("meta_transformed", spark_df)

    def transform_categoria(self, df):
        spark_df = self._garantir_dataframe(df)
        spark_df = self._normalizar_colunas(spark_df)
        spark_df = spark_df.dropDuplicates(["codigo_categoria"])
        spark_df = spark_df.withColumn("codigo_categoria", trim(col("codigo_categoria")))
        spark_df = spark_df.withColumn("nome_categoria", trim(col("nome_categoria")))
        spark_df = spark_df.withColumn("tipo_categoria", lower(trim(col("tipo_categoria"))))
        return self._finalizar("categoria_transformed", spark_df)

    def transform_conta_pagar(self, df):
        spark_df = self._garantir_dataframe(df)
        spark_df = self._normalizar_colunas(spark_df)
        spark_df = spark_df.dropDuplicates(["id_conta"])
        spark_df = spark_df.withColumn("descricao_conta", trim(col("descricao_conta")))
        spark_df = spark_df.withColumn(
            "destino_pagamento_conta",
            trim(col("destino_pagamento_conta")),
        )
        spark_df = spark_df.withColumn("codigo_categoria", trim(col("codigo_categoria")))
        spark_df = spark_df.withColumn("valor_conta", col("valor_conta").cast("double"))
        spark_df = spark_df.withColumn("data_vencimento", to_date(col("data_vencimento")))
        spark_df = spark_df.withColumn("data_pagamento", to_date(col("data_pagamento")))
        spark_df = spark_df.withColumn("status_conta", lower(trim(col("status_conta"))))
        spark_df = spark_df.withColumn("tipo_conta", lit("pagar"))
        return self._finalizar("conta_pagar_transformed", spark_df)

    def transform_conta_receber(self, df):
        spark_df = self._garantir_dataframe(df)
        spark_df = self._normalizar_colunas(spark_df)
        spark_df = spark_df.dropDuplicates(["id_conta"])
        spark_df = spark_df.withColumn("descricao_conta", trim(col("descricao_conta")))
        spark_df = spark_df.withColumn("id_cliente", col("id_cliente").cast("string"))
        spark_df = spark_df.withColumn("codigo_categoria", trim(col("codigo_categoria")))
        spark_df = spark_df.withColumn("valor_conta", col("valor_conta").cast("double"))
        spark_df = spark_df.withColumn("data_vencimento", to_date(col("data_vencimento")))
        spark_df = spark_df.withColumn("data_pagamento", to_date(col("data_pagamento")))
        spark_df = spark_df.withColumn("status_conta", lower(trim(col("status"))))
        spark_df = spark_df.drop("status")
        spark_df = spark_df.withColumn("tipo_conta", lit("receber"))
        return self._finalizar("conta_receber_transformed", spark_df)

    def transform_cliente(self, df):
        spark_df = self._garantir_dataframe(df)
        spark_df = self._normalizar_colunas(spark_df)
        spark_df = spark_df.dropDuplicates(["id_cliente"])
        spark_df = spark_df.withColumn("id_cliente", col("id_cliente").cast("string"))
        spark_df = spark_df.withColumn("nome_cliente", trim(col("nome_cliente")))
        spark_df = spark_df.withColumn("pais_cliente", trim(col("pais_cliente")))
        spark_df = spark_df.withColumn("pais_cliente_normalizado", upper(trim(col("pais_cliente"))))
        spark_df = spark_df.withColumn(
            "data_cadastro_cliente",
            to_date(col("data_cadastro_cliente")),
        )
        return self._finalizar("cliente_transformed", spark_df)

    def transform_exchange_rates(self, df):
        spark_df = self._garantir_dataframe(df)
        spark_df = self._normalizar_colunas(spark_df)
        spark_df = spark_df.withColumn("currency", upper(trim(col("currency"))))
        spark_df = spark_df.withColumn("date", to_date(col("date")))
        for coluna in ["buy_rate", "sell_rate", "average_rate"]:
            spark_df = spark_df.withColumn(coluna, spark_round(col(coluna).cast("double"), 4))
        spark_df = spark_df.dropDuplicates(["currency", "date"])
        return self._finalizar("exchange_rates_transformed", spark_df)

    def _garantir_dataframe(self, df):
        if hasattr(df, "toPandas"):
            return df
        return self.spark.createDataFrame(df)

    def _normalizar_colunas(self, spark_df):
        for nome_coluna in spark_df.columns:
            novo_nome = nome_coluna.strip().lower()
            if novo_nome != nome_coluna:
                spark_df = spark_df.withColumnRenamed(nome_coluna, novo_nome)
        return spark_df

    def _calcular_quality_score(self, spark_df) -> float:
        total_linhas = spark_df.count()
        if total_linhas == 0:
            return 0.0
        agregacoes = [
            count(when(col(nome_coluna).isNotNull(), 1)).alias(nome_coluna)
            for nome_coluna in spark_df.columns
        ]
        contagens = spark_df.agg(*agregacoes).first().asDict()
        celulas_preenchidas = sum(contagens.values())
        total_celulas = total_linhas * len(spark_df.columns)
        completude = (celulas_preenchidas / max(total_celulas, 1)) * 100
        distintos = spark_df.distinct().count()
        duplicidade = ((total_linhas - distintos) / max(total_linhas, 1)) * 100
        nota = completude - (duplicidade * 0.5)
        return round(min(100.0, max(0.0, nota)), 2)

    def _finalizar(self, nome_arquivo: str, spark_df):
        nota = self._calcular_quality_score(spark_df)
        spark_df = (
            spark_df.withColumn("_transformation_date", lit(datetime.now()))
            .withColumn("_data_quality_score", lit(nota))
        )
        output_path = os.path.join(self.silver_path, f"{nome_arquivo}.parquet")
        spark_df.write.mode("overwrite").parquet(output_path)
        logger.info(f"Arquivo transformado salvo em {output_path}")
        return spark_df
