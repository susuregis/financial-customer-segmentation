import os
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv


def _resolver_caminho(valor: str, base_path: Path) -> str:
    caminho = Path(valor)
    if not caminho.is_absolute():
        caminho = base_path / caminho
    return str(caminho.resolve())


load_dotenv()


class Config:
    BASE_PATH = Path(__file__).resolve().parents[2]

    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_USER = os.getenv("DB_USER", "airflow")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "airflow")
    DB_NAME = os.getenv("DB_NAME", "airflow_db")

    DATA_RAW_PATH = _resolver_caminho(
        os.getenv("DATA_RAW_PATH", "data/data_raw"),
        BASE_PATH,
    )
    DATA_BRONZE_PATH = _resolver_caminho(
        os.getenv("DATA_BRONZE_PATH", "data/bronze"),
        BASE_PATH,
    )
    DATA_SILVER_PATH = _resolver_caminho(
        os.getenv("DATA_SILVER_PATH", "data/silver"),
        BASE_PATH,
    )
    DATA_GOLD_PATH = _resolver_caminho(
        os.getenv("DATA_GOLD_PATH", "data/gold"),
        BASE_PATH,
    )

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = _resolver_caminho(
        os.getenv("LOG_FILE", "logs/app.log"),
        BASE_PATH,
    )

    BCB_COTACAO_URL = (
        "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
        "CotacaoMoedaDia(moeda=@moeda,dataCotacao=@dataCotacao)"
        "?@moeda='{moeda}'&@dataCotacao='{data}'&$top=100&$format=json"
    )
    BCB_LOOKBACK_DAYS = int(os.getenv("BCB_LOOKBACK_DAYS", "10"))
    BCB_MOEDAS = tuple(
        moeda.strip().upper()
        for moeda in os.getenv("BCB_MOEDAS", "USD,EUR,GBP,JPY,CAD,ARS").split(",")
        if moeda.strip()
    )

    RANDOM_STATE = int(os.getenv("RANDOM_STATE", "42"))
    N_CLUSTERS = int(os.getenv("N_CLUSTERS", "0"))
    MIN_CLUSTERS = int(os.getenv("MIN_CLUSTERS", "2"))
    MAX_CLUSTERS = int(os.getenv("MAX_CLUSTERS", "6"))

    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    @classmethod
    def get_config(cls) -> Dict[str, Any]:
        return {
            "database_url": cls().DATABASE_URL,
            "data_raw_path": cls.DATA_RAW_PATH,
            "data_bronze_path": cls.DATA_BRONZE_PATH,
            "data_silver_path": cls.DATA_SILVER_PATH,
            "data_gold_path": cls.DATA_GOLD_PATH,
            "bcb_moedas": list(cls.BCB_MOEDAS),
            "log_level": cls.LOG_LEVEL,
        }
