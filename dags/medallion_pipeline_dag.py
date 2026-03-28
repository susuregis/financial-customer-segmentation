from datetime import datetime, timedelta
import importlib
import sys

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


sys.path.insert(0, "/opt/airflow/dags")

from main import MedallionPipeline
from src.bronze.ingestion import SparkSessionManager
from src.utils.logger import LoggerConfig


logger = LoggerConfig.setup_logging(__name__)


default_args = {
    "owner": "ray_consulting",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    dag_id="medallion_pipeline",
    default_args=default_args,
    description="Pipeline medalhao com Bronze, Silver, Gold e ML",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "medallion", "ray"],
)


def _executar_etapa(nome_etapa: str):
    pipeline = MedallionPipeline()
    try:
        if nome_etapa == "bronze":
            pipeline.executar_bronze()
        elif nome_etapa == "silver":
            pipeline.executar_silver()
        elif nome_etapa == "gold":
            pipeline.executar_gold()
        elif nome_etapa == "ml":
            pipeline.executar_ml()
        elif nome_etapa == "validacao":
            pipeline.validar_saidas()
        else:
            raise AirflowException(f"Etapa desconhecida: {nome_etapa}")
    finally:
        SparkSessionManager.stop_spark()


def validar_ambiente():
    pacotes = [
        "pandas",
        "sqlalchemy",
        "requests",
        "sklearn",
        "openpyxl",
        "pyspark",
    ]
    faltantes = [pacote for pacote in pacotes if importlib.util.find_spec(pacote) is None]
    if faltantes:
        raise AirflowException(f"Pacotes obrigatorios ausentes: {', '.join(faltantes)}")
    logger.info("Ambiente validado com sucesso")


def executar_bronze():
    _executar_etapa("bronze")


def executar_silver():
    _executar_etapa("silver")


def executar_gold():
    _executar_etapa("gold")


def executar_ml():
    _executar_etapa("ml")


def validar_outputs():
    _executar_etapa("validacao")


validar_task = PythonOperator(
    task_id="validar_ambiente",
    python_callable=validar_ambiente,
    dag=dag,
)

bronze_task = PythonOperator(
    task_id="executar_bronze",
    python_callable=executar_bronze,
    dag=dag,
)

silver_task = PythonOperator(
    task_id="executar_silver",
    python_callable=executar_silver,
    dag=dag,
)

gold_task = PythonOperator(
    task_id="executar_gold",
    python_callable=executar_gold,
    dag=dag,
)

ml_task = PythonOperator(
    task_id="executar_ml",
    python_callable=executar_ml,
    dag=dag,
)

validar_outputs_task = PythonOperator(
    task_id="validar_outputs",
    python_callable=validar_outputs,
    dag=dag,
)


validar_task >> bronze_task >> silver_task >> gold_task >> ml_task >> validar_outputs_task
