"""
DAG: init_dag -- Full bootstrap of the ETL pipeline.
-------------------------------------------------

This DAG performs a complete initial execution of the ETL pipeline, including:
- Running all extractors across all data frequencies (daily, weekly, etc.)
- Executing the entire Spark transformation logic
- Loading the transformed data into the PostgreSQL data warehouse

This DAG is meant to be run manually (schedule=None) **once** during initial deployment
or after a full reset of the database.

After this initial run, regular operation should be handled by the 'master_etl_dag'.

Tags: init_pipeline, run_all
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Resolve project path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Import extractor classes
from src.extract.extractors.global_metrics_quotes_latest_extractor import GlobalMetricsQuotesLatestExtractor
from src.extract.extractors.fear_and_greed_historical_extractor import FearAndGreedHistoricalExtractor
from src.extract.extractors.crypto_listings_latest_extractor import CryptoListingsLatestExtractor
from src.extract.extractors.fear_and_greed_latest_extractor import FearAndGreedLatestExtractor
from src.extract.extractors.crypto_categories_extractor import CryptoCategoriesExtractor
from src.extract.extractors.exchange_assets_extractor import ExchangeAssetsExtractor
from src.extract.extractors.crypto_category_extractor import CryptoCategoryExtractor
from src.extract.extractors.exchange_info_extractor import ExchangeInfoExtractor
from src.extract.extractors.exchange_map_extractor import ExchangeMapExtractor
from src.extract.extractors.crypto_info_extractor import CryptoInfoExtractor
from src.extract.extractors.crypto_map_extractor import CryptoMapExtractor


extractors = [
    ExchangeMapExtractor,
    ExchangeInfoExtractor,
    ExchangeAssetsExtractor,
    CryptoMapExtractor,
    CryptoInfoExtractor,
    CryptoListingsLatestExtractor,
    CryptoCategoryExtractor,
    CryptoCategoriesExtractor,
    GlobalMetricsQuotesLatestExtractor,
    FearAndGreedLatestExtractor,
    FearAndGreedHistoricalExtractor,
]

transfom_runner_path = f"{PROJECT_ROOT}/src/transform/orchestrators/transform_pipeline_runner.py"
load_runner_path = f"{PROJECT_ROOT}/src/load/orchestrators/load_pipeline_runner.py"
jars_path = f"{PROJECT_ROOT}/jars/postgresql-42.6.0.jar"


def run_all_extractors():
    for extractor_class in extractors:
        extractor_instance = extractor_class()
        extractor_instance.run()


with DAG(
    dag_id="init_dag",
    start_date=datetime(2025, 6, 10),
    schedule=None,  # manualley executed
    catchup=False,
    tags=["init_pipeline", "run_all"],
    description=(
        "This DAG is intended for the first and only run to initialize the entire ETL pipeline. "
        "It executes all extractors, the full transformation pipeline, and loads all available data "
        "into the data warehouse. Once this initialization is complete, the regular daily operation "
        "should be handled by the 'master_dag'."
    ),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    extract_all = PythonOperator(
        task_id="extract_all",
        python_callable=run_all_extractors,
    )

    transform_all = BashOperator(
        task_id="transform_all",
        bash_command=(f"spark-submit --packages io.delta:delta-spark_2.12:3.3.1 {transfom_runner_path} all"),
    )

    load_all = BashOperator(
        task_id="load_all",
        bash_command=(
            f"spark-submit --packages io.delta:delta-spark_2.12:3.3.1 --jars {jars_path} {load_runner_path} all"
        ),
    )

    extract_all >> transform_all >> load_all
