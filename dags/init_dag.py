"""
DAG: init_dag -- Full bootstrap of the ELT pipeline.
-------------------------------------------------

This DAG performs a complete initial execution of the ELT pipeline, including:
- Running all extractors across all data frequencies (daily, weekly, etc.)
- Executing the entire Spark transformation logic
- Loading the transformed data into the PostgreSQL data warehouse

This DAG is meant to be run manually (schedule=None) **once** during initial deployment
or after a full reset of the database.

After this initial run, regular operation should be handled by the 'master_etl_dag'.

Tags: init_pipeline, run_all
"""

from dag_utils import TRANSFORM_RUNNER, LOAD_RUNNER, POSTGRES_JARS, SPARK_DELTA_OPT, make_callable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
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

transform_bash_command = f"spark-submit {SPARK_DELTA_OPT} {TRANSFORM_RUNNER}" + " all"
load_bash_command = f"spark-submit {SPARK_DELTA_OPT} {POSTGRES_JARS} {LOAD_RUNNER}" + " all"


with DAG(
    dag_id="init_dag",
    start_date=datetime(2025, 6, 21),
    schedule=None,  # manualley executed
    catchup=False,
    tags=["init_pipeline", "run_all"],
    description=(
        "This DAG is intended for the first and only run to initialize the entire ELT pipeline. "
        "It executes all extractors, the full transformation pipeline, and loads all available data "
        "into the data warehouse. Once this initialization is complete, the regular daily operation "
        "should be handled by the 'master_dag'."
    ),
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    # Extract task group
    with TaskGroup("extract_all", dag=dag) as extract_all:
        tasks = []
        for extractor in extractors:
            tasks.append(
                PythonOperator(
                    task_id=f"extract_{extractor.__name__}",
                    python_callable=make_callable(extractor),
                )
            )
        # Serialize extraction tasks to respect API rate limits (avoid parallel API calls)
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]

    transform_all = BashOperator(task_id="transform_all", bash_command=transform_bash_command)

    load_all = BashOperator(task_id="load_all", bash_command=load_bash_command)

    extract_all >> transform_all >> load_all
