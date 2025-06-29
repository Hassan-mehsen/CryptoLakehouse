"""
Daily ELT DAG - Orchestration of the crypto data pipeline for the "daily" frequency.

This DAG automates the full ELT pipeline once on weekdays at 8:30 AM. (UTC). It extracts, transforms, loads,
and post-processes the daily data snapshots powering the Data Warehouse.

----------------------------------------------------
       Architecture & Execution Strategy
----------------------------------------------------
- Pipeline phases: Extraction, Transformation, Load, Post-Load SQL :
    - Extraction: Each extractor (API/endpoint) runs **sequentially** (serialized via PythonOperators)
      to respect API rate limits and ensure deterministic order. All raw data is loaded into the data lake "bronze" layer.
    - Transformation: A single Spark job orchestrates the entire daily transformation logic and loads
      transformed data into the data lake "silver" layer.
    - Load: A single Spark job loads the transformed data into the Data Warehouse.
    - Post-Load SQL: Runs data quality SQL scripts to fix missing fields and perform daily cleaning
      after load (guaranteeing data consistency and freshness).

----------------------------------------------------
         Orchestration Highlights
----------------------------------------------------
- TaskGroups structure each phase for clarity, traceability, and robust monitoring.
- Extraction is always serialized to avoid API quota overruns and ensure reliability.
- Only the daily pipeline triggers the Post-Load data quality and cleaning phase.
- The orchestration is optimized for deterministic runs, efficient Spark utilization,
  and minimal operational latency.
- Due to the high-level orchestration, **the Spark driver is launched only once per phase**,
  optimizing resource usage and minimizing Spark-driver startup latency.

----------------------------------------------------
         Operations & Usage
----------------------------------------------------
- Compatible with any Airflow executor (Local, Celery, Kubernetes).
- No pool required; each phase (transform/load) launches only one global Spark job.
- Designed for production environments requiring daily data freshness and high data quality.
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Resolve project path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Import extractor classes
from src.extract.extractors.exchange_assets_extractor import ExchangeAssetsExtractor
from src.extract.extractors.crypto_category_extractor import CryptoCategoryExtractor
from src.extract.extractors.exchange_map_extractor import ExchangeMapExtractor
from src.extract.extractors.crypto_map_extractor import CryptoMapExtractor

# Import shared configs, runners path, JAR dependencies, and utility function
from dags.dag_utils import (
    SPARK_DELTA_OPT,
    TRANSFORM_RUNNER,
    LOAD_RUNNER,
    POSTGRES_JARS,
    SQL_DIR,
    make_callable,
)

daily_extract = [
    ExchangeMapExtractor,
    ExchangeAssetsExtractor,
    CryptoMapExtractor,
    CryptoCategoryExtractor,
]

transform_bash_command = f"spark-submit {SPARK_DELTA_OPT} {TRANSFORM_RUNNER}" + " daily"
load_bash_command = f"spark-submit {SPARK_DELTA_OPT} {POSTGRES_JARS} {LOAD_RUNNER}" + " daily"
sql_commands = ["CALL fill_nulls_exchange_map_info();", "CALL fill_nulls_spot_volume_and_wallet_weight();"]

default_args = {
    "start_date": datetime(2025, 6, 22, 14, 30),
    "retries": 3,
    "retry_delay": timedelta(minutes=9),
}

with DAG(
    "daily_dag",
    default_args=default_args,
    schedule="20 8 * * 1-5",  # On weekdays at 8:30 AM.
    catchup=False,
    tags=["daily_dag", "daily_ELT"],
    template_searchpath=[str(SQL_DIR)],
) as dag:

    # Extract task group
    with TaskGroup("extract_daily", dag=dag) as extract_phase_daily:
        tasks = []
        for extractor in daily_extract:
            tasks.append(
                PythonOperator(
                    task_id=f"extract_{extractor.__name__}",
                    python_callable=make_callable(extractor),
                )
            )
        # Serialize extraction tasks to respect API rate limits (avoid parallel API calls)
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]

    # Transform task
    transform_daily = BashOperator(task_id="transform_daily", dag=dag, bash_command=transform_bash_command)

    # Load task
    load_daily = BashOperator(task_id="load_daily", dag=dag, bash_command=load_bash_command)

    #  Data cleaning task
    with TaskGroup("post_load_daily", dag=dag) as post_load_daily:
        tasks = []
        for i, command in enumerate(sql_commands):
            tasks.append(
                SQLExecuteQueryOperator(
                    task_id=f"run_procedure_{i+1}",
                    conn_id="postgres_local",
                    sql=command,
                    database="crypto_dw",
                )
            )
        # Serialize procedures to avoid locked table problems
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]

    # Orchestrate
    extract_phase_daily >> transform_daily >> load_daily >> post_load_daily
