"""
10x-per-Day ELT DAG - Orchestration for the "10x per day" frequency of the crypto data pipeline.

This DAG executes hourly, from 10:30 to 19:30 UTC on weekdays, handling endpoints that require high-frequency updates
and providing near real-time market/business indicators.

----------------------------------------------------
       Architecture & Execution Strategy
----------------------------------------------------
- Pipeline phases: Extraction, Transformation, Load.
    - Extraction: All extractors are executed sequentially to respect API quotas and load raw data into
      the data lake "bronze" layer.
    - Transformation: All necessary transformations are managed by a single Spark job, writing to the data lake "silver" layer.
    - Load: A single Spark job loads the silver-layer data into the Data Warehouse.

----------------------------------------------------
         Orchestration Highlights
----------------------------------------------------
- Straightforward, high-frequency orchestration for high-velocity endpoints.
- Resource usage is optimized: only one Spark driver per phase.
- Robust, deterministic, and easily traceable scheduling for near real-time ELT needs.

----------------------------------------------------
         Operations & Usage
----------------------------------------------------
- Runs every hour during business hours for maximum data freshness.
- Optimized for market metrics and indicators that benefit from high update rates.
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Resolve project path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Import 10x extractor classe
from src.extract.extractors.global_metrics_quotes_latest_extractor import GlobalMetricsQuotesLatestExtractor
from src.extract.extractors.fear_and_greed_latest_extractor import FearAndGreedLatestExtractor
from src.extract.extractors.crypto_categories_extractor import CryptoCategoriesExtractor

# Import shared configs, runners path, JAR dependencies, and utility function
from dags.dag_utils import SPARK_DELTA_OPT, TRANSFORM_RUNNER, LOAD_RUNNER, POSTGRES_JARS, make_callable

extract_10x = [
    CryptoCategoriesExtractor,
    GlobalMetricsQuotesLatestExtractor,
    FearAndGreedLatestExtractor,
]

transform_bash_command = f"spark-submit {SPARK_DELTA_OPT} {TRANSFORM_RUNNER}" + " 10x"
load_bash_command = f"spark-submit {SPARK_DELTA_OPT} {POSTGRES_JARS} {LOAD_RUNNER}" + " 10x"


default_args = {
    "start_date": datetime(2025, 6, 22, 14, 30),
    "retries": 3,
    "retry_delay": timedelta(minutes=8),
}

with DAG(
    "10x_dag",
    default_args=default_args,
    schedule="30 10-19 * * 1-5",  # On Weekdays, every hour between 10:30 AM and 7:30 PM.
    catchup=False,
    tags=["10x_dag", "10x_ELT"],
) as dag:

    # Extract
    with TaskGroup("extract_10x", dag=dag) as extract_phase_10x:
        tasks = []
        for extractor in extract_10x:
            tasks.append(
                PythonOperator(
                    task_id=f"extract_{extractor.__name__}",
                    python_callable=make_callable(extractor),
                )
            )
        # Serialize extraction tasks to respect API rate limits (avoid parallel API calls)
        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]

    # Transform
    transform_10x = BashOperator(task_id="transform_10x", dag=dag, bash_command=transform_bash_command)

    # Load
    load_10x = BashOperator(task_id="load_10x", dag=dag, bash_command=load_bash_command)

    # Orchestrate
    extract_phase_10x >> transform_10x >> load_10x
