"""
5x-per-Day ELT DAG - Orchestration for the "5x per day" frequency of the crypto data pipeline.

This DAG runs five times per day (09:00, 11:00, 13:00, 15:00, 17:00 UTC) on weekdays, ideal for endpoints and datasets
requiring frequent intra-day updates to keep the Data Warehouse nearly real-time.

----------------------------------------------------
       Architecture & Execution Strategy
----------------------------------------------------
- Pipeline phases: Extraction, Transformation, Load.
    - Extraction: The extractor(s) are launched sequentially to respect API limits and load raw data into
      the data lake "bronze" layer.
    - Transformation: A single Spark job executes all transformation logic, moving the batch to the data lake "silver" layer.
    - Load: A single Spark job loads the transformed data into the Data Warehouse.

----------------------------------------------------
         Orchestration Highlights
----------------------------------------------------
- Simple, robust structure: one TaskGroup for extraction, one unitary task each for transform and load.
- Only a single Spark driver is started per phase, optimizing compute cost and job startup.
- Designed for semi-real-time freshness with minimal resource overhead.
- Monitoring and troubleshooting are straightforward due to phase unitarity.

----------------------------------------------------
         Operations & Usage
----------------------------------------------------
- Runs 5x per day for near-continuous data refresh.
- Optimized for endpoints/datasets that are dynamic but do not require minute-level latency.
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

# Import 5x extractor classe
from src.extract.extractors.crypto_listings_latest_extractor import CryptoListingsLatestExtractor

# Import shared configs, runners path, JAR dependencies, and utility function
from dags.dag_utils import SPARK_DELTA_OPT, TRANSFORM_RUNNER, LOAD_RUNNER, POSTGRES_JARS, make_callable

extract_5x = [CryptoListingsLatestExtractor]
transform_bash_command = f"spark-submit {SPARK_DELTA_OPT} {TRANSFORM_RUNNER}" + " 5x"
load_bash_command = f"spark-submit {SPARK_DELTA_OPT} {POSTGRES_JARS} {LOAD_RUNNER}" + " 5x"


default_args = {
    "start_date": datetime(2025, 6, 22, 14, 30),
    "retries": 3,
    "retry_delay": timedelta(minutes=8),
}

with DAG(
    "5x_dag",
    default_args=default_args,
    schedule="0 9,11,13,15,17 * * 1-5",  # On Weekdays at 9 and 11 AM, 1, 3, and 5 PM.
    catchup=False,
    tags=["5x_dag", "5x_ELT"],
) as dag:

    # Extract
    with TaskGroup("extract_5x", dag=dag) as extract_phase_5x:
        tasks = []
        for extractor in extract_5x:
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
    transform_5x = BashOperator(task_id="transform_5x", dag=dag, bash_command=transform_bash_command)

    # Load
    load_5x = BashOperator(task_id="load_5x", dag=dag, bash_command=load_bash_command)

    # Orchestrate
    extract_phase_5x >> transform_5x >> load_5x
