"""
Weekly ETL DAG - Modular orchestration for the "weekly" frequency of the crypto data pipeline.

This DAG automates extraction, transformation, and loading every Monday at 08:00 UTC,
targeting endpoints and business logic that only require weekly updates.

----------------------------------------------------
       Architecture & Execution Strategy
----------------------------------------------------
- Pipeline phases: Extraction, Transformation, Load.
    - Extraction: Each weekly extractor runs sequentially to respect API rate limits and loads
      raw data into the data lake "bronze" layer.
    - Transformation: A single Spark job manages all transformations and writes to the "silver" layer of the data lake.
    - Load: A single Spark job loads the transformed (silver) data into the Data Warehouse.

----------------------------------------------------
         Orchestration Highlights
----------------------------------------------------
- TaskGroups structure and monitor each step for clear tracking and robust error handling.
- The Spark driver is launched only once per phase, optimizing resource consumption and startup times.
- The weekly DAG is dedicated to low-frequency endpoints and batch updates.

----------------------------------------------------
         Operations & Usage
----------------------------------------------------
- Runs on any Airflow executor.
- Retry policy is reinforced for high-stakes, less frequent updates.
- Suitable for slow-moving metadata or large batch business logic.
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

# Import weekly extractor classes
from src.extract.extractors.exchange_info_extractor import ExchangeInfoExtractor
from src.extract.extractors.crypto_info_extractor import CryptoInfoExtractor

# Import shared configs, runners path, JAR dependencies, and utility function
from dags.dag_utils import SPARK_DELTA_OPT, TRANSFORM_RUNNER, LOAD_RUNNER, POSTGRES_JARS, make_callable

weekly_extract = [
    ExchangeInfoExtractor,
    CryptoInfoExtractor,
]

transform_bash_command = f"spark-submit {SPARK_DELTA_OPT} {TRANSFORM_RUNNER}" + " weekly"
load_bash_command = f"spark-submit {SPARK_DELTA_OPT} {POSTGRES_JARS} {LOAD_RUNNER}" + " weekly"


default_args = {
    "start_date": datetime(2025, 6, 22, 14, 30),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "weekly_dag",
    default_args=default_args,
    schedule="0 8 * * 1",  # Every Monday at 8:00 AM
    catchup=False,
    tags=["weekly_dag", "weekly_ETL"],
) as dag:

    # Extract
    with TaskGroup("extract_weekly", dag=dag) as extract_phase_weekly:
        tasks = []
        for extractor in weekly_extract:
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
    transform_weekly = BashOperator(task_id="transform_weekly", dag=dag, bash_command=transform_bash_command)

    # Load
    load_weekly = BashOperator(task_id="load_weekly", dag=dag, bash_command=load_bash_command)

    # Orchestrate
    extract_phase_weekly >> transform_weekly >> load_weekly
