"""
Module: transform.py

Dynamic task selector for the transformation phase of the ETL pipeline.

This module is imported by the master DAG to determine -- based on the current UTC time --
which single transformation task should be executed at each scheduled run.
It ensures strict frequency-based orchestration of the Spark transformation pipeline.

-----------------------------------------------------
Scheduling Strategy -- One Task per Execution Slot
-----------------------------------------------------

Only **one** task should be selected and returned per DAG run.
If multiple tasks match the current time, an error is raised to avoid concurrent execution.

All logic is based on **UTC time** to stay aligned with:

- Airflow’s scheduler clock
- External API timestamps (e.g., CoinMarketCap)
- Spark application names and log timestamps

-----------------------------------------------------
Frequency-to-Time Mapping (UTC)
-----------------------------------------------------

- DAILY   -> Between 08:00 and 08:29
- WEEKLY  -> Mondays only, between 08:30 and 08:59
- 5x      -> At 09h, 11h, 13h, 15h, 17h -- only between minutes 00–29
- 10x     -> Same hours as 5x, but only between minutes 30–59
         -> Plus full execution at: 10h, 12h, 14h, 16h, 18h, 19h, 20h (after :30)

-----------------------------------------------------
INIT Transformation Tasks (Manual Only)
-----------------------------------------------------

- Init tasks like historic enrichment are not time-based.
- They are launched via a dedicated DAG (`transform_init_dag`) from the Airflow UI.
- The runner is called with `init` frequency: `spark-submit ... transform_pipeline_runner.py init`

-----------------------------------------------------
Purpose and Benefits
-----------------------------------------------------

- Clean separation of concerns: scheduling logic is fully isolated
- Guarantees **no frequency collision** (daily/weekly/5x/10x)
- Keeps logs, UI, and Spark jobs **easy to track and debug**
- Prevents unexpected behavior when tasks overlap
"""

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.bash import BashOperator
from datetime import datetime, timezone
from pathlib import Path

log = LoggingMixin().log

# resolve path dynamically
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def get_transform_task():

    now = datetime.now(timezone.utc)
    hour = now.hour
    minute = now.minute
    weekday = now.weekday()

    task = []

    runner_path = f"{PROJECT_ROOT}/src/transform/orchestrators/transform_pipeline_runner.py"

    if hour == 8 and minute < 30:
        task.append(
            BashOperator(
                task_id="run_transform_daily",
                bash_command=(f"spark-submit --packages io.delta:delta-spark_2.12:3.3.1 {runner_path} daily"),
            )
        )

    if weekday == 0 and hour == 8 and 30 <= minute < 60:
        task.append(
            BashOperator(
                task_id="run_transform_weekly",
                bash_command=(f"spark-submit --packages io.delta:delta-spark_2.12:3.3.1 {runner_path} weekly"),
            )
        )

    if hour in [9, 11, 13, 15, 17] and minute < 30:
        task.append(
            BashOperator(
                task_id="run_transform_5x",
                bash_command=(f"spark-submit --packages io.delta:delta-spark_2.12:3.3.1 {runner_path} 5x"),
            )
        )

    if hour in range(10, 21) and minute >= 30:
        task.append(
            BashOperator(
                task_id="run_transform_10x",
                bash_command=(f"spark-submit --packages io.delta:delta-spark_2.12:3.3.1 {runner_path} 10x"),
            )
        )

    if len(task) > 1:
        raise ValueError(
            f"[TransformScheduler ERROR] Multiple tasks selected at {now.isoformat()}: {[task.task_id for task in task]}. "
            "Only one task should run per transform cycle."
        )
    if len(task) == 0:
        log.warning(f"[TransformScheduler] No task scheduled at {now.isoformat()}.")
        return []

    log.info(f"[TransformScheduler] Task triggered at {now.isoformat()} -> {[task.task_id for task in task]}")

    return task[0] if task else None
