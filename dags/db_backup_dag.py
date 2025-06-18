"""
DAG: db_backup_dag
------------------

This DAG performs a full weekly backup of the PostgreSQL database used by the Data Warehouse.

How it works:
-------------
- Triggered every Monday at 3:00 AM (server time).
- Executes a standalone Python script (`backup_postgres.py`) located in `src/db/`, which uses `pg_dump` to generate a `.sql` dump file.
- The backup file is saved in the `data/gold/dumps/` directory with a timestamp in its filename.
- Database connection parameters are read from the `.env` file using the `DUMP_URL` environment variable.

Features:
---------
- Designed to be robust: includes 1 retry after a 5-minute delay in case of failure.
- Sends an email notification if the backup fails.
- Intended for maintenance purposes to ensure data resilience and disaster recovery readiness.

Tags: maintenance, backup
"""

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta, datetime
from pathlib import Path

# path to the script
PROJECT_ROOT = Path(__file__).resolve().parents[1]
BACKUP_SCRIPT = PROJECT_ROOT / "src" / "db" / "backup_postgres.py"

default_args = {
    "start_date": datetime(2025, 6, 10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": "hassan.mehsenn@hotmail.com",
}

with DAG(
    dag_id="db_backup_dag",
    schedule="0 3 * * 1",  # Each monday at 3 AM
    catchup=False,
    default_args=default_args,
    tags=["maintenance", "backup"],
) as dag:

    run_backup_script = BashOperator(task_id="run_python_backup_script", bash_command=f"python3 {BACKUP_SCRIPT}")
