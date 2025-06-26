"""
dag_utils.py

Centralized configuration for project paths, Spark/Postgres options, and common utility functions.
This module simplifies maintenance by grouping all settings and helpers shared across DAGs, 
guaranteeing consistency, modularity, and DRY principles for the ETL pipeline.

Contents:
- Project root path, Spark and JDBC configuration options.
- Paths to orchestrator scripts (transform/load) and SQL directories/scripts for post-load cleaning.
- Utility function to convert an Extractor class into a callable for use in PythonOperator.
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPARK_DELTA_OPT = "--packages io.delta:delta-spark_2.12:3.3.1"
POSTGRES_JARS = f"--jars {PROJECT_ROOT}/jars/postgresql-42.6.0.jar"
TRANSFORM_RUNNER = PROJECT_ROOT / "src/transform/orchestrators/transform_pipeline_runner.py"
LOAD_RUNNER = PROJECT_ROOT / "src/load/orchestrators/load_pipeline_runner.py"
SQL_DIR = PROJECT_ROOT / "warehouse" / "scripts" / "procedures"
BACKUP_SCRIPT = PROJECT_ROOT / "src" / "db" / "backup_postgres.py"


def make_callable(cls):
    """
    Instantiates the provided extractor class and returns its `run` method as a callable.
    Args:
        cls (type): The class of the extractor (must implement a `run()` method).
    Returns:
        callable: The `run` method of the instantiated extractor, ready to be used as a PythonOperator's python_callable.
    """
    instance = cls()
    return instance.run
