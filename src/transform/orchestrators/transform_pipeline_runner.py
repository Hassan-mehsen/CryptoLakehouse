"""
-> transform_pipeline_runner.py

Entry point script for executing the Transformation pipeline using Apache Spark.

This script is designed to be executed via `spark-submit` from Airflow (or manually),
and acts as the runtime dispatcher for different transformation frequencies: daily, 5x, 10x, weekly, init, or all.

Usage:
    spark-submit -> transform_pipeline_runner.py [frequency]

Arguments:
    frequency: Specifies which group of transformation tasks to execute. Accepted values:
        - "daily"   -> Executes all daily tasks (e.g. daily crypto + exchange updates)
        - "5x"      -> Executes intra-day (5x/day) market metrics and facts
        - "10x"     -> Executes high-frequency updates (categories, sentiment, metrics)
        - "weekly"  -> Executes heavier updates (e.g. exchange/crypto info refresh)
        - "init"    -> Executes one-time initialization logic (history, links, etc.)
        - "all"     -> Executes the full transformation pipeline (full rebuild)

Architecture:
    - A single SparkSession is created per run (per frequency) with a clearly identifiable `appName`
      in the Spark UI for easier debugging and monitoring.
    - This session is reused across all transformer domains (Exchange, Crypto, GlobalMetrics, FearAndGreed),
      which optimizes resource usage and centralizes log tracing.

    Spark UI will display the application as: `CryptoELT_Transform_<frequency>`,
    e.g. `CryptoELT_Transform_5x` or `CryptoELT_Transform_daily`.

This design provides frequency-based modularity while maintaining a single SparkSession per run,
enhancing performance observability and reducing complexity across Airflow and Spark layers.
"""

from pyspark.sql import SparkSession
import argparse
from pathlib import Path
import sys

CURRENT_FILE = Path(__file__).resolve()
SRC_PATH = CURRENT_FILE.parents[1]
sys.path.insert(0, str(SRC_PATH))

from orchestrators.transform_orchestrator import TransformationPipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("frequency", choices=["daily", "weekly", "5x", "10x", "init", "all"])
    args = parser.parse_args()

    # Initialize SparkSession
    spark = (
        SparkSession.builder
        .appName(f"CryptoELT_Transform_{args.frequency}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    runner = TransformationPipeline(spark=spark)

    match args.frequency:
        case "daily":
            runner.run_daily_tasks()
        case "weekly":
            runner.run_weekly_tasks()
        case "5x":
            runner.run_5x_daily_tasks()
        case "10x":
            runner.run_10x_daily_tasks()
        case "init":
            runner.run_init_tasks()
        case "all":
            runner.run_all_tasks()

    runner.log_end()
    spark.stop()
