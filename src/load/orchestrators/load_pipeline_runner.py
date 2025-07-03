"""
-> load_pipeline_runner.py

Entry point script for executing the Load pipeline using Apache Spark.

This script is designed to be executed via `spark-submit` from Airflow (or manually),
and acts as the runtime dispatcher for different load frequencies: daily, 5x, 10x, weekly, or all.

Usage:
    spark-submit -> load_pipeline_runner.py [frequency]

Arguments:
    frequency: Specifies which group of load tasks to execute. Accepted values:
        - "daily"   -> Loads daily dimension and fact tables (IDs, links, exchange assets)
        - "5x"      -> Loads intra-day (5x/day) market facts (crypto prices, supply, etc.)
        - "10x"     -> Loads high-frequency data (categories, sentiment, global metrics)
        - "weekly"  -> Loads slower-changing metadata and descriptions
        - "all"     -> Loads all tables across all domains in recommended order

Architecture:
    - A single SparkSession is created per execution with an identifiable `appName`
      in the Spark UI for easier debugging and performance tracking.
    - This session is shared across all loader modules (Crypto, Exchange, GlobalMetrics, FearAndGreed),
      enabling efficient foreign key validation and metadata consolidation.

    Spark UI will display the application as: `CryptoELT_Load_<frequency>`,
    e.g. `CryptoELT_Load_10x` or `CryptoELT_Load_daily`.

This design ensures frequency-aligned modular loading while maintaining referential integrity
and minimizing data duplication in the PostgreSQL Data Warehouse.
"""

from pyspark.sql import SparkSession
import argparse
from pathlib import Path
import sys

CURRENT_FILE = Path(__file__).resolve()
SRC_PATH = CURRENT_FILE.parents[1]
sys.path.insert(0, str(SRC_PATH))
JARS_PATH = "jars/postgresql-42.6.0.jar"

from orchestrators.load_orchestrator import LoadPipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("frequency", choices=["daily", "weekly", "5x", "10x", "all"])
    args = parser.parse_args()

    # Initialize SparkSession
    spark = (
        SparkSession.builder
        .appName(f"CryptoELT_Load_{args.frequency}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars",JARS_PATH)
        .getOrCreate()
    )


    runner = LoadPipeline(spark=spark)

    match args.frequency:
        case "daily":
            runner.run_daily_tasks()
        case "weekly":
            runner.run_weekly_tasks()
        case "5x":
            runner.run_5x_daily_tasks()
        case "10x":
            runner.run_10x_daily_tasks()
        case "all":
            runner.run_all_tasks()

    runner.log_end()
    spark.stop()
