from pyspark.sql import SparkSession
from pathlib import Path
import sys

CURRENT_FILE = Path(__file__).resolve()
SRC_PATH = CURRENT_FILE.parents[2]
sys.path.insert(0, str(SRC_PATH))

from transform.transformers.global_metrics_transformer import GlobalMetricsTransformer
from transform.transformers.fear_and_greed_transformer import FearAndGreedTransformer
from transform.transformers.exchange_transformer import ExchangeTransformer
from transform.transformers.crypto_transformer import CryptoTransformer


class TransformationPipeline:
    """
    Orchestrates the transformation of raw extracted data into cleaned, structured Delta tables.

    This class integrates transformation logic across all business domains (Exchange, Crypto,
    GlobalMetrics, FearAndGreed), exposing methods organized by execution frequency
    (e.g., daily, weekly, 10x daily).

    To ensure proper ordering of data dependencies and optimal broadcast of dimension tables across
    transformers, the following execution order is strongly recommended:

    1. `run_weekly_tasks()`     -> metadata and slowly changing dimensions
    2. `run_daily_tasks()`      -> essential ID and map dimensions + link tables
    3. `run_5x_daily_tasks()`   -> high-frequency market facts
    4. `run_10x_daily_tasks()`  -> foundational categories and metrics
    5. `run_init_tasks()`       -> one-time transformations

    A shared SparkSession must be created externally and passed to the constructor
    (typically via `spark-submit` or orchestrators like Airflow).
    """

    def __init__(self, spark: SparkSession):
        """
        Initializes the pipeline with a shared SparkSession, and instantiates all transformers
        with this session for consistency and efficient resource usage.

        Args:
            spark (SparkSession): A SparkSession created externally (typically by spark-submit).
        """

        self.exchange_session = ExchangeTransformer(spark=spark)
        self.crypto_session = CryptoTransformer(spark=spark)
        self.global_metrics_session = GlobalMetricsTransformer(spark=spark)
        self.fear_greed_session = FearAndGreedTransformer(spark=spark)

        self.global_metrics_session.log_section(
            title="Initializing Spark application - Transformation Pipeline is ready for processing.", width=100
        )

    def run_all_tasks(self):
        """
        Executes the full transformation pipeline across all domains.

        Use case:
        - Manual backfill or complete rebuild of the Data Warehouse.
        """  
        self.run_weekly_tasks()
        self.run_daily_tasks()
        self.run_5x_daily_tasks()
        self.run_10x_daily_tasks()
        self.run_init_tasks()
        
    def run_daily_tasks(self):
        """
        Runs all transformation tasks scheduled to execute daily.

        Includes:
        - Exchange dimensions and facts (frequently updated metrics)
        - Crypto ID and category linking
        """
        self.exchange_session.build_exchange_id_dim()
        self.exchange_session.build_exchange_info_dim()
        self.exchange_session.build_exchange_map_dim()
        self.exchange_session.build_exchange_assets_fact()
        self.crypto_session.build_crypto_id_dim()
        self.crypto_session.build_crypto_map_dim()
        self.crypto_session.build_crypto_category_link()

    def run_weekly_tasks(self):
        """
        Runs all transformation tasks scheduled to execute weekly.

        Includes less volatile dimensions such as:
        - Exchange metadata
        - Crypto project metadata
        """
        self.crypto_session.build_crypto_info_dim()

    def run_init_tasks(self):
        """
        Runs one-time initialization tasks (run only once during setup).

        Useful for:
        - Bootstrapping historical data
        - Populating base time series for sentiment indicators
        """
        self.fear_greed_session.build_market_sentiment_history()

    def run_10x_daily_tasks(self):
        """
        Runs tasks scheduled to execute at high frequency (e.g., every 2–3 hours).

        Targets near real-time metrics such as:
        - Crypto category evolution
        - Global market statistics
        - Market sentiment scores
        """
        self.crypto_session.build_dim_crypto_category()
        self.crypto_session.build_fact_crypto_category()
        self.global_metrics_session.build_global_market_fact()
        self.fear_greed_session.build_market_sentiment_fact()

    def run_5x_daily_tasks(self):
        """
        Runs tasks scheduled ~5 times per day (e.g., every 4–5 hours).

        Currently focused on:
        - Fact table for crypto market data
        """
        self.crypto_session.build_fact_crypto_market()

    def log_end(self):
        self.global_metrics_session.log_section(
            title="All scheduled transformation tasks for this run have been executed.", width=100
        )


if __name__ == "__main__":

    # usecase: fast demo
    spark = (
        SparkSession.builder.appName(f"CryptoETL_Transform_Test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    try:
        runner = TransformationPipeline(spark)
        runner.run_all_tasks()
        runner.log_end()
    finally:
        spark.stop()
