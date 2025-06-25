from pyspark.sql import SparkSession
from pathlib import Path
import sys

CURRENT_FILE = Path(__file__).resolve()
SRC_PATH = CURRENT_FILE.parents[2]
sys.path.insert(0, str(SRC_PATH))

from load.loaders.global_metrics_loader import GlobalMetricsLoader
from load.loaders.fear_and_greed_loader import FearAndGreedLoader
from load.loaders.exchange_loader import ExchangeLoader
from load.loaders.crypto_loader import CryptoLoader


class LoadPipeline:
    """
    Orchestrates the loading of transformed data into the PostgreSQL Data Warehouse.

    This class integrates loaders for all business domains (Crypto, Exchange, GlobalMetrics, FearAndGreed),
    and exposes methods organized by execution frequency (e.g., daily, weekly, 10x daily).

    To ensure referential integrity and absorb all available data correctly,
    the following execution order is strongly recommended:
    1. `run_10x_daily_tasks()`  -> most volatile dimensions & facts
    2. `run_daily_tasks()`      -> core IDs and link tables (prepares FKs)
    3. `run_5x_daily_tasks()`   -> detailed fact tables (market)
    4. `run_weekly_tasks()`     -> metadata & slowly changing dimensions
    5. `run_init_tasks()`       -> one-time initialization (if applicable)

    A shared SparkSession must be injected externally (typically via `spark-submit`).
    """

    def __init__(self, spark: SparkSession):
        """
        Initializes the LoadPipeline with domain-specific loaders using the provided SparkSession.

        Args:
            spark (SparkSession): Shared session used across all loaders.
        """
        self.crypto_loader = CryptoLoader(spark=spark)
        self.exchange_loader = ExchangeLoader(spark=spark)
        self.fear_greed_loader = FearAndGreedLoader(spark=spark)
        self.global_metrics_loader = GlobalMetricsLoader(spark=spark)

        self.global_metrics_loader.log_section(
            title="Initializing Spark application - Load Pipeline is ready for processing.", width=100
        )

    def run_all_tasks(self):
        """
        Executes the full Load pipeline across all domains.

        Intended for manual or full-load scenarios (e.g., staging rebuild, new environment).
        """
        self.run_10x_daily_tasks()
        self.run_daily_tasks()
        self.run_5x_daily_tasks()
        self.run_weekly_tasks()

    def run_daily_tasks(self):
        """
        Loads daily-scheduled tables, primarily dimensions and link tables.
        """
        self.exchange_loader.load_dim_exchange_id()
        self.exchange_loader.load_dim_exchange_info()
        self.exchange_loader.load_dim_exchange_map()
        self.exchange_loader.load_fact_exchange_assets()
        self.crypto_loader.load_dim_crypto_id()
        self.crypto_loader.load_dim_crypto_map()
        self.crypto_loader.load_crypto_category_link()

    def run_weekly_tasks(self):
        """
        Loads weekly-scheduled tables that change slowly.

        Includes descriptive metadata about exchanges and cryptos.
        """
        self.crypto_loader.load_dim_crypto_info()

    def run_10x_daily_tasks(self):
        """
        Loads high-frequency metrics updated multiple times per day.

        Includes:
        - Crypto categories and metrics
        - Global market indicators
        - Fear and Greed sentiment score
        """
        self.crypto_loader.load_dim_crypto_category()
        self.crypto_loader.load_fact_crypto_category()
        self.global_metrics_loader.load_fact_global_market()
        self.fear_greed_loader.load_fact_market_sentiment()

    def run_5x_daily_tasks(self):
        """
        Loads medium-frequency metrics, including detailed market data per crypto.

        Typically run ~5 times per day (every 4–5 hours).
        """
        self.crypto_loader.load_fact_crypto_market()

    def log_end(self):
        self.global_metrics_loader.log_section(
            title="All scheduled transformation tasks for this run have been executed.", width=100
        )


if __name__ == "__main__":

    CURRENT_FILE = Path(__file__).resolve()
    SRC_PATH = CURRENT_FILE.parents[3]
    sys.path.insert(0, str(SRC_PATH))
    JARS_PATH = "jars/postgresql-42.6.0.jar"

    spark = (
        SparkSession.builder.appName("TestCryptoLoader")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars",JARS_PATH)
        .getOrCreate()
    )

    try:
        runner = LoadPipeline(spark=spark)
        runner.run_all_tasks()
        runner.log_end()
    finally:
        spark.stop()
