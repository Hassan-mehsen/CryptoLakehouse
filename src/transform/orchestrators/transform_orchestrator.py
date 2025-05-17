from pyspark import SparkConf, SparkContext
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


class TransformationPipelineRunner:
    """
    Orchestrates the execution of multiple transformer components within a shared SparkContext.
    Designed for integration with workflow schedulers like Airflow, with clearly separated methods
    corresponding to different execution frequencies (daily, weekly, etc.).
    """

    def __init__(self):
        """
        Initializes a shared SparkContext and sets up dedicated SparkSessions for each transformer.

        Each transformer (Exchange, Crypto, GlobalMetrics, FearAndGreed) receives its own SparkSession
        with a unique application name for better traceability in the Spark UI.

        This separation improves observability, debugging, and scheduling when used in orchestrators
        like Airflow or in multi-step DAGs.
        """

        # Create a shared SparkContext for the entire pipeline
        self.sc = SparkContext(conf=SparkConf().setAppName("CryptoETL_MasterContext"))

        # Initialize each transformer with an isolated SparkSession.
        self.exchange_session = self.create_spark_session(ExchangeTransformer, app_name="ExchangeTransformer")
        self.crypto_session = self.create_spark_session(CryptoTransformer, app_name="CryptoTransformer")
        self.global_metrics_session = self.create_spark_session(GlobalMetricsTransformer, app_name="GlobalMetricsTransformer")
        self.fear_greed_session = self.create_spark_session(FearAndGreedTransformer, app_name="FearAndGreedTransformer")

        # Log a header to indicate the pipeline is ready
        self.global_metrics_session.log_section(
            title="Initializing Spark application - Transformation Pipeline is ready for processing.", width=100
        )

    def create_spark_session(self, transformer_class, app_name: str):
        """
        Instantiates a transformer object using a dedicated SparkSession.

        Args:
            transformer_class: The transformer class to instantiate (e.g. GlobalMetricsTransformer).
            app_name (str): The name to assign to the Spark application (visible in Spark UI).

        Returns:
            An instance of the transformer class with its SparkSession initialized.
        """
        spark = (
            SparkSession(self.sc)
            .newSession()
            .builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )

        return transformer_class(spark)

    def run_all_tasks(self):
        """
        Runs the full Transformation pipeline. Meant for manual or full-rebuild scenarios.
        """
        self.exchange_session.run()
        self.crypto_session.run()
        self.global_metrics_session.build_global_market_fact()
        self.fear_greed_session.build_market_sentiment_history()
        self.fear_greed_session.build_market_sentiment_fact()

    def run_daily_tasks(self):
        """
        Runs all ETL tasks that are scheduled to run daily.
        """
        # Exchange domain
        self.exchange_session.build_exchange_id_dim()
        self.exchange_session.build_exchange_map_dim()
        self.exchange_session.build_exchange_assets_fact()

        # Crypto domain
        self.crypto_session.build_crypto_id_dim()
        self.crypto_session.build_crypto_map_dim()

    def run_weekly_tasks(self):
        """
        Runs all ETL tasks that are scheduled to run weekly.
        """
        # Exchange domain
        self.exchange_session.build_exchange_info_dim()

        # Crypto domain
        self.crypto_session.build_crypto_info_dim()

    def run_init_tasks(self):
        """
        Runs all ETL tasks that are only needed during the initial setup of the pipeline.
        """
        # Crypto domain
        self.crypto_session.build_crypto_category_link()

        # Fear and Greed domain
        self.fear_greed_session.build_market_sentiment_history()

    def run_10x_daily_tasks(self):
        """
        Runs tasks scheduled to execute multiple times per day (e.g., every 2-3 hours).
        """
        # Crypto domain
        self.crypto_session.build_crypto_categories_dim()
        self.crypto_session.build_crypto_category_fact()

        # Global Metrics domain
        self.global_metrics_session.build_global_market_fact()

        # Fear and Greed domain
        self.fear_greed_session.build_market_sentiment_fact()

    def run_5x_daily_tasks(self):
        """
        Runs tasks scheduled approximately 5 times per day (e.g., every 4-5 hours).
        """
        # Crypto domain
        self.crypto_session.build_crypto_market_fact()

    def log_end(self):
        self.global_metrics_session.log_section(
            title="All scheduled transformation tasks for this run have been executed.", width=100
        )


if __name__ == "__main__":

    try:
        runner = TransformationPipelineRunner()
        runner.run_all_tasks()
        runner.log_end()
    finally:
        runner.sc.stop()
