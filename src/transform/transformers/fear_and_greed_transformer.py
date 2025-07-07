from pyspark.sql.types import IntegerType, StringType, StructType, StructField, LongType
from pyspark.sql.functions import col, to_timestamp, date_format, from_unixtime
from transform.base.base_transformer import BaseTransformer
from pyspark.sql import DataFrame, SparkSession
from typing import Optional, Tuple


class FearAndGreedTransformer(BaseTransformer):
    """
    FearAndGreedTransformer processes snapshots of the Fear & Greed index
    into a structured fact table (`fact_market_sentiment`) for market sentiment analysis.

    This transformer operates in two phases:
    - Initial load using historical data
    - Daily appending using live updates

    Source: `fear_and_greed_data`
    Output: `fact_market_sentiment`
    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark)

    # --------------------------------------------------------------------
    #                          Public entrypoint
    # --------------------------------------------------------------------

    def build_market_sentiment_history(self) -> None:
        """
        Performs a one-time initialization of the `fact_market_sentiment` table
        using historical Fear & Greed index data.

        This method is intended to be run only once to populate the table with
        historical sentiment scores. All subsequent updates should use the
        `build_market_sentiment_fact()`.

        Steps:
        - Loads a historical snapshot from `fear_and_greed_data`
        - Delegates transformation to `__prepare_market_sentiment_history()` via `_run_build_step`
        """
        self.log_section("Running FearAndGreedTransformer [History Init]")

        snapshot_paths = self.find_latest_data_files("fear_and_greed_data")
        if not snapshot_paths:
            self.log("No snapshot available for fact_market_sentiment (history). Skipping.", table_name="market_sentiment_fact")
            return

        latest_snapshot = snapshot_paths[0]
        if self.read_last_metadata("market_sentiment_history") is not None:
            self.log("Historical data already initialized. Skipping.", table_name="market_sentiment_fact")
            self.log_section("End FearAndGreedTransformer [History Init]")
            return

        self._run_build_step(
            "market_sentiment_history",
            lambda: self.__prepare_market_sentiment_history(latest_snapshot),
            "fact_market_sentiment"
        )

        self.log_section("End FearAndGreedTransformer [History Init]")

    def build_market_sentiment_fact(self) -> None:
        """
        Transform daily Fear & Greed index data to the `fact_market_sentiment` table.

        Steps:
        - Loads the most recent live snapshot from `fear_and_greed_data`
        - Delegates transformation to `__prepare_market_sentiment_df()` via `_run_build_step`
        - Writing the result to the existing Silver Delta table
        """
        self.log_section("Running FearAndGreedTransformer [Live Update]")

        snapshot_paths = self.find_latest_data_files("latest_fear_and_greed_data")
        if not snapshot_paths:
            self.log("No snapshot available for fact_market_sentiment (live). Skipping.", table_name="market_sentiment_fact")
            return

        latest_snapshot = snapshot_paths[0]
        if not self.should_transform("fact_market_sentiment", latest_snapshot, daily_comparison=False):
            self.log("fact_market_sentiment is up to date. Skipping transformation.", table_name="market_sentiment_fact")
            self.log_section("End FearAndGreedTransformer [Live Update]")
            return

        self._run_build_step(
            "fact_market_sentiment",
            lambda: self.__prepare_market_sentiment_df(latest_snapshot),
            "fact_market_sentiment"
        )

        self.log_section("End FearAndGreedTransformer [Live Update]")

    # --------------------------------------------------------------------
    #                    Internal Transformation Method
    # --------------------------------------------------------------------

    def __prepare_market_sentiment_history(self, latest_snapshot: str) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares historical Fear & Greed index data from the snapshot.

        Expected schema:
        - `value`: integer score
        - `value_classification`: qualitative label
        - `timestamp`: string ISO date

        Returns:
            Tuple of cleaned DataFrame with columns:
            - `snapshot_date`: parsed date
            - `fear_greed_score`
            - `sentiment_label`
        """
        self.log("> Starting historical transformation...", table_name="market_sentiment_fact")

        schema = StructType([
                        StructField("value", LongType(), True),
                        StructField("value_classification", StringType(), True),
                        StructField("timestamp", StringType(), True),
                    ])

        self.log(f"Reading snapshot: {latest_snapshot.name}", table_name="market_sentiment_fact")
        try:
            df_raw = self.spark.read.schema(schema).parquet(str(latest_snapshot))
            self.log("Successfully loaded raw historical DataFrame", table_name="market_sentiment_fact")

        except Exception as e:
            self.log(f"[ERROR] Failed to load historical snapshot: {e}", table_name="market_sentiment_fact")
            return None

        df_cleaned = (
            df_raw.withColumn("snapshot_timestamp", to_timestamp(from_unixtime(col("timestamp"))))
            .withColumn("snapshot_str", date_format(col("snapshot_timestamp"), "yyyy-MM-dd"))
            .withColumn("value", col("value").cast("int"))
            .select(
                "snapshot_timestamp",
                "snapshot_str",
                col("value").alias("fear_greed_score"),
                col("value_classification").alias("sentiment_label"),
            )
            .dropna(subset=["snapshot_timestamp"])
            .dropDuplicates(subset=["snapshot_timestamp"])
        )

        self.log_dataframe_info(df_cleaned, "Cleaned fact_market_sentiment [history]", table_name="market_sentiment_fact")
        return df_cleaned, latest_snapshot

    def __prepare_market_sentiment_df(self, latest_snapshot: str) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares live Fear & Greed index data from the most recent snapshot.

        Expected schema:
        - `value`: integer score
        - `value_classification`: qualitative label
        - `update_time`: string ISO date

        Returns:
            Tuple of cleaned DataFrame with columns:
            - `snapshot_date`: parsed date
            - `fear_greed_score`
            - `sentiment_label`
        """
        self.log("> Starting live transformation...", table_name="market_sentiment_fact")

        schema = StructType([
                        StructField("value", LongType(), True),
                        StructField("value_classification", StringType(), True),
                        StructField("update_time", StringType(), True),
                    ])

        self.log(f"Reading snapshot: {latest_snapshot.name}", table_name="market_sentiment_fact")
        try:
            df_raw = self.spark.read.schema(schema).parquet(str(latest_snapshot))
            self.log("Successfully loaded raw live DataFrame", table_name="market_sentiment_fact")

        except Exception as e:
            self.log(f"[ERROR] Failed to load live snapshot: {e}", table_name="market_sentiment_fact")
            return None

        df_cleaned = (
            df_raw.withColumn("snapshot_timestamp", to_timestamp(col("update_time")))
            .withColumn("snapshot_str", date_format(col("snapshot_timestamp"), "yyyy-MM-dd"))
            .withColumn("value", col("value").cast("int"))
            .select(
                "snapshot_timestamp",
                "snapshot_str",
                col("value").alias("fear_greed_score"),
                col("value_classification").alias("sentiment_label"),
            )
            .dropna(subset=["snapshot_timestamp"])
            .dropDuplicates(subset=["snapshot_timestamp"])
        )

        self.log_dataframe_info(df_cleaned, "Cleaned fact_market_sentiment [live]", table_name="market_sentiment_fact")
        return df_cleaned, latest_snapshot
