from pyspark.sql.functions import col, to_timestamp, to_date, date_format, when, lit, abs
from transform.base.base_transformer import BaseTransformer
from pyspark.sql import DataFrame, SparkSession
from typing import Optional, Tuple
from pyspark.sql.types import *


class GlobalMetricsTransformer(BaseTransformer):
    """
    GlobalMetricsTransformer processes raw global market metrics into a clean,
    structured fact table: `fact_global_market`.

    Data is extracted from the `global_metrics_data` source (e.g., /v1/global-metrics/quotes/latest)
    and enriched with calculated KPIs such as dominance delta, liquidity ratio, and growth rates.

    This class supports daily ingestion and is designed for analytics layers .
    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark)

    # --------------------------------------------------------------------
    #                          Public entrypoint
    # --------------------------------------------------------------------

    def build_global_market_fact(self) -> None:
        """
        Triggers the full transformation process for the `fact_global_market` table.

        Steps:
        - Load the latest snapshot file from `global_metrics_data`
        - Skip processing if already handled (based on metadata)
        - Delegate to `__prepare_global_market_df` to process and return a clean DataFrame
        - Write results to the Silver layer using Delta Lake
        """
        self.log_section("Running GlobalMetricsTransformer")

        snapshot_paths = self.find_latest_data_files("global_metrics_data")
        if not snapshot_paths:
            self.log("No snapshot available for fact_global_market. Skipping.", table_name="global_market_fact")
            return

        latest_snapshot = snapshot_paths[0]
        if not self.should_transform("fact_global_market", latest_snapshot, daily_comparison=False):
            self.log("fact_global_market is up to date. Skipping transformation.", table_name="global_market_fact")
            self.log_section("End GlobalMetricsTransformer")
            return

        self._run_build_step(
            "fact_global_market",
            lambda: self.__prepare_global_market_df(latest_snapshot),
            "fact_global_market",
            mode="append",
        )
        self.log_section("End GlobalMetricsTransformer")

    # --------------------------------------------------------------------
    #                    Internal Transformation Method
    # --------------------------------------------------------------------

    def __prepare_global_market_df(self, latest_snapshot: str) -> Optional[Tuple[DataFrame, str]]:
        """
        Reads, cleans, and transforms a snapshot of global crypto market metrics into
        a structured fact table format, ready for storage and analytics.

        Steps:
        - Apply strict schema to raw input
        - Rename columns for clarity and consistency
        - Cast LongType columns to IntegerType
        - Derive business KPIs with safe handling of nulls and zero-division
        - Parse and format timestamp fields for compatibility with SQL/BI tools
        - Return a final DataFrame with selected columns only

        Args:
            latest_snapshot (str): Path to the latest raw data snapshot file

        Returns:
            Optional[Tuple[DataFrame, str]]: Tuple of cleaned DataFrame and the input path used,
            or None if the input is unreadable.
        """
        self.log("> Starting transformation: global_market_fact...", table_name="global_market_fact")

        # Define schema
        schema = StructType(
            [
                # Primary key
                StructField("date_snapshot", StringType(), True),
                StructField("last_updated", StringType(), True),
                # Actual dominance
                StructField("btc_dominance", DoubleType(), True),
                StructField("eth_dominance", DoubleType(), True),
                # global activity
                StructField("active_cryptocurrencies", LongType(), True),
                StructField("total_cryptocurrencies", LongType(), True),
                StructField("active_market_pairs", LongType(), True),
                StructField("active_exchanges", LongType(), True),
                StructField("total_exchanges", LongType(), True),
                # dominance history
                StructField("btc_dominance_yesterday", DoubleType(), True),
                StructField("eth_dominance_yesterday", DoubleType(), True),
                StructField("btc_dominance_24h_percentage_change", DoubleType(), True),
                StructField("eth_dominance_24h_percentage_change", DoubleType(), True),
                # DeFi
                StructField("defi_volume_24h", DoubleType(), True),
                StructField("defi_volume_24h_reported", DoubleType(), True),
                StructField("defi_market_cap", DoubleType(), True),
                StructField("defi_24h_percentage_change", DoubleType(), True),
                # Stablecoins
                StructField("stablecoin_market_cap", DoubleType(), True),
                StructField("stablecoin_volume_24h", DoubleType(), True),
                StructField("stablecoin_volume_24h_reported", DoubleType(), True),
                StructField("stablecoin_24h_percentage_change", DoubleType(), True),
                # Derivatives
                StructField("derivatives_volume_24h", DoubleType(), True),
                StructField("derivatives_volume_24h_reported", DoubleType(), True),
                StructField("derivatives_24h_percentage_change", DoubleType(), True),
                # Altcoins
                StructField("quote_altcoin_volume_24h", DoubleType(), True),
                StructField("quote_altcoin_volume_24h_reported", DoubleType(), True),
                StructField("quote_altcoin_market_cap", DoubleType(), True),
                # Volume & market cap global (USD)  ----
                StructField("quote_total_market_cap", DoubleType(), True),
                StructField("quote_total_volume_24h", DoubleType(), True),
                StructField("quote_total_volume_24h_reported", DoubleType(), True),
                # evolution vs yesterday
                StructField("quote_total_market_cap_yesterday", DoubleType(), True),
                StructField("quote_total_volume_24h_yesterday", DoubleType(), True),
                StructField("quote_total_market_cap_yesterday_percentage_change", DoubleType(), True),
                StructField("quote_total_volume_24h_yesterday_percentage_change", DoubleType(), True),
                # evolution of listed crypto
                StructField("total_crypto_dex_currencies", LongType(), True),
                StructField("today_incremental_crypto_number", LongType(), True),
                StructField("past_24h_incremental_crypto_number", LongType(), True),
                StructField("past_7d_incremental_crypto_number", LongType(), True),
                StructField("past_30d_incremental_crypto_number", LongType(), True),
                StructField("today_change_percent", DoubleType(), True),
                # Extreme values in one year
                StructField("tracked_maxIncrementalNumber", LongType(), True),
                StructField("tracked_maxIncrementalDate", StringType(), True),
                StructField("tracked_minIncrementalNumber", LongType(), True),
                StructField("tracked_minIncrementalDate", StringType(), True),
            ]
        )

        self.log(f"Reading global_metrics snapshot: {latest_snapshot.name}", table_name="global_market_fact")

        try:
            df_raw = self.spark.read.schema(schema).parquet(str(latest_snapshot))
            self.log("Successfully loaded global_metrics raw DataFrame", table_name="global_market_fact")

        except Exception as e:
            self.log(f"[ERROR] Failed to read Parquet file at {latest_snapshot} -> {e}", table_name="global_market_fact")
            return None

        # Step 1: Rename columns for clarity
        df_renamed = (
            df_raw.withColumnRenamed("btc_dominance_24h_percentage_change", "btc_dominance_24h_change")
            .withColumnRenamed("eth_dominance_24h_percentage_change", "eth_dominance_24h_change")
            .withColumnRenamed("quote_total_market_cap", "total_market_cap")
            .withColumnRenamed("quote_total_volume_24h", "total_volume_24h")
            .withColumnRenamed("quote_total_volume_24h_reported", "total_volume_24h_reported")
            .withColumnRenamed("quote_altcoin_market_cap", "altcoin_market_cap")
            .withColumnRenamed("quote_altcoin_volume_24h", "altcoin_volume_24h")
            .withColumnRenamed("quote_altcoin_volume_24h_reported", "altcoin_volume_24h_reported")
            .withColumnRenamed("quote_total_market_cap_yesterday", "total_market_cap_yesterday")
            .withColumnRenamed("quote_total_volume_24h_yesterday", "total_volume_24h_yesterday")
            .withColumnRenamed("quote_total_market_cap_yesterday_percentage_change", "total_market_cap_yesterday_change")
            .withColumnRenamed("quote_total_volume_24h_yesterday_percentage_change", "total_volume_24h_yesterday_change")
            .withColumnRenamed("today_incremental_crypto_number", "new_cryptos_today")
            .withColumnRenamed("past_24h_incremental_crypto_number", "new_cryptos_last_24h")
            .withColumnRenamed("past_7d_incremental_crypto_number", "new_cryptos_last_7d")
            .withColumnRenamed("past_30d_incremental_crypto_number", "new_cryptos_last_30d")
            .withColumnRenamed("today_change_percent", "new_cryptos_today_pct")
            .withColumnRenamed("tracked_maxIncrementalNumber", "max_daily_new_cryptos")
            .withColumnRenamed("tracked_maxIncrementalDate", "max_new_cryptos_day")
            .withColumnRenamed("tracked_minIncrementalNumber", "min_daily_new_cryptos")
            .withColumnRenamed("tracked_minIncrementalDate", "min_new_cryptos_day")
        )
        self.log_dataframe_info(df_renamed, "After renaming columns", table_name="global_market_fact")

        # Step 2: Cast all LongType columns to IntegerType for compatibility and performance
        df_casted = df_renamed
        for field in df_casted.schema.fields:
            if isinstance(field.dataType, LongType):
                df_casted = df_casted.withColumn(field.name, col(field.name).cast(IntegerType()))
        self.log_dataframe_info(df_casted, "After casting LongType to IntegerType", table_name="global_market_fact")

        # Step 3: Compute derived KPIs with null and zero-safety
        df_kpis = (
            df_casted.withColumn(
                "market_liquidity_ratio",
                when(
                    col("total_market_cap").isNotNull() & (abs(col("total_market_cap")) > 1e-6) ,
                    col("total_volume_24h") / col("total_market_cap"),
                ).otherwise(lit(0.0)),
            )
            .withColumn("check_market_liquidity_ratio", col("market_liquidity_ratio") != 0.0)
            .withColumn(
                "defi_volume_share",
                when(
                    col("total_volume_24h").isNotNull() & (abs(col("total_volume_24h")) > 1e-6),
                    col("defi_volume_24h") / col("total_volume_24h"),
                ).otherwise(lit(0.0)),
            )
            .withColumn("check_defi_volume_share", col("defi_volume_share") != 0.0)
            .withColumn(
                "stablecoin_market_share",  
                when(
                    col("total_market_cap").isNotNull() & (abs(col("total_market_cap")) > 1e-6),
                    col("stablecoin_market_cap") / col("total_market_cap"),
                ).otherwise(lit(0.0)),
            )
            .withColumn(
                "check_stablecoin_market_share", col("stablecoin_market_share") != 0.0
            )
            .withColumn(
                "btc_dominance_delta",
                when(
                    col("btc_dominance").isNotNull() & col("btc_dominance_yesterday").isNotNull(),
                    col("btc_dominance") - col("btc_dominance_yesterday"),
                ).otherwise(lit(0.0)),
            )
            .withColumn("check_btc_dominance_delta", col("btc_dominance_delta") != 0.0)
            .withColumn(
                "eth_dominance_delta",
                when(
                    col("eth_dominance").isNotNull() & col("eth_dominance_yesterday").isNotNull(),
                    col("eth_dominance") - col("eth_dominance_yesterday"),
                ).otherwise(lit(0.0)),
            )
            .withColumn("check_eth_dominance_delta", col("eth_dominance_delta") != 0.0)
            .withColumn(
                "crypto_growth_rate_30d",
                when(
                    col("total_cryptocurrencies").isNotNull() & (col("total_cryptocurrencies") != 0),
                    col("new_cryptos_last_30d") / col("total_cryptocurrencies"),
                ).otherwise(lit(0.0)),
            )
            .withColumn("check_crypto_growth_rate_30d",col("crypto_growth_rate_30d") != 0.0)
        )

        self.log_dataframe_info(df_kpis, "After computing derived KPIs", table_name="global_market_fact")

        # Final list of columns to retain in the fact_global_market table (including derived KPIs)
        columns_to_keep = [
            # Metadata
            "snapshot_timestamp",      # Parsed timestamp of data extraction
            "snapshot_str",            # Human-readable string version of snapshot
            "last_updated_timestamp",  # Timestamp from source API (CoinMarketCap)
            "last_updated_str",        # Human-readable version of last_updated

            # Dominance metrics
            "btc_dominance","eth_dominance",
            "btc_dominance_yesterday","eth_dominance_yesterday",
            "btc_dominance_24h_change","eth_dominance_24h_change",
            "btc_dominance_delta",  # Calculated delta from yesterday
            "eth_dominance_delta",
            "check_btc_dominance_delta",  # Sanity check: 1 = valid
            "check_eth_dominance_delta",

            # Global market activity
            "active_cryptocurrencies","total_cryptocurrencies",
            "active_market_pairs","active_exchanges",
            "total_exchanges",

            # DeFi metrics
            "defi_market_cap","defi_volume_24h",
            "defi_volume_24h_reported","defi_24h_percentage_change",
            "defi_volume_share",  # DeFi volume as share of total
            "check_defi_volume_share",

            # Stablecoin metrics
            "stablecoin_market_cap","stablecoin_volume_24h",
            "stablecoin_volume_24h_reported","stablecoin_24h_percentage_change",
            "stablecoin_market_share",  # Stablecoin cap as share of market
            "check_stablecoin_market_share",

            # Derivatives
            "derivatives_volume_24h","derivatives_volume_24h_reported",
            "derivatives_24h_percentage_change",

            # Altcoins
            "altcoin_market_cap","altcoin_volume_24h",
            "altcoin_volume_24h_reported",

            # Global market (USD)
            "total_market_cap","total_volume_24h","total_volume_24h_reported",
            "market_liquidity_ratio",  # Volume / Market Cap
            "check_market_liquidity_ratio",

            # Yesterday's values
            "total_market_cap_yesterday","total_volume_24h_yesterday",
            "total_market_cap_yesterday_change","total_volume_24h_yesterday_change",

            # Listed crypto growth
            "total_crypto_dex_currencies","new_cryptos_today",
            "new_cryptos_last_24h","new_cryptos_last_7d",
            "new_cryptos_last_30d","new_cryptos_today_pct",
            "crypto_growth_rate_30d",  # Share of newly listed cryptos in 30d
            "check_crypto_growth_rate_30d",

            # Historical extremes
            "max_daily_new_cryptos","max_new_cryptos_day",
            "min_daily_new_cryptos","min_new_cryptos_day",
        ]

        # Step 4: Add parsed timestamps and select final schema
        df_final = (
            df_kpis.withColumn("snapshot_timestamp", to_timestamp(col("date_snapshot")))
            .withColumn("snapshot_str", date_format(col("snapshot_timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("last_updated_timestamp", to_timestamp(col("last_updated")))
            .withColumn("last_updated_str", date_format(col("last_updated_timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("max_new_cryptos_day", to_date("max_new_cryptos_day"))
            .withColumn("min_new_cryptos_day", to_date("min_new_cryptos_day"))
            .select(*columns_to_keep)
        )

        self.log_dataframe_info(df_final, "Cleaned fact_global_market", table_name="global_market_fact")

        return df_final, latest_snapshot
