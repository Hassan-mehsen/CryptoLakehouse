from pyspark.sql.functions import col, to_timestamp, split, trim, broadcast, when, lit, size, date_format
from transform.base.base_transformer import BaseTransformer
from pyspark.sql import DataFrame, SparkSession
from typing import Optional, Tuple
from pyspark.sql.types import *


class ExchangeTransformer(BaseTransformer):
    """
    ExchangeTransformer is responsible for transforming raw exchange-related data
    into structured and optimized analytical tables for the Silver layer of the data lake.

    This class handles data from multiple raw sources:
    - exchange_map_data: general metadata about exchanges (id, name, is_active)
    - exchange_info_data: detailed exchange profiles (description, URLs, volumes)
    - exchange_assets_data: wallet-level information for each exchange

    Core responsibilities:
    - Cleaning and casting raw data using explicit schemas
    - Aligning raw field names with the dimensional model (renaming, typing)
    - Calculating derived business KPIs (e.g. `wallet_weight`, `total_usd_value`)
    - Broadcasting enriched dimension data (e.g. `spot_volume_usd`) for reuse across tables
    - Ensuring referential integrity and primary key uniqueness (via `.dropDuplicates()` logic)
    - Writing clean, deduplicated Delta tables into the silver layer (partitioned by logical entity)

    The class separates transformations by target table:
    - `dim_exchange_id`: static identifiers (id, name, is_active)
    - `dim_exchange_info`: descriptive metadata and launch details
    - `dim_exchange_map`: operational and market metadata (fees, volume)
    - `fact_exchange_assets`: wallet-level metrics, enriched with exchange volume

    Design principles:
    - Each transformation step is isolated in its own method (`__prepare_*`)
    - **Metadata about each transformation** (duration, input snapshot, status) is tracked
    - **Fault-tolerance** is built in: missing or invalid inputs do not crash the pipeline
    - **Reusability and maintainability** are favored through shared logic and logging

    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark)

        # Stores the broadcasted metrics DataFrame from dim_exchange_info,
        # used in dim_exchange_map to inject KPIs (visits, fees) and timestamps
        self.broadcasted_exchange_info_df = None

        # Stores the broadcasted status DataFrame from dim_exchange_id,
        # used in dim_exchange_map to add the `is_active` exchange flag
        self.broadcasted_exchange_id_df = None

    # --------------------------------------------------------------------
    #                          Public entrypoints
    # --------------------------------------------------------------------

    def build_exchange_id_dim(self) -> None:
        """
        Triggers the transformation pipeline for the dim_exchange_id table.

        This method:
        - Loads the latest snapshot from the `exchange_map_data` folder
        - Skips transformation if the snapshot is already processed (via metadata check)
        - Delegates the actual transformation to `__prepare_dim_exchange_id_df` via `_run_build_step`. 
        """
        self.log(style="\n")

        snapshot_paths = self.find_latest_data_files("exchange_map_data")
        if not snapshot_paths:
            self.log("No snapshot available for dim_exchange_id. Skipping.", table_name="exchange_id_dim")
            return

        latest_snapshot = snapshot_paths[0]
        if not self.should_transform("dim_exchange_id", latest_snapshot, daily_comparison=True):
            self.log("dim_exchange_id is up to date. Skipping transformation.", table_name="exchange_id_dim")
            return

        self._run_build_step("dim_exchange_id", lambda: self.__prepare_dim_exchange_id_df(latest_snapshot), "dim_exchange_id")

    def build_exchange_info_dim(self) -> None:
        """
        Triggers the transformation pipeline for the dim_exchange_info table.

        This method:
        - Loads the latest snapshot from the `exchange_info_data` folder
        - Skips transformation if the snapshot is already processed (via metadata check)
        - Delegates the actual transformation to `__prepare_dim_exchange_info_df` via `_run_build_step`. 
        """
        self.log(style="\n")

        snapshot_paths = self.find_latest_data_files("exchange_info_data")
        if not snapshot_paths:
            self.log("No snapshot available for dim_exchange_info. Skipping.", table_name="exchange_info_dim")
            return

        latest_snapshot = snapshot_paths[0]
        if not self.should_transform("dim_exchange_info", latest_snapshot, daily_comparison=True):
            self.log("dim_exchange_info is up to date. Skipping transformation.", table_name="exchange_info_dim")
            return

        self._run_build_step(
            "dim_exchange_info", lambda: self.__prepare_dim_exchange_info_df(latest_snapshot), "dim_exchange_info"
        )

    def build_exchange_map_dim(self) -> None:
        """
        Triggers the transformation pipeline for the dim_exchange_map table.

        This method:
        - Depends entirely on the broadcasted data from `build_exchange_info_dim`
        - Does not perform snapshot freshness check, since the broadcasted data is transient
        - Delegates the actual transformation to `__prepare_dim_exchange_map_df` via `_run_build_step`. 
        """
        self.log(style="\n")

        self._run_build_step("dim_exchange_map", self.__prepare_dim_exchange_map_df, "dim_exchange_map")

    def build_exchange_assets_fact(self) -> None:
        """
        Triggers the transformation pipeline for the fact_exchange_assets table.

        This method:
        - Loads the latest snapshot from `exchange_assets_data`
        - Skips transformation if the snapshot was already processed (checked via metadata timestamp)
        - Delegates the actual transformation to `__prepare_exchange_assets_df` via `_run_build_step`. 
        """
        self.log(style="\n")
        
        snapshot_paths = self.find_latest_data_files("exchange_assets_data")
        if not snapshot_paths:
            self.log("No snapshot available for fact_exchange_assets. Skipping.", table_name="exchange_assets_fact")
            return

        latest_snapshot = snapshot_paths[0]
        if not self.should_transform("fact_exchange_assets", latest_snapshot, daily_comparison=False):
            self.log("fact_exchange_assets is up to date. Skipping transformation.", table_name="exchange_assets_fact")
            return

        self._run_build_step(
            "fact_exchange_assets",
            lambda: self.__prepare_exchange_assets_df(latest_snapshot),
            "fact_exchange_assets",
        )

    def run(self):
        """
        Executes the full transformation pipeline for the exchange domain in sequence.

        This method is intended for local testing or manual execution.
        In production, transformations are grouped by frequency and triggered individually through DAG orchestration.
        """
        self.log_section("Running ExchangeTransformer")
        self.build_exchange_id_dim()
        self.build_exchange_info_dim()
        self.build_exchange_map_dim()
        self.build_exchange_assets_fact()
        self.log_section("End ExchangeTransformer")

    # --------------------------------------------------------------------
    #                    Internal Transformation Methods
    # --------------------------------------------------------------------

    def __prepare_dim_exchange_id_df(self, latest_snapshot: str) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the dim_exchange_id table from the latest exchange_map snapshot.

        This method:
        - Reads and validates the latest Parquet snapshot from 'exchange_map_data'
        - Extracts and casts stable identity fields: exchange_id and name
        - Builds a cleaned, deduplicated DataFrame for `dim_exchange_id`
        - Separately prepares a broadcasted DataFrame (exchange_id, is_active)
        for enriching `dim_exchange_map`

        Returns:
            Optional[Tuple[DataFrame, str]]:
                - Cleaned DataFrame for `dim_exchange_id`
                - Path to the snapshot used
        """
        self.log("> Starting transformation: exchange_id... ", table_name="exchange_id_dim")

        # Define strict schema
        schema = StructType([
                    StructField("id", LongType(), True),
                    StructField("name", StringType(), True),
                    StructField("is_active", LongType(), True),
                ])
        
        self.log(f"Reading latest exchange_map snapshot: {latest_snapshot}", table_name="exchange_id_dim")

        # Read with schema
        try:
            df_raw = self.spark.read.schema(schema).parquet(str(latest_snapshot))

        except Exception as e:
            self.log(f"[ERROR] Failed to read Parquet file at {latest_snapshot} -> {e}", table_name="exchange_id_dim")
            return None

        # --- Build dim_exchange_id table ---
        df_cleaned = (
            df_raw.withColumn("exchange_id", df_raw["id"].cast("int")).drop("id", "is_active").dropna(subset=["exchange_id"])
        )
        self.log_dataframe_info(df_cleaned, "Cleaned dim_exchange_id", table_name="exchange_id_dim")

        # Build broadcasted version for map (with is_active)
        broadcast_id_df = (
            df_raw.withColumn("exchange_id", col("id").cast("int"))
            .withColumn("is_active", col("is_active").cast("boolean"))
            .drop("id", "name")
            .dropna(subset=["exchange_id"])
        )

        self.log_dataframe_info(broadcast_id_df, "Cleaned broadcast_id_df", table_name="exchange_id_dim")

        self.broadcasted_exchange_id_df = broadcast(broadcast_id_df)

        return df_cleaned, latest_snapshot

    def __prepare_dim_exchange_info_df(self, latest_snapshot: str) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the dim_exchange_info table from the latest exchange_info snapshot.

        This method:
        - Reads and validates the latest Parquet snapshot from 'exchange_info_data'
        - Extracts and casts descriptive metadata (slug, description, launch date, URLs, fiats)
        - Constructs the cleaned `dim_exchange_info` table
        - Builds a separate broadcasted DataFrame (KPIs, snapshot date) for reuse in
        `dim_exchange_map` and `fact_exchange_assets`

        Returns:
            Optional[Tuple[DataFrame, str]]:
                - Cleaned DataFrame for `dim_exchange_info`
                - Path to the snapshot used
        """
        self.log("> Starting transformation: exchange_info... ", table_name="exchange_info_dim")

        # Define expected schema
        schema = StructType([
                    StructField("id", LongType(), True),  
                    StructField("slug", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("date_launched", StringType(), True),
                    StructField("logo", StringType(), True),
                    StructField("weekly_visits", DoubleType(), True),   
                    StructField("spot_volume_usd", DoubleType(), True),
                    StructField("maker_fee", DoubleType(), True),       
                    StructField("taker_fee", DoubleType(), True),       
                    StructField("urls",StructType([
                                            StructField("blog", ArrayType(StringType()), True),
                                            StructField("fee", ArrayType(StringType()), True),
                                            StructField("twitter", ArrayType(StringType()), True),
                                            StructField("website", ArrayType(StringType()), True),
                                        ]),
                        True),
                    StructField("fiats", StringType(), True),
                    StructField("date_snapshot", StringType(), True),
                ])

        self.log(f"Reading latest exchange_info snapshot: {latest_snapshot.name}", table_name="exchange_info_dim")

        try:
            df_raw = self.spark.read.schema(schema).parquet(str(latest_snapshot))
            self.log("Successfully loaded exchange_info raw DataFrame", table_name="exchange_info_dim")

        except Exception as e:
            self.log(f"[ERROR] Failed to read Parquet file at {latest_snapshot} -> {e}", table_name="exchange_info_dim")
            return None

        # --- Build dim_exchange_info table ---
        self.log("Preparing dim_exchange_info DataFrame...", table_name="exchange_info_dim")

        columns_to_keep = [
            "exchange_id",
            "slug",
            "date_launched",
            "description",
            "logo_url",
            "website_url",
            "twitter_url",
            "fee_url",
            "blog_url",
            "fiats",
        ]

        dim_exchange_info_df = (
            df_raw.withColumn("exchange_id", col("id").cast("int"))
            .withColumn("date_launched", date_format(col("date_launched"), "yyyy-MM-dd HH:mm:ss"))
            .withColumnRenamed("logo", "logo_url")
            .withColumn(
                "website_url",
                when(
                    col("urls").isNotNull() & col("urls.website").isNotNull() & (size(col("urls.website")) > 0),
                    col("urls.website")[0],
                ).otherwise(lit(None)),
            )
            .withColumn(
                "twitter_url",
                when(
                    col("urls").isNotNull() & col("urls.twitter").isNotNull() & (size(col("urls.twitter")) > 0),
                    col("urls.twitter")[0],
                ).otherwise(lit(None)),
            )
            .withColumn(
                "fee_url",
                when(
                    col("urls").isNotNull() & col("urls.fee").isNotNull() & (size(col("urls.fee")) > 0), col("urls.fee")[0]
                ).otherwise(lit(None)),
            )
            .withColumn(
                "blog_url",
                when(
                    col("urls").isNotNull() & col("urls.blog").isNotNull() & (size(col("urls.blog")) > 0), col("urls.blog")[0]
                ).otherwise(lit(None)),
            )
            .withColumn("fiats", when(col("fiats").isNotNull(), split(trim(col("fiats")), r",\s*")).otherwise(lit(None)))
            .select(*columns_to_keep)
            .dropna(subset=["exchange_id"])
            .drop_duplicates(subset=["exchange_id"])
        )

        self.log_dataframe_info(dim_exchange_info_df, "Cleaned dim_exchange_info", table_name="exchange_info_dim")

        # --- Broadcast useful columns for other tables ---
        self.log("Preparing broadcasted DataFrame for other tables...", table_name="exchange_info_dim")

        broadcast_columns = [
            "exchange_id",
            "snapshot_timestamp",
            "snapshot_str",
            "weekly_visits",
            "spot_volume_usd",
            "maker_fee",
            "taker_fee",
        ]

        broadcast_df = (
            df_raw.withColumn("exchange_id", col("id").cast("int"))
            .withColumn("weekly_visits", col("weekly_visits").cast("int"))
            .withColumn("maker_fee", col("maker_fee").cast("float"))
            .withColumn("taker_fee", col("taker_fee").cast("float"))
            .withColumn("snapshot_timestamp", to_timestamp("date_snapshot"))
            .withColumn("snapshot_str", date_format(col("snapshot_timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .select(*broadcast_columns)
            .dropna(subset=["exchange_id"])
        )

        self.log_dataframe_info(broadcast_df, "Cleaned broadcast_df", table_name="exchange_info_dim")

        # Broadcast the DataFrame globally (available to be reused across joins)
        # NOTE: self.broadcasted_exchange_info_df does NOT store the actual DataFrame,
        # but a reference (logical plan) to a broadcasted DataFrame.
        self.broadcasted_exchange_info_df = broadcast(broadcast_df)

        return dim_exchange_info_df, latest_snapshot

    def __prepare_dim_exchange_map_df(self) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the dim_exchange_map table by joining broadcasted metadata.

        This method:
        - Uses `broadcasted_exchange_info_df` (from `exchange_info`)
        to extract KPIs: weekly_visits, fees, snapshot_timestamp
        - Optionally joins with `broadcasted_exchange_id_df` (from `exchange_id`)
        to add the `is_active` column
        - Applies deduplication on (exchange_id, snapshot_timestamp)

        Returns:
            Optional[Tuple[DataFrame, str]]:
                - Cleaned and enriched DataFrame for `dim_exchange_map`
                - None (no static snapshot path used)
        """
        self.log(
            "> Starting transformation: dim_exchange_map (append mode from broadcasted info)...", table_name="dim_exchange_map"
        )

        self.log("Preparing dim_exchange_map DataFrame", table_name="dim_exchange_map")
        if self.broadcasted_exchange_info_df is not None:
            df = (
                self.broadcasted_exchange_info_df.select(
                    "exchange_id", "snapshot_timestamp", "snapshot_str", "weekly_visits", "maker_fee", "taker_fee"
                )
                .dropna(subset=["exchange_id", "snapshot_timestamp"])
                .drop_duplicates(subset=["exchange_id", "snapshot_timestamp"])
            )
        else:
            self.log(
                "[WARNING] broadcasted_exchange_info_df is not available. Please run __prepare_dim_exchange_info_df() first.",
                table_name="dim_exchange_map",
            )
            return None, None

        if self.broadcasted_exchange_id_df is not None:
            df = df.join(self.broadcasted_exchange_id_df, on="exchange_id", how="left")

        self.log_dataframe_info(df, "Cleaned dim_exchange_map", table_name="dim_exchange_map")
        return df, None

    def __prepare_exchange_assets_df(self, latest_snapshot: str) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the fact_exchange_assets table from the latest exchange_assets snapshot.

        This method:
        - Loads and validates wallet-level metrics for each exchange
        - Renames fields and casts types to match analytical model
        - Enriches the dataset with broadcasted exchange volume (spot_volume_usd)
        - Computes derived KPIs: total_usd_value, wallet_weight
        - Applies deduplication on the composite key (exchange_id, wallet_address, crypto_id, snapshot_timestamp)

        Returns:
            Optional[Tuple[DataFrame, str]]:
                - Transformed and enriched DataFrame for `fact_exchange_assets`
                - Path to the snapshot used
        """
        self.log("Processing exchange_assets", table_name="exchange_assets_fact")

        # Define expected schema
        schema = StructType([
                        StructField("exchange_id", LongType(), True),
                        StructField("date_snapshot", StringType(), True),
                        StructField("platform_symbol", StringType(), True),
                        StructField("platform_name", StringType(), True),
                        StructField("wallet_address", StringType(), True),
                        StructField("currency_crypto_id", LongType(), True),
                        StructField("balance", DoubleType(), True),
                        StructField("currency_price_usd", DoubleType(), True),
                    ])

        self.log(f"Reading latest exchange_assets snapshot: {latest_snapshot.name}", table_name="exchange_assets_fact")

        # Read the data with the explicite schema
        try:
            df_raw = self.spark.read.schema(schema).parquet(str(latest_snapshot))
            self.log("Successfully loaded exchange_assets raw DataFrame", table_name="exchange_assets_fact")

        except Exception as e:
            self.log(f"[ERROR] Failed to read Parquet file at {latest_snapshot} -> {e}", table_name="exchange_assets_fact")
            return None

        self.log("Preparing fact_exchange_assets DataFrame...", table_name="exchange_assets_fact")

        columns_to_keep = [
            "exchange_id",
            "snapshot_timestamp",
            "snapshot_str",
            "spot_volume_usd",
            "blockchain_symbol",
            "blockchain_name",
            "wallet_address",
            "crypto_id",
            "balance",
            "currency_price_usd",
            "total_usd_value",
            "wallet_weight",
        ]

        # Inject 'spot_volume_usd' from broadcasted dataframe if available, else use none
        if self.broadcasted_exchange_info_df is not None:
            df_with_volume = df_raw.join(
                self.broadcasted_exchange_info_df.select("exchange_id", "spot_volume_usd"), on="exchange_id", how="left"
            )
        else:
            self.log(
                "[WARNING] broadcasted_exchange_info_df is None — using Null as fallback for spot_volume_usd.",
                table_name="exchange_assets_fact",
            )
            df_with_volume = df_raw.withColumn("spot_volume_usd", lit(None))

        df_transformed = (
            df_with_volume.withColumn("exchange_id", col("exchange_id").cast("int"))
            .withColumn("crypto_id", col("currency_crypto_id").cast("int"))
            .withColumn("snapshot_timestamp", to_timestamp("date_snapshot"))
            .withColumn("snapshot_str", date_format(col("snapshot_timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumnRenamed("platform_symbol", "blockchain_symbol")
            .withColumnRenamed("platform_name", "blockchain_name")
            .dropna(subset=["exchange_id", "crypto_id", "wallet_address", "snapshot_timestamp"])
            .dropDuplicates(["exchange_id", "wallet_address", "crypto_id", "snapshot_timestamp"])
        )

        # Add derived KPIs
        df_enriched = (
            df_transformed
            # KPI 1 : total_usd_value
            .withColumn(
                "total_usd_value",
                when(
                    col("balance").isNotNull() & col("currency_price_usd").isNotNull(), col("balance") * col("currency_price_usd")
                ).otherwise(None),
            )
            # KPI 2 : wallet_weight
            .withColumn(
                "wallet_weight",
                when(
                    col("spot_volume_usd").isNotNull()
                    & (col("spot_volume_usd") != 0)
                    & col("balance").isNotNull()
                    & col("currency_price_usd").isNotNull(),
                    (col("balance") * col("currency_price_usd")) / col("spot_volume_usd"),
                ).otherwise(None),
            ).select(*columns_to_keep)
        )

        self.log_dataframe_info(df_enriched, "Cleaned fact_exchange_assets", table_name="exchange_assets_fact")

        return df_enriched, latest_snapshot
