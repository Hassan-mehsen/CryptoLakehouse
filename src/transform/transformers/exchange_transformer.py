from pyspark.sql.functions import col, to_timestamp, split, trim, broadcast, when, lit
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
    - Metadata about each transformation (duration, input snapshot, status) is tracked
    - Fault-tolerance is built in: missing or invalid inputs do not crash the pipeline
    - Reusability and maintainability are favored through shared logic and logging

    This class is typically invoked as part of a DAG step in a Spark ETL job or notebook pipeline.
    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark)
    
        self.broadcasted_exchange_info_df = None

    def build_exchange_id_dim(self) -> None:
        """
        Triggers the transformation pipeline for the dim_exchange_id table.

        Delegates to:
        - `__prepare_dim_exchange_id_df` for reading and cleaning the latest snapshot
        - `_run_build_step` for write, logging, and metadata tracking

        This is a high-level entrypoint used by the main run() orchestration.
        """
        self._run_build_step("dim_exchange_id", self.__prepare_dim_exchange_id_df, "dim_exchange_id")

    def build_exchange_info_dim(self) -> None:
        """
        Orchestrates the transformation of the dim_exchange_info table.

        This high-level method:
        - Prepares the cleaned exchange info DataFrame
        - Broadcasts selected fields for reuse
        - Delegates write and metadata tracking to `_run_build_step`
        """
        self._run_build_step("dim_exchange_info", self.__prepare_dim_exchange_info_df, "dim_exchange_info")

    def build_exchange_map_dim(self) -> None:
        """
        Coordinates the creation of the dim_exchange_map table.

        Relies on the broadcasted exchange_info data to:
        - Extract metrics (fees, visits) for each exchange
        - Delegate writing and metadata to `_run_build_step`
        """
        self._run_build_step("dim_exchange_map", self.__prepare_dim_exchange_map_df, "dim_exchange_map", mode="append")

    def build_exchange_assets_fact(self) -> None:
        """
        Initiates the transformation for the fact_exchange_assets table.

        This method:
        - Joins exchange asset data with broadcasted exchange info
        - Computes derived wallet-level KPIs
        - Delegates write and metadata logic to `_run_build_step`
        """
        self._run_build_step("fact_exchange_assets", self.__prepare_exchange_assets_df, "fact_exchange_assets", mode="append")

    def run(self):
        """
        Entry point for executing the full transformation workflow for the exchange domain.

        This method runs all transformation pipelines sequentially:
        - dim_exchange_id
        - dim_exchange_info
        - dim_exchange_map
        - fact_exchange_assets

        Each step logs its own progress and writes structured metadata.
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

    def __prepare_dim_exchange_id_df(self) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the dim_exchange_id table from the latest exchange_map snapshot.

        - Reads the most recent Parquet file from the exchange_map_data folder
        - Applies a strict schema for validation
        - Renames and casts fields to match target model
        - Drops rows with null values in critical columns (exchange_id, name)
        - Logs the input and output DataFrame stats

        Returns:
        Optional[Tuple[DataFrame, str]]: Cleaned DataFrame and path to snapshot,
        or None if no file or read error occurs.
        """
        self.log(style="\n")
        self.log("Processing exchange_id... \n")

        # Define strict schema
        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("name", StringType(), True),
                StructField("is_active", LongType(), True),
            ]
        )

        # Locate latest snapshot file
        data_paths = self.find_latest_data_files("exchange_map_data")
        if not data_paths:
            self.log("No snapshot found in 'exchange_map_data' folder. Skipping dim_exchange_id transformation.")
            return None

        latest_snapshot = data_paths[0]
        self.log(f"Reading latest exchange_map snapshot: {latest_snapshot.name}")

        # Read with schema
        try:
            df_raw = self.spark.read.schema(schema).parquet(str(latest_snapshot))
        except Exception as e:
            self.log(f"[ERROR] Failed to read Parquet file at {latest_snapshot} → {e}")
            return None

        df_cleaned = (
            df_raw.withColumn("exchange_id", df_raw["id"].cast("int"))
            .drop("id")
            .withColumn("is_active", df_raw["is_active"].cast("boolean"))
            .dropna(subset=["exchange_id", "name"])
        )
        self.log_dataframe_info(df_cleaned, "Cleaned dim_exchange_id")

        return df_cleaned, latest_snapshot

    def __prepare_dim_exchange_info_df(self) -> Optional[Tuple[DataFrame, str]]:
        """
        Reads and processes the latest snapshot of the exchange_info dataset.

        - Applies a strict schema to the source Parquet file.
        - Cleans and selects relevant columns for the dim_exchange_info dimension table.
        - Casts and renames fields to match the data model.
        - Extracts a subset of columns used in other tables and broadcasts them for join optimization.

        Returns:
            DataFrame: A cleaned DataFrame ready to be written to silver/dim_exchange_info
                    or None if no snapshot is found or reading fails.
        """
        self.log(style="\n")
        self.log("Processing exchange_info... \n")

        # Define expected schema
        schema = StructType(
            [
                StructField("id", LongType(), True),  # To be cast to Integer
                StructField("slug", StringType(), True),
                StructField("description", StringType(), True),
                StructField("date_launched", StringType(), True),
                StructField("logo", StringType(), True),
                StructField("weekly_visits", DoubleType(), True),  # To be cast to Integer
                StructField("spot_volume_usd", DoubleType(), True),
                StructField("maker_fee", DoubleType(), True),  # To be cast to Float
                StructField("taker_fee", DoubleType(), True),  # To be cast to Float
                StructField(
                    "urls",
                    StructType(
                        [
                            StructField("blog", ArrayType(StringType()), True),
                            StructField("fee", ArrayType(StringType()), True),
                            StructField("twitter", ArrayType(StringType()), True),
                            StructField("website", ArrayType(StringType()), True),
                        ]
                    ),
                    True,
                ),
                StructField("fiats", StringType(), True),
                StructField("date_snapshot", StringType(), True),
            ]
        )

        # Locate latest snapshot
        snapshot_paths = self.find_latest_data_files("exchange_info_data")
        if not snapshot_paths:
            self.log("No snapshot found in 'exchange_info_data'. Skipping dim_exchange_info transformation.")
            return None

        latest_snapshot = snapshot_paths[0]
        self.log(f"Reading latest exchange_info snapshot: {latest_snapshot.name}")

        try:
            df_raw = self.spark.read.schema(schema).parquet(str(latest_snapshot))
            self.log("Successfully loaded exchange_info raw DataFrame")
        except Exception as e:
            self.log(f"[ERROR] Failed to read Parquet file at {latest_snapshot} → {e}")
            return None

        # --- Build dim_exchange_info table ---
        self.log("Preparing dim_exchange_info DataFrame...")

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
            .withColumn("date_launched", to_timestamp("date_launched"))
            .withColumnRenamed("logo", "logo_url")
            .withColumn("website_url", col("urls.website")[0])
            .withColumn("twitter_url", col("urls.twitter")[0])
            .withColumn("fee_url", col("urls.fee")[0])
            .withColumn("blog_url", col("urls.blog")[0])
            .withColumn("fiats", split(trim(col("fiats")), r",\s*"))
            .select(*columns_to_keep)
            .dropna(subset=["exchange_id"])
        )

        self.log_dataframe_info(dim_exchange_info_df, "Cleaned dim_exchange_info")

        # --- Broadcast useful columns for other tables ---
        self.log("Preparing broadcasted DataFrame for other tables...")

        broadcast_columns = [
            "exchange_id",
            "weekly_visits",
            "spot_volume_usd",
            "maker_fee",
            "taker_fee",
            "date_snapshot",
        ]

        broadcast_df = (
            df_raw.withColumn("exchange_id", col("id").cast("int"))
            .withColumn("spot_volume_usd", col("spot_volume_usd"))
            .withColumn("weekly_visits", col("weekly_visits").cast("int"))
            .withColumn("maker_fee", col("maker_fee").cast("float"))
            .withColumn("taker_fee", col("taker_fee").cast("float"))
            .withColumn("date_snapshot", to_timestamp("date_snapshot"))
            .select(*broadcast_columns)
            .dropna(subset=["exchange_id"])
        )

        self.log_dataframe_info(broadcast_df, "Cleaned broadcast_df")

        # Broadcast the DataFrame globally (available to be reused across joins)
        # NOTE: self.broadcasted_exchange_info_df does NOT store the actual DataFrame,
        # but a reference (logical plan) to a broadcasted DataFrame.
        self.broadcasted_exchange_info_df = broadcast(broadcast_df)

        return dim_exchange_info_df, latest_snapshot

    def __prepare_dim_exchange_map_df(self) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the dim_exchange_map table from the broadcasted exchange_info data.

        This method relies on `broadcasted_exchange_info_df`, which is produced during the
        execution of `__prepare_dim_exchange_info_df()`. It extracts only the columns
        relevant for the dim_exchange_map table from this shared broadcasted DataFrame.

        Returns:
            DataFrame: A cleaned DataFrame with selected columns for dim_exchange_map
            or None if the broadcasted DataFrame is not available.
        """
        self.log(style="\n")
        self.log("Processing exchange_map...\n")

        if not hasattr(self, "broadcasted_exchange_info_df"):
            self.log("[ERROR] broadcasted_exchange_info_df is not available. Please run __prepare_dim_exchange_info_df() first.")
            return None

        self.log("Preparing dim_exchange_map DataFrame")

        df = (
            self.broadcasted_exchange_info_df.select("exchange_id", "weekly_visits", "maker_fee", "taker_fee", "date_snapshot")
            .dropna(subset=["exchange_id", "date_snapshot"])
            .drop_duplicates(subset=["exchange_id", "date_snapshot"])
        )

        self.log_dataframe_info(df, "Cleaned dim_exchange_map")
        return df, None

    def __prepare_exchange_assets_df(self) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the fact_exchange_assets table from the latest snapshot.

        This method :
        - Loads and validates data from 'exchange_assets_data'
        - Casts string timestamp to actual timestamp
        - Renames technical fields to domain terms (e.g., platform_* → blockchain_*)
        - Enriches each row with the corresponding `spot_volume_usd` from the broadcasted DataFrame
        - Calculates new KPIs for downstream analytics:
            * `total_usd_value`: monetary value held in the wallet (balance × price)
            * `wallet_weight`: relative importance of the wallet within the exchange (value / total volume)
        - Filters out invalid rows with missing essential fields
        - Logs row count and schema after transformation
        - Ensures uniqueness by dropping duplicates based on the composite primary key
          (`exchange_id`, `wallet_address`, `crypto_id`, `date_snapshot`)
        - Selects and orders final columns for writing to Delta Lake

        Returns:
            Optional[DataFrame]: Transformed and cleaned DataFrame, or None if snapshot missing.
        """
        self.log(style="\n")
        self.log("Processing exchange_assets\n")

        # Define expected schema
        schema = StructType(
            [
                StructField("exchange_id", LongType(), True),
                StructField("date_snapshot", StringType(), True),
                StructField("platform_symbol", StringType(), True),
                StructField("platform_name", StringType(), True),
                StructField("wallet_address", StringType(), True),
                StructField("currency_crypto_id", LongType(), True),
                StructField("balance", DoubleType(), True),
                StructField("currency_price_usd", DoubleType(), True),
            ]
        )

        # Locate latest snapshot
        snapshot_paths = self.find_latest_data_files("exchange_assets_data")
        if not snapshot_paths:
            self.log("No snapshot found in 'exchange_assets_data'. Skipping fact_exchange_assets transformation.")
            return None

        latest_snapshot = snapshot_paths[0]
        self.log(f"Reading latest exchange_assets snapshot: {latest_snapshot.name}")

        # Read the data with the explicite schema
        try:
            df_raw = self.spark.read.schema(schema).parquet(str(latest_snapshot))
            self.log("Successfully loaded exchange_assets raw DataFrame")
        except Exception as e:
            self.log(f"[ERROR] Failed to read Parquet file at {latest_snapshot} → {e}")
            return None

        self.log("Preparing fact_exchange_assets DataFrame...")

        columns_to_keep = [
            "exchange_id",
            "spot_volume_usd",
            "date_snapshot",
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
            self.log("[WARNING] broadcasted_exchange_info_df is None — using Null as fallback for spot_volume_usd.")
            df_with_volume = df_raw.withColumn("spot_volume_usd", lit(None))

        # Below a description of the added column's :
        # total_usd_value = total USD value held in this wallet
        # wallet_weight =   relative to the total spot volume
        df_transformed = (
            df_with_volume.withColumn("exchange_id", col("exchange_id").cast("int"))
            .withColumn("crypto_id", col("currency_crypto_id").cast("int"))
            .withColumn("date_snapshot", to_timestamp(col("date_snapshot")))
            .withColumn("total_usd_value", col("balance") * col("currency_price_usd"))
            .withColumn(
                "wallet_weight",
                when(col("spot_volume_usd").isNotNull(), col("total_usd_value") / col("spot_volume_usd")).otherwise(None),
            )
            .withColumnRenamed("platform_symbol", "blockchain_symbol")
            .withColumnRenamed("platform_name", "blockchain_name")
            .dropna(subset=["exchange_id", "crypto_id", "wallet_address", "date_snapshot"])
            .dropDuplicates(["exchange_id", "wallet_address", "crypto_id", "date_snapshot"])
            .select(*columns_to_keep)
        )

        self.log_dataframe_info(df_transformed, "Cleaned fact_exchange_assets")

        return df_transformed, latest_snapshot
