from pyspark.sql.functions import col, to_timestamp, split, broadcast, when, lit, date_format, to_date
from transform.base.base_transformer import BaseTransformer
from pyspark.sql import DataFrame, SparkSession
from typing import Optional, Tuple, List
from pyspark.sql.types import *


class CryptoTransformer(BaseTransformer):
    """
    Handles the transformation pipeline for all cryptocurrency-related datasets,
    preparing structured and enriched tables for the silver layer and the data warehouse.

    This transformer processes data from the following CoinMarketCap endpoints:
    - /cryptocurrency/map
    - /cryptocurrency/info
    - /cryptocurrency/listings/latest
    - /cryptocurrency/categories
    - /cryptocurrency/category

    Main responsibilities:
    - Enforcing schemas and normalizing raw data formats
    - Cleansing data (null handling, type casting, de-duplication)
    - Computing key financial KPIs (e.g., market cap ratios, price variations, supply metrics)
    - Broadcasting intermediate datasets for reuse across transformations
    - Generating both dimension and fact tables aligned with the DWH schema
    - Writing outputs to partitioned Delta Lake tables (silver layer)
    - Managing snapshot dependencies and ensuring transformation idempotency
    - Logging metadata and lineage information for audit and traceability

    This transformer is designed to be modular, scalable, and cloud-ready.
    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark)

        # Stores the latest snapshot path from /cryptocurrency/map,
        # shared between dim_crypto_id and dim_crypto_map transformations
        self.last_map_snapshot = None

        # Stores the latest snapshot path from /cryptocurrency/categories and /category,
        # shared between dim_crypto_categories and fact_crypto_category
        self.last_categories_snapshot = None

        # Stores the broadcasted metrics DataFrame from crypto categories,
        # used later for computing fact_crypto_category KPIs
        self.broadcasted_crypto_categories_metrics = None

    # --------------------------------------------------------------------
    #                          Public entrypoints
    # --------------------------------------------------------------------

    def run(self):
        """
        Executes the full transformation pipeline for the crypto domain in sequence.

        This method is intended for local testing or manual execution.
        In production, transformations are triggered individually through DAG orchestration.

        Returns:
            None
        """
        self.log_section("Running CryptoTransformer")
        self.build_crypto_id_dim()
        self.build_crypto_map_dim()
        self.build_crypto_info_dim()
        self.build_fact_crypto_market()
        self.build_crypto_category_link()
        self.build_dim_crypto_category()
        self.build_fact_crypto_category()
        self.log_section("End CryptoTransformer")

    def build_crypto_id_dim(self) -> None:
        """
        Orchestrates the transformation of the `dim_crypto_id` table from the latest crypto_map snapshot.

        Responsibilities:
        - Retrieves the most recent snapshot file from the `bronze/data/crypto_map_data` directory
        - Skips the transformation if the data has already been processed (based on metadata and snapshot freshness)
        - Delegates the actual transformation to `__prepare_dim_crypto_id_df()` via `_run_build_step`

        Notes:
        - Updates `self.last_map_snapshot` to maintain snapshot context across dependent transformations
        - This method should be executed before `build_crypto_map_dim()` to ensure consistency in the snapshot lineage

        Returns:
            None
        """

        self.log(style="\n")

        snapshot_paths = self.find_latest_data_files("crypto_map_data")
        if not snapshot_paths:
            self.log("No snapshot available for dim_crypto_id. Skipping", table_name="crypto_id_dim")
            return None

        last_snapshot = snapshot_paths[0]
        self.last_map_snapshot = last_snapshot

        if last_snapshot is None:
            self.log("[ERROR] No snapshot set for crypto_id. Aborting.", table_name="crypto_id_dim")
            return

        if not self.should_transform("dim_crypto_id", last_snapshot, force=False, daily_comparison=True):
            self.log("dim_crypto_id is up to date. Skipping transformation.", table_name="crypto_id_dim")
            return

        self._run_build_step("dim_crypto_id", lambda: self.__prepare_dim_crypto_id_df(last_snapshot), "dim_crypto_id")

    def build_crypto_map_dim(self) -> None:
        """
        Builds the `dim_crypto_map` table using the same snapshot previously assigned via `build_crypto_id_dim()`.

        Responsibilities:
        - Validates that `self.last_map_snapshot` is set (dependency on `build_crypto_id_dim()`)
        - Checks whether the snapshot has already been processed using metadata freshness
        - Delegates transformation to `__prepare_dim_crypto_map_df()` via `_run_build_step`

        Notes:
        - This method must be called after `build_crypto_id_dim()` to maintain snapshot consistency across the crypto identity pipeline
        - Ensures aligned and traceable lineage between ID mapping and rank/map information

        Returns:
            None
        """

        self.log(style="\n")

        if self.last_map_snapshot is None:
            self.log(
                "[ERROR] No snapshot set for crypto_map. Please run build_crypto_id_dim() first.", table_name="crypto_map_dim"
            )
            return

        if not self.should_transform("dim_crypto_map", self.last_map_snapshot, force=False, daily_comparison=True):
            self.log("dim_crypto_map is up to date. Skipping transformation.", table_name="crypto_map_dim")
            return

        self._run_build_step("dim_crypto_map", self.__prepare_dim_crypto_map_df, "dim_crypto_map")

    def build_crypto_info_dim(self) -> None:
        """
        Builds the `dim_crypto_info` table from the latest available snapshot in the `crypto_info_data` directory.

        Responsibilities:
        - Loads the most recent snapshot file from the Bronze layer
        - Verifies whether the snapshot has already been transformed using metadata freshness
        - Delegates the actual transformation logic to `__prepare_dim_crypto_info_df()` via `_run_build_step`
        - Updates `self.last_map_snapshot` with the current snapshot path for consistency across related methods

        Notes:
        - This dimension includes technical metadata, descriptions, URLs, launch dates, and classification fields
        - Should be run after `build_crypto_id_dim()` to maintain alignment across crypto dimensions

        Returns:
            None
        """
        self.log(style="\n")

        snapshot_paths = self.find_latest_data_files("crypto_info_data")
        if not snapshot_paths:
            self.log("No snapshot available for dim_crypto_info. Skipping", table_name="crypto_info_dim")
            return None

        last_snapshot = snapshot_paths[0]
        self.last_map_snapshot = last_snapshot

        if last_snapshot is None:
            self.log("[ERROR] No snapshot set for dim_crypto_info. Aborting.", table_name="crypto_info_dim")
            return

        if not self.should_transform("dim_crypto_info", last_snapshot, force=False, daily_comparison=True):
            self.log("dim_crypto_info is up to date. Skipping transformation.", table_name="crypto_info_dim")
            return

        self._run_build_step("dim_crypto_info", lambda: self.__prepare_dim_crypto_info_df(last_snapshot), "dim_crypto_info")

    def build_fact_crypto_market(self) -> None:
        """
        Orchestrates the transformation of the `fact_crypto_market` table using the latest batch of snapshot files.

        Responsibilities:
        - Retrieves the latest complete batch from `data/bronze/crypto_listings_latest_data` (multiple Parquet files)
        - Validates whether the batch has already been processed using metadata tracking
        - Delegates the actual transformation to `__prepare_fact_crypto_market_df()` via `_run_build_step`
        - Handles logging, writing to Delta Lake, and metadata persistence

        Notes:
        - This transformation computes KPIs like market cap ratios, supply utilization, and price change metrics
        - Only one representative file from the batch is used to determine freshness (assumes atomic batch logic)

        Returns:
            None
        """

        self.log(style="\n")

        batch_paths = self.find_latest_batch("crypto_listings_latest_data")

        if not batch_paths:
            self.log(
                "[SKIP] No snapshot found in directory: crypto_listings_latest_data. Skipping transformation.",
                table_name="fact_crypto_market",
            )
            return None

        # Check if one file from the batch has already been processed (all 5 are treated together)
        if not self.should_transform("fact_crypto_market", batch_paths[0], force=False, daily_comparison=False):
            self.log("fact_crypto_market is up to date. Skipping transformation.", table_name="fact_crypto_market")
            return

        # Run the build step by invoking the preparation function
        self._run_build_step("fact_crypto_market", lambda: self.__prepare_fact_crypto_market_df(batch_paths), "fact_crypto_market")

    def build_dim_crypto_category(self) -> None:
        """
        Orchestrates the transformation of the `dim_crypto_categories` table from the latest category snapshot.

        This method:
        - Retrieves the latest snapshot file from the `data/bronze/crypto_categories_data` directory
        - Skips transformation if the snapshot was already processed (based on metadata)
        - Stores the snapshot reference in `self.last_categories_snapshot` for downstream use
        - Delegates the actual transformation to `__prepare_dim_crypto_categories_df()` via `_run_build_step`
        - Broadcasts cleaned category metrics for reuse in fact table construction

        Notes:
        - `self.last_categories_snapshot` is reused by `fact_crypto_category` to ensure snapshot consistency

        Returns:
            None
        """
        self.log(style="\n")

        snapshot_paths = self.find_latest_data_files("crypto_categories_data")
        if not snapshot_paths:
            self.log("No snapshot available for dim_crypto_category. Skipping", table_name="dim_crypto_category")
            return None

        last_snapshot = snapshot_paths[0]
        self.last_categories_snapshot = last_snapshot

        if last_snapshot is None:
            self.log("[ERROR] No snapshot set for dim_crypto_category. Aborting.", table_name="dim_crypto_category")
            return

        if not self.should_transform("dim_crypto_category", last_snapshot, force=False, daily_comparison=False):
            self.log("dim_crypto_category is up to date. Skipping transformation.", table_name="dim_crypto_category")
            return

        self._run_build_step(
            "dim_crypto_category", lambda: self.__prepare_dim_crypto_categories_df(last_snapshot), "dim_crypto_category"
        )

    def build_fact_crypto_category(self) -> None:
        """
        Orchestrates the transformation of the `fact_crypto_category` table using broadcasted category metrics.

        Responsibilities:
        - Verifies that the latest category snapshot has been loaded via `build_dim_crypto_category`
        - Checks if the data has already been processed (based on snapshot freshness metadata)
        - Delegates KPI enrichment and preparation to `__prepare_fact_crypto_category_df()` via `_run_build_step`

        Notes:
        - Depends on `self.broadcasted_crypto_categories_metrics` and `self.last_categories_snapshot`
        - KPIs include: volume/market_cap ratio, change rates, token dominance, etc.

        Returns:
            None
        """
        self.log(style="\n")

        if self.last_categories_snapshot is None:
            self.log(
                "[ERROR] No snapshot set for fact_crypto_category. Please run build_dim_crypto_category() first.",
                table_name="crypto_categories_fact",
            )
            return

        if not self.should_transform("fact_crypto_category", self.last_categories_snapshot, force=False):
            self.log("fact_crypto_category is up to date. Skipping transformation.", table_name="crypto_categories_fact")
            return

        self._run_build_step("fact_crypto_category", self.__prepare_fact_crypto_category_df, "fact_crypto_category")

    def build_crypto_category_link(self) -> None:
        """
        Orchestrates the transformation of the `dim_crypto_category_link` table from the latest snapshot.

        Responsibilities:
        - Retrieves the most recent snapshot from the 'crypto_category_data' directory
        - Skips the transformation if the data has already been processed (based on metadata)
        - Delegates the preparation logic to `__prepare_dim_crypto_category_link_df` via `_run_build_step`

        Notes:
        - This table maps individual cryptocurrencies to their associated categories

        Returns:
            None
        """
        self.log(style="\n")

        snapshot_paths = self.find_latest_data_files("crypto_category_data")
        if not snapshot_paths:
            self.log("No snapshot available for crypto_category_link. Skipping", table_name="crypto_categories_link")
            return None

        last_snapshot = snapshot_paths[0]
        #self.last_map_snapshot = last_snapshot

        if last_snapshot is None:
            self.log("[ERROR] No snapshot set for crypto_category_link. Aborting.", table_name="crypto_categories_link")
            return

        if not self.should_transform("crypto_category_link", last_snapshot, force=False, daily_comparison=False):
            self.log("crypto_category_link is up to date. Skipping transformation.", table_name="crypto_categories_link")
            return

        self._run_build_step(
            "crypto_category_link", lambda: self.__prepare_dim_crypto_category_link_df(last_snapshot), "crypto_category_link"
        )

    # --------------------------------------------------------------------
    #                    Internal Transformation Methods
    # --------------------------------------------------------------------

    def __prepare_dim_crypto_id_df(self, latest_snapshot: str) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the dim_crypto_id table from the latest crypto_map snapshot.

        This method:
        - Reads a typed Parquet snapshot containing crypto identity fields
        - Extracts and casts core columns (e.g., id, name, symbol, platform info)
        - Ensures uniqueness on crypto_id and drops rows with missing values
        - Logs metadata and saves snapshot path for dependent tasks

        Args:
            latest_snapshot (str): Path to the latest crypto_map Parquet snapshot

        Returns:
            Tuple[DataFrame, str]: Cleaned dimension DataFrame and snapshot path, or None if loading fails
        """
        self.log(" > Starting transformation: dim_crypto_id", table_name="crypto_id_dim")

        # Store the latest snapshot path for reuse by dependent methods (map_dim)
        self.last_map_snapshot = latest_snapshot

        # Define strict schema
        crypto_id_schema = StructType([
                                StructField("id", LongType(), True),
                                StructField("name", StringType(), True),
                                StructField("symbol", StringType(), True),
                                StructField("platform_id", DoubleType(), True),
                                StructField("platform_name", StringType(), True),
                                StructField("platform_symbol", StringType(), True),
                                StructField("platform_token_address", StringType(), True),
                            ])

        self.log(f"Reading latest crypto_map snapshot: {latest_snapshot.name}", table_name="crypto_id_dim")

        # Read with schema
        try:
            df_raw = self.spark.read.schema(crypto_id_schema).parquet(str(latest_snapshot))

        except Exception as e:
            self.log(f"[ERROR] Failed to read Parquet file at {latest_snapshot} -> {e}", table_name="crypto_id_dim")
            return None

        crypto_id_columns = [
            "crypto_id",
            "name",
            "symbol",
            "platform_id",
            "platform_name",
            "platform_symbol",
            "platform_token_address",
        ]

        crypto_id_df = (
            df_raw.withColumn("crypto_id", col("id").cast("int"))
            .withColumn("platform_id", col("platform_id").cast("int"))
            .select(*crypto_id_columns)
            .dropna(subset=["crypto_id"])
            .dropDuplicates(subset=["crypto_id"])
        )

        self.log_dataframe_info(crypto_id_df, "Cleaned dim_crypto_id", table_name="crypto_id_dim")

        return crypto_id_df, latest_snapshot

    def __prepare_dim_crypto_map_df(self) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the dim_crypto_map table from the same snapshot used for dim_crypto_id.

        Requires:
        - `self.last_map_snapshot` to be already initialized by __prepare_dim_crypto_id_df()

        This method:
        - Loads dynamic mapping fields (rank, status, historical data)
        - Ensures primary key uniqueness on (crypto_id, snapshot_timestamp)
        - Logs cleaned output DataFrame

        Returns:
            Optional[DataFrame]: Transformed DataFrame, or None if snapshot is missing or fails to load.
        """
        self.log(" > Starting transformation: dim_crypto_map", table_name="crypto_map_dim")

        if self.last_map_snapshot is None:
            self.log(
                "[ERROR] self.last_map_snapshot not found — run __prepare_dim_crypto_id_df first.",
                table_name="crypto_map_dim",
            )
            return None

        crypto_map_schema = StructType([
                                StructField("id", LongType(), True),
                                StructField("rank", LongType(), True),
                                StructField("is_active", LongType(), True),
                                StructField("last_historical_data", StringType(), True),  
                                StructField("date_snapshot", StringType(), True),        
                            ])

        self.log(f"Reading snapshot from: {self.last_map_snapshot.name}", table_name="crypto_map_dim")

        try:
            df_raw = self.spark.read.schema(crypto_map_schema).parquet(str(self.last_map_snapshot))

        except Exception as e:
            self.log(f"[ERROR] Failed to read Parquet file at {self.last_map_snapshot} -> {e}", table_name="crypto_map_dim")
            return None

        crypto_map_columns = ["crypto_id", "snapshot_timestamp", "snapshot_str", "rank", "is_active", "last_historical_data"]

        crypto_map_df = (
            df_raw.withColumn("crypto_id", col("id").cast("int"))
            .withColumn("rank", col("rank").cast("int"))
            .withColumn("is_active", col("is_active").cast("int"))
            .withColumn("snapshot_timestamp", to_timestamp("date_snapshot"))
            .withColumn("snapshot_str", date_format(col("snapshot_timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("last_historical_data", date_format(to_timestamp(col("last_historical_data")), "yyyy-MM-dd HH:mm:ss"))
            .select(*crypto_map_columns)
            .dropna(subset=["crypto_id", "snapshot_timestamp"])
            .dropDuplicates(subset=["crypto_id", "snapshot_timestamp"])
        )

        self.log_dataframe_info(crypto_map_df, "Cleaned dim_crypto_map", table_name="crypto_map_dim")

        return crypto_map_df, self.last_map_snapshot

    def __prepare_dim_crypto_info_df(self, last_snapshot: str) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the dim_crypto_info table from the latest crypto information snapshot.

        This method:
        - Reads a typed Parquet file containing extended metadata for cryptocurrencies
        - Casts and normalizes date fields (date_added, date_launched, date_snapshot)
        - Renames technical columns for clarity (e.g., logo → logo_url)
        - Ensures uniqueness on crypto_id and filters nulls
        - Returns a cleaned DataFrame ready for dimension loading

        Args:
            last_snapshot (str): Path to the latest Parquet snapshot.

        Returns:
            Tuple[DataFrame, str]: Cleaned dimension DataFrame and snapshot path, or None if loading fails.
        """
        self.log(" > Starting transformation: dim_crypto_info", table_name="crypto_info_dim")

        schema = StructType([
                        StructField("id", LongType(), True),  
                        StructField("slug", StringType(), True),
                        StructField("category", StringType(), True),
                        StructField("tags", StringType(), True),
                        StructField("description", StringType(), True),
                        StructField("date_launched", StringType(), True),  
                        StructField("date_added", StringType(), True), 
                        StructField("logo", StringType(), True),
                        StructField("website", StringType(), True),
                        StructField("technical_doc", StringType(), True),
                        StructField("twitter", StringType(), True),
                        StructField("explorer", StringType(), True),
                        StructField("source_code", StringType(), True),
                        StructField("date_snapshot", StringType(), True), 
                    ])

        self.log(f"Reading latest crypto_map snapshot: {last_snapshot.name}", table_name="crypto_info_dim")

        try:
            df_raw = self.spark.read.schema(schema=schema).parquet(str(last_snapshot))

        except Exception as e:
            self.log(f"[ERROR] Failed to read Parquet file at {last_snapshot} -> {e}", table_name="crypto_info_dim")
            return None

        # Help to drop unwanted colmuns and to keep the other orderd
        info_columns = [
            "crypto_id",
            "slug",
            "snapshot_timestamp",
            "snapshot_date",
            "date_launched",
            "date_added",
            "category",
            "tags",
            "description",
            "logo_url",
            "website_url",
            "technical_doc_url",
            "explorer_url",
            "source_code_url",
            "twitter_url",
        ]

        crypto_info_df = (
            df_raw.withColumn("crypto_id", col("id").cast("int"))
            .withColumn("snapshot_timestamp", to_timestamp("date_snapshot"))
            .withColumn("snapshot_date", to_date(col("snapshot_timestamp")))
            .withColumn("date_launched", to_date(col("date_launched")))
            .withColumn("date_added", to_date(col("date_added")))
            .withColumn("tags", split(col("tags"), ","))
            .withColumnRenamed("logo", "logo_url")
            .withColumnRenamed("website", "website_url")
            .withColumnRenamed("technical_doc", "technical_doc_url")
            .withColumnRenamed("twitter", "twitter_url")
            .withColumnRenamed("source_code", "source_code_url")
            .withColumnRenamed("explorer", "explorer_url")
            .select(*info_columns)
            .dropna(subset=["crypto_id"])
            .dropDuplicates(subset=["crypto_id"])
        )

        self.log_dataframe_info(crypto_info_df, "Cleaned dim_crypto_info", table_name="crypto_info_dim")
        self.log("dim_crypto_info transformation prepared successfully.", table_name="crypto_info_dim")

        return crypto_info_df, last_snapshot

    def __prepare_fact_crypto_market_df(self, last_batch: List[str]) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the fact_crypto_market table from a batch of raw Parquet snapshots.

        This method:
        - Loads and unions a complete snapshot batch (e.g., 5 files)
        - Applies strict schema enforcement and normalization (types, formats, array parsing)
        - Cleans invalid rows (missing keys, duplicates)
        - Calculates key financial KPIs (volume ratios, supply metrics)
        - Returns a cleaned and enriched DataFrame ready for silver layer or DWH ingestion

        Args:
            last_batch (List[str]): List of parquet file paths for the latest crypto market snapshot batch.

        Returns:
            Optional[Tuple[DataFrame, str]]: Enriched DataFrame and the associated batch identifier (file path).
        """
        self.log(" > Starting transformation: fact_crypto_market", table_name="fact_crypto_market")

        # Define the expected schema for raw crypto market data
        schema = StructType([
                        StructField("id", LongType(), True),  
                        StructField("cmc_rank", LongType(), True),
                        StructField("num_market_pairs", DoubleType(), True),
                        StructField("circulating_supply", DoubleType(), True),
                        StructField("total_supply", DoubleType(), True),
                        StructField("max_supply", DoubleType(), True),
                        StructField("infinite_supply", BooleanType(), True),
                        StructField("self_reported_circulating_supply", DoubleType(), True),
                        StructField("self_reported_market_cap", DoubleType(), True),
                        StructField("platform_id", DoubleType(), True),
                        StructField("price_usd", DoubleType(), True),
                        StructField("volume_24h_usd", DoubleType(), True),
                        StructField("volume_change_24h", DoubleType(), True),
                        StructField("percent_change_1h_usd", DoubleType(), True),
                        StructField("percent_change_24h_usd", DoubleType(), True),
                        StructField("percent_change_7d_usd", DoubleType(), True),
                        StructField("market_cap_usd", DoubleType(), True),
                        StructField("market_cap_dominance_usd", DoubleType(), True),
                        StructField("fully_diluted_market_cap_usd", DoubleType(), True),
                        StructField("last_updated_usd", StringType(), True),  
                        StructField("date_snapshot", StringType(), True), 
                        StructField("batch_index", LongType(), True),
                    ])
        
        # Define the final column selection and order
        selected_columns = [
            "crypto_id",
            "snapshot_timestamp",
            "snapshot_str",
            "batch_index",
            "cmc_rank",
            "last_updated_usd",
            "platform_id",
            "circulating_supply",
            "total_supply",
            "max_supply",
            "infinite_supply",
            "self_reported_circulating_supply",
            "price_usd",  
            "volume_24h_usd",
            "volume_change_24h",
            "percent_change_1h_usd",
            "percent_change_24h_usd",
            "percent_change_7d_usd",
            "market_cap_usd",
            "self_reported_market_cap",
            "market_cap_dominance_usd",
            "fully_diluted_market_cap_usd",
            "num_market_pairs",
        ]

        try:
            # Load the first snapshot file to initialize the DataFrame
            self.log(
                f"[BATCH INIT] Reading first snapshot of fact_crypto_market batch — total files: {len(last_batch)}",
                table_name="fact_crypto_market",
            )
            df_combined = self.spark.read.schema(schema).parquet(str(last_batch[0]))

            # Union all remaining snapshot files
            for i, snapshot_path in enumerate(last_batch[1:]):
                self.log(
                    f"[{i+2}/{len(last_batch)}] Merging snapshot file: {snapshot_path} - Remaining: {len(last_batch) -i -2}",
                    table_name="fact_crypto_market",
                )
                df_next = self.spark.read.schema(schema).parquet(str(snapshot_path))
                df_combined = df_combined.unionByName(df_next, allowMissingColumns=True)

        except Exception as e:
            self.log(f"[ERROR] Failed to read or merge snapshot parquet files -> {e}", table_name="fact_crypto_market")
            return None

        try:
            # Clean and normalize fields
            df_cleaned = (
                df_combined.withColumn("crypto_id", col("id").cast("int"))
                .withColumn("snapshot_timestamp", to_timestamp("date_snapshot"))
                .withColumn("snapshot_str", date_format(col("snapshot_timestamp"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("platform_id", col("platform_id").cast("int"))
                .withColumn("batch_index", col("batch_index").cast("int"))
                .withColumn("cmc_rank", col("cmc_rank").cast("int"))
                .withColumn("num_market_pairs", col("num_market_pairs").cast("int"))
                .withColumn("last_updated_usd", date_format(to_timestamp(col("last_updated_usd")), "yyyy-MM-dd HH:mm:ss"))
                .drop("id")
                .dropna(subset=["crypto_id", "snapshot_timestamp"])
                .dropDuplicates(subset=["crypto_id", "snapshot_timestamp"])
                .select(*selected_columns)
            )

            self.log("Data cleaning and column normalization done.", table_name="fact_crypto_market")

            # Add derived KPIs
            df_enriched = (
                df_cleaned
                # KPI 1 : volume_to_market_cap_ratio
                .withColumn(
                    "volume_to_market_cap_ratio",
                    when(
                        col("market_cap_usd").isNotNull() & (col("market_cap_usd") != 0),
                        col("volume_24h_usd") / col("market_cap_usd"),
                    ).otherwise(lit(0.0)),
                )
                # KPI 2 : missing_supply_ratio
                .withColumn(
                    "missing_supply_ratio",
                    when(
                        col("total_supply").isNotNull() & (col("total_supply") != 0) & col("circulating_supply").isNotNull(),
                        (col("total_supply") - col("circulating_supply")) / col("total_supply"),
                    ).otherwise(lit(0.0)),
                )
                # KPI 3 : supply_utilization
                .withColumn(
                    "supply_utilization",
                    when(
                        col("max_supply").isNotNull() & (col("max_supply") != 0) & col("circulating_supply").isNotNull(),
                        col("circulating_supply") / col("max_supply"),
                    ).otherwise(lit(0.0)),
                )
                # KPI 4 : fully_diluted_cap_ratio
                .withColumn(
                    "fully_diluted_cap_ratio",
                    when(
                        col("market_cap_usd").isNotNull()
                        & (col("market_cap_usd") != 0)
                        & col("fully_diluted_market_cap_usd").isNotNull(),
                        col("fully_diluted_market_cap_usd") / col("market_cap_usd"),
                    ).otherwise(lit(0.0)),
                )
            )

            self.log_dataframe_info(df_enriched, "Cleaned & enriched fact_crypto_market_df", table_name="fact_crypto_market")
            return df_enriched, last_batch[0]

        except Exception as e:
            self.log(f"[ERROR] Failed during DataFrame cleaning or KPI enrichment -> {e}", table_name="fact_crypto_market")
            return None, None

    def __prepare_dim_crypto_categories_df(self, last_snapshot: str) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the dim_crypto_categories table and broadcasts the associated metrics for use in the fact table.

        This method:
        - Reads the latest snapshot of category data (Parquet format)
        - Extracts and cleans the stable descriptive fields to build the dimension table
        - Selects and broadcasts metric columns (e.g., num_tokens, market_cap, volume) to be reused in the fact table
        - Stores the cleaned DataFrame of metrics in `self.broadcasted_crypto_categories_metrics`

        Args:
            last_snapshot (str): Path to the most recent category snapshot file.

        Returns:
            Tuple[DataFrame, str]: Cleaned dimension DataFrame and the snapshot path, or None if loading fails.
        """
        self.log(" > Starting transformation: dim_crypto_category", table_name="dim_crypto_category")

        # Store the latest snapshot path for reuse by dependent methods (fact_crypto_category)
        self.last_categories_snapshot = last_snapshot

        # Define strict schema
        categories_schema = StructType([
                                StructField("category_id", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("title", StringType(), True),
                                StructField("description", StringType(), True),
                                StructField("num_tokens", DoubleType(), True),
                                StructField("avg_price_change", DoubleType(), True),
                                StructField("market_cap", DoubleType(), True),
                                StructField("market_cap_change", DoubleType(), True),
                                StructField("volume", DoubleType(), True),
                                StructField("volume_change", DoubleType(), True),
                                StructField("last_updated", StringType(), True),  
                                StructField("date_snapshot", StringType(), True),  
                            ])

        self.log(f"Reading latest dim_crypto_category snapshot: {last_snapshot.name}", table_name="dim_crypto_category")

        # Read with schema
        try:
            df_raw = self.spark.read.schema(categories_schema).parquet(str(last_snapshot))

        except Exception as e:
            self.log(f"[ERROR] Failed to read Parquet file at {last_snapshot} -> {e}", table_name="dim_crypto_category")
            return None

        # --- Build dim_crypto_category table ---
        dim_columns = ["category_id", "name", "title", "description"]
        dim_df = df_raw.select(*dim_columns).dropna(subset=["category_id"]).dropDuplicates(subset=["category_id"])
        self.log_dataframe_info(dim_df, "Cleaned dim_crypto_categories", table_name="dim_crypto_category")

        # --- Broadcast the metrics columns in destination to the fact table ---
        fact_columns = [
            "category_id",
            "snapshot_timestamp",
            "snapshot_str",
            "num_tokens",
            "avg_price_change",
            "market_cap",
            "market_cap_change",
            "volume",
            "volume_change",
            "last_updated",
        ]
        category_metrics_df = (
            df_raw
            .withColumn("num_tokens", col("num_tokens").cast("int"))
            .withColumn("last_updated", date_format(to_timestamp(col("last_updated")), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("snapshot_timestamp", to_timestamp("date_snapshot"))
            .withColumn("snapshot_str", date_format(col("snapshot_timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .select(*fact_columns)
            .dropna(subset=["category_id", "snapshot_timestamp"])
            .dropDuplicates(subset=["category_id", "snapshot_timestamp"])
        )

        self.broadcasted_crypto_categories_metrics = broadcast(category_metrics_df)
        self.log_dataframe_info(
            category_metrics_df, "Cleaned broadcasted_crypto_categories_metrics", table_name="dim_crypto_category"
        )

        return dim_df, last_snapshot

    def __prepare_dim_crypto_category_link_df(self, last_snapshot: str) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the dim_crypto_category_link table, which captures the many-to-many relationship
        between cryptocurrencies and their associated categories.

        This method:
        - Reads a Parquet snapshot with a strict schema
        - Casts crypto_id to Integer and category_id to String
        - Drops null and duplicate combinations
        - Logs basic metadata

        Args:
            last_snapshot (str): Path to the latest snapshot containing category mappings.

        Returns:
            Tuple[DataFrame, str]: Cleaned link DataFrame and the snapshot path, or None if loading fails.
        """
        self.log(" > Starting transformation: crypto_category_link", table_name="crypto_categories_link")

        # Define strict schema
        crypto_category_link_schema = StructType([
                                                StructField("crypto_id", LongType(), True),
                                                StructField("category_id", StringType(), True),
                                            ])

        self.log(f"Reading latest crypto_category_link snapshot: {last_snapshot.name}", table_name="crypto_categories_link")

        # Read with schema
        try:
            df_raw = self.spark.read.schema(crypto_category_link_schema).parquet(str(last_snapshot))

        except Exception as e:
            self.log(f"[ERROR] Failed to read Parquet file at {last_snapshot} -> {e}", table_name="crypto_categories_link")
            return None

        link_crypto_category_df = (
            df_raw.withColumn("crypto_id", col("crypto_id").cast("int"))
            .dropna(subset=["crypto_id", "category_id"])
            .dropDuplicates(subset=["crypto_id", "category_id"])
        )

        self.log_dataframe_info(link_crypto_category_df, "Cleaned crypto_category_link", table_name="crypto_categories_link")

        return link_crypto_category_df, last_snapshot

    def __prepare_fact_crypto_category_df(self) -> Optional[Tuple[DataFrame, str]]:
        """
        Prepares the fact_crypto_category table using the broadcasted metrics from the category snapshot.

        This method:
        - Consumes the broadcasted DataFrame created in `__prepare_dim_crypto_categories_df`
        - Computes robust KPI fields (e.g., volume_to_market_cap_ratio, market_cap_change_rate)
        with null protection using Spark `when(...).otherwise(...)`
        - Logs schema and row count of the resulting DataFrame

        Requires:
            - `self.broadcasted_crypto_categories_metrics` to be initialized
            - `self.last_categories_snapshot` to be set for metadata tracking

        Returns:
            Tuple[DataFrame, str]: Enriched fact DataFrame and the snapshot path, or None if the metrics are unavailable.
        """
        self.log(
            " > Starting transformation: fact_crypto_category (append mode from broadcasted categories)",
            table_name="crypto_categories_fact",
        )

        if self.broadcasted_crypto_categories_metrics is not None:
            self.log(
                "[OK] Found broadcasted_crypto_categories_metrics. Proceeding with KPI calculations for fact_crypto_category.",
                table_name="crypto_categories_fact",
            )
            metrics_df = (
                self.broadcasted_crypto_categories_metrics
                # KPI 1 : volume_to_market_cap_ratio
                .withColumn(
                    "volume_to_market_cap_ratio",
                    when(col("market_cap").isNotNull() & (col("market_cap") != 0), col("volume") / col("market_cap")).otherwise(
                        lit(0.0)
                    ),
                )
                # KPI 2 : dominance_per_token
                .withColumn(
                    "dominance_per_token",
                    when(
                        col("num_tokens").isNotNull() & (col("num_tokens") != 0), col("market_cap") / col("num_tokens")
                    ).otherwise(lit(0.0)),
                )
                # KPI 3 : market_cap_change_rate
                .withColumn(
                    "market_cap_change_rate",
                    when(
                        col("market_cap").isNotNull() & (col("market_cap") != 0), col("market_cap_change") / col("market_cap")
                    ).otherwise(lit(0.0)),
                )
                # KPI 4 : volume_change_rate
                .withColumn(
                    "volume_change_rate",
                    when(col("volume").isNotNull() & (col("volume") != 0), col("volume_change") / col("volume")).otherwise(
                        lit(0.0)
                    ),
                )
                # KPI 5 : price_change_index
                .withColumn(
                    "price_change_index",
                    when(
                        col("avg_price_change").isNotNull() & col("num_tokens").isNotNull(),
                        col("avg_price_change") * col("num_tokens"),
                    ).otherwise(lit(0.0)),
                )
            )

            metrics_df = metrics_df.select(
                "category_id",
                "snapshot_timestamp",
                "snapshot_str",
                "num_tokens",
                "avg_price_change",
                "market_cap",
                "market_cap_change",
                "volume",
                "volume_change",
                "last_updated",
                "volume_to_market_cap_ratio",
                "dominance_per_token",
                "market_cap_change_rate",
                "volume_change_rate",
                "price_change_index",
            )
        else:
            self.log(
                "[ERROR] broadcasted_crypto_categories_metrics is not available. Please run build_dim_crypto_category() first.",
                table_name="crypto_categories_fact",
            )
            return None, None

        self.log_dataframe_info(metrics_df, "Cleaned fact_crypto_category", table_name="crypto_categories_fact")
        return metrics_df, self.last_categories_snapshot
