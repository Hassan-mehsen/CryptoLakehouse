from pyspark.sql import SparkSession, DataFrame, functions as f
from typing import Optional, List, Tuple
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path
import json
import os

load_dotenv()


class BaseLoader:
    """
    Abstract base class for all data loaders responsible for persisting transformed Delta tables
    into the data warehouse.

    This class provides standardized utilities for:
    - Reading Delta Lake tables (optionally by version)
    - Writing Spark DataFrames to a PostgreSQL data warehouse
    - Logging execution details and data characteristics
    - Managing and persisting load metadata (as JSONL) for audit and idempotency
    - Deciding whether a load should occur, based on transformation metadata comparison

    Subclasses must specify a domain-specific loader name (e.g., GlobalMetricsLoader),
    which automatically maps to the appropriate data paths and metadata folders.

    Attributes:
        spark (SparkSession): The active Spark session used for I/O operations.
        name (str): The name of the subclass loader.
        domain (str): Inferred domain name based on the loader class.
        log_path (Path): Path to the centralized log file.
        data_path (Path): Path to the silver Delta tables for the domain.
        metadata_dir (Path): Path to the directory containing load metadata.
        load_metadata (dict): Dictionary holding metadata fields for the current load operation.

    Intended to be extended by domain-specific loader classes (e.g., CryptoLoader, ExchangeLoader).
    """

    # Maps loaders class names to their corresponding domain folder names
    DOMAIN_NAME_MAP = {
        "ExchangeLoader": "exchange",
        "CryptoLoader": "crypto",
        "FearAndGreedLoader": "fear_and_greed",
        "GlobalMetricsLoader": "global_metrics",
    }

    def __init__(self, spark: SparkSession):

        self.spark = spark
        self.name = self.__class__.__name__

        # JDBC connection parameters loaded from environment variables for security and portability
        self.jdbc_url = os.getenv("SPARK_JDBC_URL")
        self.jdbc_properties = {
            "user": os.getenv("DATABASE_USER"),
            "password": os.getenv("DATABASE_PASSWORD"),
            "driver": "org.postgresql.Driver",
        }

        # Unified UTC timestamp for all log lines of this transformer instance
        self.str_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.timestamp = datetime.now(timezone.utc).isoformat()

        # Dynamically resolves the root of the project
        self.PROJECT_ROOT = Path(__file__).resolve().parents[3]

        # Centralized log file
        self.log_path = self.PROJECT_ROOT / "logs" / "load.log"

        # Domain folder name used in silver layer paths
        self.domain = self.DOMAIN_NAME_MAP.get(self.name)

        # Data store
        self.data_path = self.PROJECT_ROOT / "data" / "silver" / self.domain

        # Path to store load metadata files for a specific domain
        # (e.g., metadata/load/exchange/) — created if not existing
        self.metadata_dir = self.PROJECT_ROOT / "metadata" / "load" / self.domain
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

        # Dictionary to store load metadata.
        # Used for auditability, data lineage, and conditional loading logic.
        self.load_metadata = {
            "table": "",
            "domain": self.domain,
            "status": "",
            "record_count": "",
            "mode": "",
            "started_at": "",
            "ended_at": "",
            "transformation_ended_at": "",  # Latest known successful transform timestamp
            "notes": "",  # Optional free-form comment
        }

    # --------------------------------------------------------------------
    #                           Logging Methods
    # --------------------------------------------------------------------

    def log(self, message: str = "", table_name: str = None, style: str = None) -> None:
        """
        Writes a log entry to the centralized load.log file with a standardized format.

        Logs include a UTC timestamp, the current loader class name, and optionally the table name.
        If a custom pre-formatted message is provided via `style`, it will be written as-is.

        Args:
            message (str): Message to log. Ignored if `style` is provided.
            table_name (str, optional): Table name to include in the log prefix.
            style (str, optional): Full formatted log string to write directly (bypasses formatting logic).
        """
        if table_name:
            formatted = f"[{self.str_timestamp}] [LOAD] [{self.name.upper()}] [{table_name}] {message} \n"
        else:
            formatted = f"[{self.str_timestamp}] [LOAD] [{self.name.upper()}] {message} \n"

        try:
            msg = style if style else formatted
            with open(self.log_path, "a") as f:
                f.write(msg)

        except Exception as e:
            print(f"[LOGGING FAILURE] Could not write to {self.log_path} -> {e}")

    def log_section(self, title: str, width: int = 50) -> None:
        """
        Logs a formatted visual section header to improve log readability.

        This creates a separator block (e.g., ===== Section Title =====) in the logs, useful to
        distinguish stages within a loader's execution.

        Args:
            title (str): Title to display within the section block.
            width (int): Total width of the separator line (default: 50).
        """
        self.log(style="\n" + "=" * width + "\n")
        self.log(style=title.center(width))
        self.log(style="\n" + "=" * width + "\n")

    def log_dataframe_info(self, df: DataFrame, table_name: str) -> None:
        """
        Logs summary information about a Spark DataFrame involved in a load operation.

        Includes:
        - Row count
        - Schema structure (compact string form)

        Typically used before or after writing to the data warehouse to trace data volume and structure.

        Args:
            df (DataFrame): Spark DataFrame to inspect.
            table_name (str): Logical table name for tagging the log line.
        """
        try:
            row_count = df.count()
            schema_str = df.schema.simpleString()
            msg = f"Row count: {row_count} | Schema: {schema_str}"
            self.log(message=msg, table_name=table_name)

        except Exception as e:
            self.log(message=f"[ERROR] Failed to log DataFrame info during [LOAD] -> {e}", table_name=table_name)

    # --------------------------------------------------------------------
    #                       I/O Helper Methods
    # --------------------------------------------------------------------

    def read_delta(self, relative_path: str, version: Optional[int] = None) -> Optional[DataFrame]:
        """
        Reads a Delta table from the silver data layer.

        This method supports reading either the latest version of the table,
        or a specific historical snapshot using the Delta Lake versioning mechanism.

        Args:
            relative_path (str): Subpath to the Delta table within the domain's silver directory.
                                Example: `fact_crypto_market` or `dim_exchange_map`.
            version (int, optional): Delta version to load. If None, loads the latest state of the table.

        Returns:
            Optional[DataFrame]: The loaded Spark DataFrame if successful, or None if an error occurred.

        Logging:
            - Logs the path and version being read.
            - Logs an error if the read operation fails.

        Example:
            df = self.read_delta("fact_global_market", version=12)
        """
        full_path = self.data_path / relative_path

        try:
            delta_reader = self.spark.read.format("delta")

            if version is not None:
                delta_reader = delta_reader.option("versionAsOf", int(version))
                self.log(f"Reading Delta table from: {full_path} @version={version}", table_name=relative_path)
            else:
                self.log(f"Reading Delta table from: {full_path}", table_name=relative_path)

            return delta_reader.load(str(full_path))

        except Exception as e:
            self.log(f"[ERROR] Failed to read Delta table from {full_path} -> {e}", table_name=relative_path)
            return None

    def save_metadata(self, table_name: str, metadata: dict) -> bool:
        """
        Appends a metadata record to the corresponding JSONL file for the given table.

        Metadata files are stored under: metadata/load/`domain`/`table_name`.jsonl

        This method ensures traceability of all loading operations by persistently recording
        metadata such as record count, status, timestamp, etc.

        Args:
            table_name (str): Logical name of the target table.
            metadata (dict): Dictionary containing metadata fields to persist.

        Returns:
            bool: True if the metadata was successfully saved, False otherwise.

        Logging:
            - Logs the path where metadata was saved.
            - Logs any error encountered during the write operation.
        """
        file_path = self.metadata_dir / f"{table_name}.jsonl"

        try:
            with open(file_path, "a", encoding="utf-8") as f:
                json.dump(metadata, f)
                f.write("\n")
            self.log(f"Metadata saved to {file_path}")
            return True

        except Exception as e:
            self.log(f"[ERROR] Failed to write metadata to {file_path} -> {e}")
            return False

    def read_last_metadata(self, table_name: str) -> Optional[dict]:
        """
        Retrieves the most recent metadata record for the specified table from its JSONL file.

        The method reads the metadata/load/`domain`/`table_name`.jsonl file in reverse order
        to find and return the last non-empty, well-formed JSON line.

        This metadata typically contains information about the last successful or attempted load
        operation for traceability and conditional logic in the pipeline.

        Args:
            table_name (str): Logical name of the table whose metadata should be read.

        Returns:
            Optional[dict]: The most recent metadata entry as a dictionary, or None if unavailable or malformed.

        Logging:
            - Logs whether metadata was found or missing.
            - Logs the path and outcome of the read operation.
        """
        metadata_path = self.metadata_dir / f"{table_name}.jsonl"

        if not metadata_path.exists():
            self.log(f"[INFO] No existing metadata for {table_name}")
            return None
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in reversed(lines):
                    if line.strip():
                        snapshot = json.loads(line.strip())
                        self.log(f"Last snapshot loaded from {metadata_path}")
                        return snapshot

        except Exception as e:
            self.log(f"[ERROR] Failed to parse metadata for {table_name} -> {e}")
            return None

    def read_last_transformation_metadata(self, table_name: str) -> Optional[dict]:
        """
        Retrieves the most recent transformation metadata record for the specified table.

        This method reads the latest JSON line from the corresponding file in:
        metadata/transform/`domain`/`table_name`.jsonl

        The metadata typically includes transformation timestamps, version, status,
        and data lineage information used to determine whether a new load should be triggered.

        Args:
            table_name (str): Logical name of the table whose transformation metadata is to be retrieved.

        Returns:
            Optional[dict]: Dictionary of the last transformation metadata if available, otherwise None.

        Logging:
            - Logs whether the metadata was found.
            - Logs the path accessed and any parsing errors encountered.
        """

        transform_metadata_path = self.PROJECT_ROOT / "metadata" / "transform" / self.domain / f"{table_name}.jsonl"

        if not transform_metadata_path.exists():
            self.log(f"[INFO] No existing transform metadata for {table_name}")
            return None
        try:
            with open(transform_metadata_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in reversed(lines):
                    if line.strip():
                        snapshot = json.loads(line.strip())
                        self.log(f"Last snapshot loaded from {transform_metadata_path}")
                        return snapshot

        except Exception as e:
            self.log(f"[ERROR] Failed to parse transform metadata for {table_name} -> {e}")
            return None

    # --------------------------------------------------------------------
    #                 Internal Execution Helpers
    # --------------------------------------------------------------------

    def read_from_dw(self, table_name: str, columns: Optional[List[str]] = None) -> Optional[DataFrame]:
        """
        Reads the target table from the data warehouse with optional column selection.

        Args:
            table_name (str): The target table to read from.
            columns (List[str], optional): Columns to select (default: all).

        Returns:
            Optional[DataFrame]: The loaded DataFrame or None if failed.
        """
        try:
            if columns:
                query = f"(SELECT {', '.join(columns)} FROM {table_name}) AS subquery"
            else:
                query = table_name  # full scan

            return self.spark.read.jdbc(url=self.jdbc_url, table=query, properties=self.jdbc_properties)

        except Exception as e:
            self.log(f"[ERROR] Failed to read from data warehouse: {e}", table_name=table_name)
            return None

    def write_to_dw(self, df: DataFrame, table_name: str, mode: str = "append") -> bool:
        """
        Writes a DataFrame to the data warehouse.
        Args:
            df (DataFrame): DataFrame to write.
            mode (str): Write mode ("append", "overwrite", etc.), append by default.
        """
        try:
            df.write.jdbc(url=self.jdbc_url, table=table_name, mode=mode, properties=self.jdbc_properties)
            return True

        except Exception as e:
            self.log(f"[ERROR] Failed to write to data warehouse: {e}", table_name=table_name)
            return False

    def should_load(self, table_name: str, force: bool = False) -> bool:
        """
        Determines whether the table should be loaded based on the status and timestamp
        of the last successful transformation.

        This method compares the 'ended_at' timestamp from the latest transformation metadata
        with the 'transformation_ended_at' value stored in the last load metadata.

        - If there is no prior load metadata, loading proceeds.
        - If a new transformation has occurred (based on timestamp), loading proceeds.
        - Otherwise, loading is skipped.

        'transformation_ended_at' acts as a version identifier to ensure idempotent,
        conditional loading.

        Args:
            table_name (str): Logical name of the table.

        Returns:
            bool: True if loading should proceed, False otherwise.
        """
        if force:
            return True

        load_metadata = self.read_last_metadata(table_name=table_name)

        if not load_metadata:
            self.log("Data is not loaded yet. Proceeding...", table_name=table_name)
            return True

        transform_metadata = self.read_last_transformation_metadata(table_name=table_name)
        if not transform_metadata:
            self.log(f"[WARN] No transform metadata found for {table_name}. Skipping load.", table_name=table_name)
            return False

        status = transform_metadata.get("status")
        current_status = transform_metadata.get("current_status")
        transform_ended_at = transform_metadata.get("ended_at")
        last_loaded_transform = load_metadata.get("transformation_ended_at")

        if status == "success" and current_status == "transformed" and transform_ended_at != last_loaded_transform:
            self.log("New transformation detected. Proceeding to load.", table_name=table_name)
            return True
        else:
            self.log("No new transformation since last load. Skipping.", table_name=table_name)
            return False
        
    def secure_fk_load(self, df: DataFrame, fks_ref_table: dict, table_name: str) -> Optional[Tuple[DataFrame, int]]:
        """
        Filters a DataFrame to keep only rows that have valid foreign key references
        in the target data warehouse tables.

        This method performs one left semi join per FK/table combination to ensure
        referential integrity before loading into the DW.

        Args:
            df (DataFrame): The input DataFrame to filter.
            fks_ref_table (dict): Mapping of fk_column -> referenced_table
                                Example: {"crypto_id": "dim_crypto_id"}
            table_name (str): Name of the target table for logging context.

        Returns:
            Optional[Tuple[DataFrame, int]]: 
                A tuple containing:
                - The filtered DataFrame with only valid FK rows.
                - The final row count after all FK validations.
                Returns None if an error occurs.
        """
        for fk_column, ref_table in fks_ref_table.items():
            self.log(f"Validating foreign key '{fk_column}' against '{ref_table}'", table_name=table_name)

            ref_df = self.read_from_dw(table_name=ref_table, columns=[fk_column])
            if ref_df is None:
                self.log(f"[ERROR] Could not read reference table '{ref_table}'", table_name=table_name)
                return None

            before_count = df.count()
            df = df.alias("a").join(
                ref_df.alias("b"),
                on=f.col(f"a.{fk_column}") == f.col(f"b.{fk_column}"),
                how="left_semi"
            ).select("a.*")
            after_count = df.count()

            self.log(
                f"Filtered {before_count - after_count} invalid rows for FK '{fk_column}' (remaining: {after_count})",
                table_name=table_name
            )

        return df, after_count

    def _load_fact_or_variant_table(
        self,
        table_name: str,
        fk_presence: bool,
        fks_ref_table: Optional[dict] = None,
        mode: str = "append",
        notes: str = "",
        version: Optional[int] = None,
    ) -> bool:
        """
        Loads a fact or variant dimension table into the data warehouse.

        This method is designed for tables that do not require deduplication (e.g., facts and variant dimensions),
        but may optionally require referential integrity enforcement via foreign key validation.

        Steps performed:
        1. Read the transformed Delta table (optionally from a specific version)
        2. (Optional) Filter out rows with invalid foreign key values via semi joins
        3. Write the resulting DataFrame to the DW using the configured mode
        4. Log details and persist metadata for traceability

        Args:
            table_name (str): Name of the DW table to write to.
            fk_presence (bool): Whether FK constraints must be validated before load.
            fks_ref_table (dict): Mapping of {fk_column: referenced_table}.
            mode (str): Write mode for Spark (default: "append").
            notes (str): Additional comment to include in load metadata.
            version (Optional[int]): Delta version to read (default: latest).

        Returns:
            bool: True if the load was successful and metadata persisted, False otherwise.
        """
        # Step 1: Read transformed Delta data
        self.load_metadata.update({"started_at": self.timestamp})

        df = self.read_delta(table_name, version=version)
        if df is None:
            self.log(f"[ERROR] No DataFrame returned from Delta for {table_name}", table_name=table_name)
            self.load_metadata.update({
                "table": table_name,
                "status": "failed",
                "record_count": 0,
            })
            self.save_metadata(table_name, self.load_metadata)
            return False

        self.log(f"Successfully read Delta table {table_name}", table_name=table_name)
        self.log_dataframe_info(df, table_name=table_name)

        # Step 2: Optional FK validation
        if fk_presence:
            df, filtered_count = self.secure_fk_load(df=df, fks_ref_table=fks_ref_table, table_name=table_name)
            self.log(f"Filtered {filtered_count} new rows to load", table_name=table_name)
            if df is None:
                self.log(f"[ERROR] FK validation failed. Aborting load for {table_name}", table_name=table_name)
                return False

        # Step 3: Write to DW
        status = self.write_to_dw(df, table_name=table_name, mode=mode)
        if not status:
            self.log(f"[ERROR] Failed to write to DW for {table_name}", table_name=table_name)
            return False

        self.log(f"Successfully wrote {df.count()} rows to {table_name}", table_name=table_name)

        # Step 4: Persist load metadata
        transform_meta = self.read_last_transformation_metadata(table_name)
        self.load_metadata.update({
            "table": table_name,
            "status": "loaded",
            "record_count": df.count(),
            "mode": mode,
            "ended_at": self.timestamp,
            "transformation_ended_at": transform_meta.get("ended_at") if transform_meta else "",
            "notes": notes,
        })
        self.log(f"Saving load metadata for {table_name}", table_name=table_name)

        return self.save_metadata(table_name, self.load_metadata)

    def _load_stable_dim_table(
        self,
        table_name: str,
        fk_presence: bool,
        pk_columns: List[str],
        version: Optional[int] = None,
        fks_ref_table: Optional[dict] = None,
        mode: str = "append",
        notes: str = "",
    ) -> bool:
        """
        Loads a stable dimension table into the data warehouse by inserting only new records
        (based on composite or single-column primary keys), with optional foreign key validation.

        This method is intended for slowly changing dimension tables that require deduplication
        before load. It also ensures referential integrity if foreign key validation is enabled.

        Steps performed:
        1. Read the transformed Delta table (optionally by version)
        2. Read existing PK values from the DW table
        3. Filter out duplicates using left anti join on PKs
        4. Optionally validate FKs before insertion (left semi join)
        5. Append the cleaned DataFrame to the DW
        6. Persist load metadata for auditability

        Args:
            table_name (str): Name of the dimension table in the DW.
            fk_presence (bool): Whether FK validation is required before load.
            fks_ref_table (dict): Mapping of {fk_column: referenced_table}.
            pk_columns (List[str]): List of PK column names for deduplication.
            version (Optional[int]): Delta version to load. Defaults to latest.
            mode (str): Spark write mode. Default is "append".
            notes (str): Optional free-text note saved in metadata.

        Returns:
            bool: True if data was successfully written and metadata saved, False otherwise.
        """
        self.load_metadata.update({"started_at": self.timestamp})

        # Step 1: Read transformed Delta table
        df = self.read_delta(table_name, version=version)
        if df is None:
            self.log(f"[ERROR] No DataFrame returned from Delta for {table_name}", table_name=table_name)
            self.load_metadata.update({
                "table": table_name,
                "status": "failed",
                "record_count": 0,
            })
            self.save_metadata(table_name, self.load_metadata)
            return False

        self.log(f"Successfully read Delta table {table_name}", table_name=table_name)
        self.log_dataframe_info(df, table_name=table_name)

        # Step 2: Read existing primary keys from DW
        self.log("Reading existing records from DW for deduplication...", table_name=table_name)
        df_dw = self.read_from_dw(table_name=table_name, columns=pk_columns)
        if df_dw is None:
            self.log(f"[ERROR] Failed to read DW records for deduplication", table_name=table_name)
            return False

        # Step 3: Deduplication using left anti join
        self.log("Filtering new rows using left anti join on primary key...", table_name=table_name)
        df_filtered = df.alias("a").join(
            df_dw.alias("b"),
            on=[f.col(f"a.{col}") == f.col(f"b.{col}") for col in pk_columns],
            how="left_anti"
        ).select("a.*")
        filtered_count = df_filtered.count()
        self.log(f"Filtered {filtered_count} new rows to load", table_name=table_name)

        if filtered_count == 0:
            self.log(f"No new records found for {table_name}. Skipping write.", table_name=table_name)
            self.load_metadata.update({
                "table": table_name,
                "status": "skipped",
                "record_count": 0,
                "notes": "Data is up to date"
            })
            return self.save_metadata(table_name, self.load_metadata)

        # Step 4 (optional): Enforce FK constraints if needed
        if fk_presence:
            df_filtered, filtered_count = self.secure_fk_load(df=df_filtered, fks_ref_table=fks_ref_table, table_name=table_name)
            if df_filtered is None:
                self.log(f"[ERROR] Foreign key validation failed. Aborting load for {table_name}", table_name=table_name)
                return False

        # Step 5: Append to DW
        status = self.write_to_dw(df_filtered, table_name=table_name, mode=mode)
        if not status:
            self.log(f"[ERROR] Failed to write filtered data to DW for {table_name}", table_name=table_name)
            return False
        self.log(f"{filtered_count} new rows successfully written to {table_name}", table_name=table_name)

        # Step 6: Save metadata
        transform_meta = self.read_last_transformation_metadata(table_name)
        self.load_metadata.update({
            "table": table_name,
            "status": "loaded",
            "record_count": filtered_count,
            "mode": mode,
            "ended_at": self.timestamp,
            "transformation_ended_at": transform_meta.get("ended_at") if transform_meta else "",
            "notes": notes,
        })
        self.log(f"Saving load metadata for {table_name}", table_name=table_name)

        return self.save_metadata(table_name, self.load_metadata)

        
        