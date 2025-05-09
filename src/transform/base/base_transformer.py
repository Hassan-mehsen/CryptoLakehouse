from typing import List, Optional, Callable, Tuple
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timezone
from pathlib import Path
import json


class BaseTransformer():
    """
    Abstract base class for Spark transformation modules (one per business domain).

    Responsibilities:
    - Provides a standardized framework for domain-specific transformers (e.g., Exchange, Crypto, etc.)
    - Accepts a SparkSession scoped to the pipeline context
    - Defines logging utilities for lifecycle events, errors, and DataFrame stats
    - Tracks structured transformation metadata (e.g., table name, snapshot source, row counts, timestamps)

    Exposed helper methods include:
    - Structured logging to `logs/transform.log`
    - Reading raw input from the bronze layer (Parquet)
    - Writing cleaned output to the silver layer (Delta Lake), with optional partitioning and schema evolution
    - Inspecting DataFrame row count and schema for traceability
    - Discovering and retrieving latest input snapshot files
    - Persisting and reading transformation metadata in JSONL format
    - Determining whether a transformation is necessary via `should_transform()` based on snapshot freshness
    - Centralizing the run logic for each table via `_run_build_step()`:
        * Orchestrates: prepare -> write -> log -> metadata
        * Ensures consistent ETL patterns across all table builds

    Design principles:
    - Enforces a standard `.run()` method to be implemented by subclasses
    - Favors modularity, reusability, observability, and robustness
    - Minimizes boilerplate code for implementing new transformation steps

    Typical usage:
    - Subclassed by each domain transformer (e.g. `ExchangeTransformer`, `CryptoTransformer`)
    """


    # Maps transformer class names to their corresponding domain folder names
    DOMAIN_NAME_MAP = {
        "ExchangeTransformer": "exchange",
        "CryptoTransformer": "crypto",
        "FearAndGreedTransformer": "fear_and_greed",
        "GlobalMetricsTransformer": "global_metrics",
    }

    def __init__(self, spark: SparkSession):
        # --------------------------------------------------------------------
        #                         Core Attributes
        # --------------------------------------------------------------------

        self.spark = spark
        self.name = self.__class__.__name__

        # Unified UTC timestamp for all log lines of this transformer instance
        self.str_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.timestamp = datetime.now(timezone.utc).isoformat()

        # Dynamically resolves the root of the project
        self.project_root = Path(__file__).resolve().parents[3]

        # Centralized log file
        self.log_path = self.project_root / "logs/transform.log"

        # Input/output data stores
        self.raw_data_path = self.project_root / "data/bronze"
        self.silver_data_path = self.project_root / "data/silver"

        # Domain folder name used in silver layer paths
        self.domain = self.DOMAIN_NAME_MAP.get(self.name, self.name.replace("Transformer", "").lower())

        # Path to store transformation metadata files for this domain
        # (e.g., metadata/transform/exchange/) — created if not existing
        self.metadata_dir = self.project_root / "metadata" / "transform" / self.domain
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

        # Dictionary to store transformation metadata.
        # Useful for tracking processing status, data lineage, and audit information.
        self.metadata = {
            "table": "-",
            "domain": self.domain,
            "last_status": "raw",
            "source_snapshot": "-",
            "current_status": "-",
            "status": "-",
            "record_count": "-",
            "started_at": "-",
            "ended_at": "-",
            "duration_seconds": "-",
            "notes": "-",
        }

    # --------------------------------------------------------------------
    #                           Logging Methods
    # --------------------------------------------------------------------

    def log(self, message: str = "", style: str = None) -> None:
        """
        Logs a message to the shared transform.log file with timestamp and transformer name.
        Args:
            message (str): The log message to write.
            style (str): Optional. If provided, this exact string will be logged instead of the formatted message.
        """

        formatted = f"[{self.str_timestamp}] [TRANSFORM] [{self.name.upper()}] {message}"
        try:
            msg = style if style else formatted
            with open(self.log_path, "a") as f:
                f.write(msg + "\n")

        except Exception as e:
            print(f"[LOGGING FAILURE] Could not write to {self.log_path} → {e}")

    def log_section(self, title: str, width: int = 50) -> None:
        """
        Logs a visual separator block for readability in logs.
        Args:
            title (str): Centered title of the section.
            width (int): Width of the log block (default: 50).
        """

        self.log(style="\n" + "=" * width + "\n")
        self.log(style=title.center(width))
        self.log(style="\n" + "=" * width + "\n")

    def log_dataframe_info(self, df: DataFrame, label: str = "") -> None:
        """
        Logs summary statistics for a DataFrame, including row count and schema.
        Logs any error encountered during inspection.

        Recommended usage:
        - Before and after each transformation step
        - For tracking schema changes and volume variation
        - For debugging data issues across stages

        Args:
            df (DataFrame): The Spark DataFrame to inspect.
            label (str): Optional label to contextualize the log (e.g., 'Before filtering', 'After join').
        """
        try:
            count = df.count()
            schema = df.schema.simpleString()
            self.log(f"[{label}] Row count: {count}, Schema: {schema}")

        except Exception as e:
            self.log(f"[ERROR] Failed to inspect DataFrame [{label}] → {e}")

    # --------------------------------------------------------------------
    #                       I/O Helper Methods
    # --------------------------------------------------------------------

    def find_latest_data_files(self, relative_folder: str, limit: int = 1) -> Optional[List[Path]]:
        """
        Returns a list of Path objects pointing to the latest snapshot data files
        (sorted by descending order of filename) in a specific raw data subfolder.
        Logs any error encountered.
        Args:
            relative_folder (str): Subdirectory inside `raw_data_path` where to look.
            limit (int): Number of most recent files to return.

        Returns:
            List[Path]: List of Path objects, from newest to oldest.
        """
        try:
            folder_path = self.raw_data_path / relative_folder
            all_files = sorted(folder_path.glob("*.parquet"), reverse=True)
            latest_files = all_files[:limit]

            self.log(f"Found {len(latest_files)} parquet file(s) in {relative_folder} (limit={limit})")
            return latest_files

        except Exception as e:
            self.log(f"[ERROR] Failed to list files in {relative_folder} -> {e}")
            return None

    def write_delta(
        self, df: DataFrame, relative_path: str, mode: str = "overwrite", partition_by: Optional[List[str]] = None
    ) -> str:
        """
        Writes a DataFrame to the Delta Lake format in the silver layer.

        Automatically logs the write operation and captures any error encountered.

        Supports schema evolution which allows the schema of the existing
        Delta table to be overwritten or appended during write operations.
        Optionally allows partitioning by one or more columns.

        Args:
            df (DataFrame): The Spark DataFrame to write.
            relative_path (str): Name of the Delta table folder inside the domain (e.g., "dim_exchange_id").
                                Full path resolved as: silver/domain/relative_path/
            mode (str): Spark write mode ("overwrite", "append", "ignore", or "error"). Default is "overwrite".
            partition_by (Optional[List[str]]): List of column names to partition the Delta table by (if any).

        Returns:
            str: "ok" if write successful, "ko" otherwise.
        """
        full_path = self.silver_data_path / self.domain / relative_path

        try:
            self.log(f"Writing Delta file to: {full_path} (mode={mode})")

            writer = df.write.format("delta").mode(mode)
            if mode == "overwrite":
                writer = writer.option("overwriteSchema", "true")
            elif mode == "append":
                writer = writer.option("mergeSchema", "true")

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.save(str(full_path))

            self.log(f"Write successful in mode {mode} to: {full_path}")
            return "ok"

        except Exception as e:
            self.log(f"[ERROR] Failed to write Delta file to {full_path} -> {e}")
            return "ko"

    def read_delta(self, relative_path: str) -> Optional[DataFrame]:
        """
        Reads a Delta table from the silver layer.
        Logs any read error encountered.
        Args:
            relative_path (str): Subpath to the Delta table in silver layer.

        Returns:
            DataFrame: The loaded Spark DataFrame.
        """
        full_path = self.silver_data_path / relative_path

        try:
            self.log(f"Reading Delta table from: {full_path}")
            return self.spark.read.format("delta").load(str(full_path))

        except Exception as e:
            self.log(f"[ERROR] Failed to read Delta table from {full_path} → {e}")
            return None

    def save_metadata(self, table_name: str, metadata: dict) -> None:
        """
        Saves metadata as a JSON file inside the transform domain folder.

        The file will be written to: metadata/transform/domain/table_name.json

        Args:
            table_name (str): Logical table name (used as filename).
            metadata (dict): Dictionary containing metadata info to be saved.
        """

        file_path = self.metadata_dir / f"{table_name}.jsonl"

        try:
            with open(file_path, "a", encoding="utf-8") as f:
                json.dump(metadata, f)
                f.write("\n")
            self.log(f"Metadata saved to {file_path}")

        except Exception as e:
            self.log(f"[ERROR] Failed to write metadata to {file_path} -> {e}")

    def read_last_metadata(self, table_name: str) -> Optional[dict]:
        """
        Reads the latest metadata entry for a given table.

        This method retrieves the last line of the corresponding metadata JSONL file
        (e.g. `metadata/transform/<domain>/<table_name>.jsonl`) and returns it as a dictionary.

        Args:
            table_name (str): Logical table name used to locate the metadata file.

        Returns:
            Optional[dict]: Last metadata entry as a dictionary, or None if not found or file is empty.
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
            self.log(f"[ERROR] Failed to parse metadata for {table_name} → {e}")
            return None

    # --------------------------------------------------------------------
    #                 Internal Execution Helpers (Build Steps)
    # --------------------------------------------------------------------

    def _run_build_step(
        self,
        table_name: str,
        prepare_func: Callable[[], Optional[Tuple[DataFrame, str]]],
        relative_path: str,
        mode: str = "overwrite",
    ):
        """
        Executes a full transformation step for a given table.

        This method:
        - Calls a preparation function returning (DataFrame, source)
        - Skips write if the DataFrame is None
        - Writes the DataFrame to Delta Lake in the given mode
        - Updates and saves transformation metadata (duration, count, status)
        - Logs each stage of the build process

        Args:
            table_name (str): Logical name of the table (used in logs/metadata)
            prepare_func (Callable): Function returning (DataFrame, source_path)
            relative_path (str): Output path inside the silver layer
            mode (str): Write mode ("overwrite", "append", etc.)
        """

        started_at = self.timestamp
        df, source = prepare_func()

        self.metadata.update(
            {
                "table": table_name,
                "source_snapshot": (
                    "from broadcasted_exchange_info_df" if table_name == "dim_exchange_map" else str(source)
                ),
                "started_at": started_at,
            }
        )

        if df is None:
            self.log(f"No data to build {table_name} — skipping write.")
            self.metadata.update(
                {
                    "status": "skipped",
                    "notes": "No DataFrame returned.",
                    "current_status": "raw",
                }
            )
            self.save_metadata(table_name, self.metadata)
            return

        status = self.write_delta(df=df, relative_path=relative_path, mode=mode)
        ended_at = self.timestamp

        self.metadata.update(
            {
                "ended_at": ended_at,
                "duration_seconds": (
                    datetime.fromisoformat(ended_at) - datetime.fromisoformat(started_at)
                ).total_seconds(),
                "record_count": df.count(),
                "status": "success" if status == "ok" else "failed",
                "current_status": "transformed" if status == "ok" else "raw",
                "notes": "ready to load step" if status == "ok" else "re-transform nedeed",
            }
        )

        if status == "ok":
            self.log(f"{table_name} table successfully built and written.")
        else:
            self.log(f"[ERROR] Failed to write {table_name} table.")
            return

        self.save_metadata(str(table_name), self.metadata)

    def should_transform(
        self, 
        table_name: str, 
        latest_snapshot_path: Path, 
        force: bool = False, 
        daily_comparison: bool = True
    ) -> bool:
        """
        Determines whether a table should be transformed based on the freshness of its latest snapshot.

        This method compares the timestamp extracted from the latest .parquet snapshot filename
        (format: `name-YYYY-MM-DD_HH-MM-SS.parquet`) with the `ended_at` timestamp recorded in the
        table's most recent transformation metadata.

        If `daily_comparison` is enabled, only the date parts (YYYY-MM-DD) are compared.
        If `force` is True, the transformation proceeds regardless of timestamps.

        Args:
            table_name (str): Logical name of the table to check.
            latest_snapshot_path (Path): Path to the latest snapshot (.parquet file).
            force (bool): If True, forces the transformation to run regardless of metadata.
            daily_comparison (bool): If True, compares only dates (not full timestamps).

        Returns:
            bool: True if transformation should proceed, False otherwise.
        """
        if force : 
            return True

        try:
            # Extract timestamp from file name like: exchange_map-2025-05-07_12-34-56.parquet
            ts_str = latest_snapshot_path.stem.split("-", maxsplit=1)  # ['exchange_map'  '2025-05-07_12-34-56']
            date_part = ts_str[1].split("_", maxsplit=1)[0]  # '2025-05-07'
            time_part = ts_str[1].split("_", maxsplit=1)[1].replace("-", ":")  # '12:34:56'
            snapshot_str = f"{date_part} {time_part}"  # '2025-05-07 12:34:56'

            snapshot_dt = datetime.strptime(snapshot_str, "%Y-%m-%d %H:%M:%S")

            if daily_comparison:
                new_snapshot = snapshot_dt.date()
            else:
                new_snapshot = snapshot_dt

        except Exception as e:
            self.log(f"[ERROR] Failed to parse timestamp from file name {latest_snapshot_path.name} -> {e}")
            return True  # Safe default

        # Reading last trasnformation time from the metadata
        last_metadata = self.read_last_metadata(table_name)
        last_ended_at = last_metadata.get("ended_at") if last_metadata else None

        if not last_ended_at:
            self.log(f"[INFO] No prior ended_at found for {table_name}. Proceeding.")
            return True

        try:
            last_ts = datetime.fromisoformat(last_ended_at).replace(tzinfo=None)

            if daily_comparison:
                last_value = last_ts.date()

            else:
                last_value = last_ts

            self.log(f"Last .parquet record at : {new_snapshot} and last transformed at {last_value}")
            return new_snapshot > last_value

        except Exception as e:
            self.log(f"[ERROR] Failed to parse ended_at date from metadata → {e}")
            return True
