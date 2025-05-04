from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timezone
from abc import ABC, abstractmethod
from pathlib import Path


class BaseTransformer(ABC):
    """
    Abstract base class for Spark transformation modules (one per business domain).

    Responsibilities:
    - Receives a domain-specific SparkSession
    - Provides structured and centralized logging to 'logs/transform.log'
    - Exposes helper methods for:
        - Logging events and transformation stages
        - Reading input data from the bronze layer (Parquet)
        - Writing output data to the silver layer (Delta Lake)
        - Inspecting DataFrame shape and schema
    - Encourages traceability by logging DataFrame info before and after transformations
    - Enforces a standard .run() method to be implemented by subclasses
    """

    def __init__(self, spark: SparkSession):
        # --------------------------------------------------------------------
        #                         Core Attributes
        # --------------------------------------------------------------------

        self.spark = spark
        self.name = self.__class__.__name__

        # Unified UTC timestamp for all log lines of this transformer instance
        self.timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # Dynamically resolves the root of the project
        self.project_root = Path(__file__).resolve().parents[3]

        # Centralized log file
        self.log_path = self.project_root / "logs/transform.log"

        # Input/output data stores
        self.raw_data_path = self.project_root / "data/bronze"
        self.silver_data_path = self.project_root / "data/silver"

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

        formatted = f"[{self.timestamp}] [TRANSFORM] [{self.name.upper()}] {message}"

        with open(self.log_path, "a") as f:
            f.write(style if style else formatted + "\n")

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

        Recommended usage:
        - Before and after each transformation step
        - For tracking schema changes and volume variation
        - For debugging data issues across stages

        Args:
            df (DataFrame): The Spark DataFrame to inspect.
            label (str): Optional label to contextualize the log (e.g., 'Before filtering', 'After join').
        """

        count = df.count()
        schema = df.schema.simpleString()

        self.log(f"[{label}] Row count: {count}, Schema: {schema}")

    # --------------------------------------------------------------------
    #                       I/O Helper Methods
    # --------------------------------------------------------------------

    def read_parquet(self, relative_path: str) -> DataFrame:
        """
        Reads a Parquet file from the raw data (bronze) layer.

        Args:
            relative_path (str): Subpath to the Parquet file relative to bronze folder.

        Returns:
            DataFrame: The loaded Spark DataFrame.
        """

        full_path = self.raw_data_path / relative_path
        self.log(f"Reading Parquet file from: {full_path}")

        return self.spark.read.parquet(str(full_path))

    def write_delta(self, df: DataFrame, relative_path: str, mode: str = "overwrite") -> None:
        """
        Writes a DataFrame to the Delta format in the silver layer.

        Args:
            df (DataFrame): The DataFrame to write.
            relative_path (str): Subpath to the silver output directory.
            mode (str): Spark write mode. Default is "overwrite".
        """

        full_path = self.silver_data_path / relative_path
        self.log(f"Writing Delta file to: {full_path} (mode={mode})")

        df.write.format("delta").mode(mode).save(str(full_path))

    def read_delta(self, relative_path: str) -> DataFrame:
        """
        Reads a Delta table from the silver layer.

        Args:
            relative_path (str): Subpath to the Delta table in silver layer.

        Returns:
            DataFrame: The loaded Spark DataFrame.
        """

        full_path = self.silver_data_path / relative_path
        self.log(f"Reading Delta table from: {full_path}")

        return self.spark.read.format("delta").load(str(full_path))

    # --------------------------------------------------------------------
    #                      Abstract Execution Method
    # --------------------------------------------------------------------

    @abstractmethod
    def run(self) -> None:
        """
        Main entrypoint to run the full transformation process for a domain.

        Must be implemented by each concrete transformer class.
        """
        pass
