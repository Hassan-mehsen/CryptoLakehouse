from load.base.base_loader import BaseLoader
from pyspark.sql import SparkSession


class FearAndGreedLoader(BaseLoader):
    """
    Loader class responsible for writing the fact_market_sentiment table
    from the Delta silver layer into the data warehouse.

    Inherits all shared functionality from BaseLoader:
    - Reads Delta tables (optionally by version)
    - Applies conditional loading logic based on transformation metadata
    - Logs execution details and persists load metadata for traceability

    This class is specific to the 'fear_and_greed' domain and delegates
    the actual load logic to the standardized base implementation.
    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark)

    def load_fact_market_sentiment(self, version: int = None) -> None:
        """
        Loads the fact_market_sentiment table from the Delta silver layer into the data warehouse.
        This table is snapshot-based and does not require deduplication.
        This loader reads a fully transformed Delta snapshot and writes it to the DWH in append mode.
        It uses metadata to ensure that only new, non-duplicated transformations are loaded.

        Args:
            version (int, optional): Delta version to load. If None, the latest transformed version is used.

        Returns:
            None. All logs and metadata are persisted during the process.
        """

        table = "fact_market_sentiment"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table):
            self.log_section(title=f"Loading Skipped for {table}")
            return

        status = self._load_fact_or_variant_table(
            table_name=table,
            fk_presence=False,
            version=version,
            mode="append",
            notes="Load from transformed fear and greed delta table",
        )

        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")
