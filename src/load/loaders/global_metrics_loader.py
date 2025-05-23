from load.base.base_loader import BaseLoader
from pyspark.sql import SparkSession


class GlobalMetricsLoader(BaseLoader):
    """
    Loader class responsible for processing and loading the fact_global_market table
    from the silver Delta layer into the data warehouse.
    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark)

    def load_fact_global_market(self, version: int = None) -> None:
        """
        Loads the fact_global_market table from the Delta silver layer into the data warehouse.

        This method assumes that the specified Delta version corresponds to a fully transformed
        snapshot that has not yet been loaded into the warehouse.

        Important:
        The version being loaded must not contain snapshot_timestamp values
        already present in the data warehouse, due to a primary key constraint.

        Args:
            version (int): Delta version to load. Must be known and valid.

        Returns:
            bool: True if load was performed successfully, False if skipped or failed.
        """
        table = "fact_global_market"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table):
            self.log_section(title=f"Loading Skipped for {table}")
            return 

        status = self._load_fact_or_variant_table(
            table_name=table, 
            version=version, 
            mode="append", 
            notes="Load from transformed global metrics delta table"
        )

        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")
