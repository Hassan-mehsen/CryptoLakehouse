from load.base.base_loader import BaseLoader
from pyspark.sql import SparkSession


class ExchangeLoader(BaseLoader):
    """
    Loader class for the Exchange domain.

    This class is responsible for loading all exchange-related fact and dimension tables
    from the Delta silver layer into the PostgreSQL data warehouse.

    Strategy:
    - Tables must be loaded in dependency order:
    1. Stable dimension tables without foreign key dependencies
    2. Stable dimension tables with FK constraints
    3. Variant dimension and fact tables that rely on these dimensions

    Behavior:
    - Stable dimension tables: deduplicated using primary key filtering
    - Variant and fact tables: fully appended (overwrite is handled in the transform phase)
    - Optional foreign key validation is enforced via semi joins before insertion

    Important:
    - The `fact_exchange_assets` table depends on both `dim_exchange_id` and `dim_crypto_id`.
    These dimensions must be loaded first to satisfy referential integrity.
    - Metadata and logs are persisted for traceability and reproducibility.
    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark)

    def load_dim_exchange_id(self, version: int = None) -> None:
        """
        Loads the dim_exchange_id stable dimension table into the data warehouse.

        Deduplicates on 'exchange_id' to ensure no repeated values are inserted.
        Only new rows (not present in the DW) are appended.

        Args:
            version (int, optional): Specific Delta version to read. If None, latest version is used.
        """
        table = "dim_exchange_id"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table) and not version:
            self.log_section(title=f"Loading Skipped for {table}")
            return

        status = self._load_stable_dim_table(
            table_name=table,
            fk_presence=False,
            pk_columns=["exchange_id"],
            version=version,
            mode="append",
            notes="Load from transformed dim_exchange_id delta table",
        )

        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")

    def load_dim_exchange_info(self, version: int = None) -> None:
        """
        Loads the dim_exchange_info stable dimension table into the data warehouse.

        This table contains enriched metadata about exchanges.
        Deduplication is enforced using `exchange_id` as the primary key,
        to ensure no repeated values are inserted.
        Foreign key validation is performed against `dim_exchange_id`.

        Args:
            version (int, optional): Specific Delta version to load. If None, loads latest.
        """

        table = "dim_exchange_info"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table) and not version:
            self.log_section(title=f"Loading Skipped for {table}")
            return

        status = self._load_stable_dim_table(
            table_name=table,
            fk_presence=True,
            fks_ref_table={"exchange_id": "dim_exchange_id"},
            pk_columns=["exchange_id"],
            version=version,
            mode="append",
            notes="Load from transformed dim_exchange_info delta table",
        )

        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")

    def load_dim_exchange_map(self, version: int = None) -> None:
        """
        Loads the dim_exchange_map variant dimension table into the data warehouse.

        This table is snapshot-based and does not require deduplication.
        It has a foreign key dependency on `exchange_id` from dim_exchange_id.

        All transformed rows are appended to the data warehouse.

        Args:
            version (int, optional): Specific Delta version to load. If None, loads latest.
        """
        table = "dim_exchange_map"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table) and not version:
            self.log_section(title=f"Loading Skipped for {table}")
            return

        status = self._load_fact_or_variant_table(
            table_name=table,
            fk_presence=True,
            fks_ref_table={"exchange_id": "dim_exchange_id"},
            version=version,
            mode="append",
            notes="Load from transformed exchange map delta table",
        )

        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")

    def load_fact_exchange_assets(self, version: int = None) -> None:
        """
        Loads the fact_exchange_assets fact table into the data warehouse.

        This fact table contains transactional-level data linking exchanges and crypto assets.
        Foreign key constraints are enforced on:
        - `exchange_id` (must exist in dim_exchange_id)
        - `crypto_id` (must exist in dim_crypto_id)

        This table is snapshot-based and does not require deduplication.
        The load operates in append mode, and only new transformation snapshots are considered.

        Args:
            version (int, optional): Specific Delta version to load. If None, loads latest available.
        """
        table = "fact_exchange_assets"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table) and not version:
            self.log_section(title=f"Loading Skipped for {table}")
            return

        status = self._load_fact_or_variant_table(
            table_name=table,
            fk_presence=True,
            fks_ref_table={"crypto_id": "dim_crypto_id", "exchange_id": "dim_exchange_id"},
            version=version,
            mode="append",
            notes="Load from transformed fact_exchange_assets delta table",
        )

        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")

    def load_all(self, version: int = None) -> None:
        """
        Loads all exchange-related tables (dimension and fact) sequentially.

        This is a utility method intended primarily for demonstrations, debugging, or local testing.
        It triggers the full domain loading process in a fixed order, with domain-level logging.

        This method is not meant for production orchestration, where each table is typically loaded
        via independent DAG tasks or loaders.

        Args:
            version (int, optional): Delta version to load. If None, loads latest transformed versions.
        """
        self.log_section("Exchange Loader - Start Full Domain Load")
        self.load_dim_exchange_id(version)
        self.load_dim_exchange_info(version)
        self.load_dim_exchange_map(version)
        self.load_fact_exchange_assets(version)
        self.log_section("Exchange Loader - End Full Doamin Load")
