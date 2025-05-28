from load.base.base_loader import BaseLoader
from pyspark.sql import SparkSession


class CryptoLoader(BaseLoader):
    """
    Loader class for the Crypto domain.

    Responsible for loading all crypto-related fact and dimension tables
    from the Delta silver layer into a PostgreSQL data warehouse.

    Loading Strategy:
    - Respect dependency order:
    1. Load stable dimensions without FK (e.g. dim_crypto_id)
    2. Load dimensions with FK dependencies (e.g. dim_crypto_info)
    3. Load link tables and facts referencing those dimensions

    Behavior:
    - Stable dimension tables: deduplicated using primary key filtering
    - Variant/fact tables: fully appended (overwritten at the transform stage)

    Highlights:
    - Foreign key validation is enforced where applicable
    - Lightweight append loads with version tracking
    - Skip logic enabled unless a specific version is forced
    - Full metadata persistence and logging for auditability
    """

    def __init__(self, spark: SparkSession):
        super().__init__(spark)

    def load_dim_crypto_id(self, version: int = None) -> None:
        """
        Loads the dim_crypto_id stable dimension table.

        This is the root table of the domain and must be loaded first, as many
        other tables reference it. Applies deduplication on 'crypto_id'.

        Args:
            version (int, optional): Delta version to load. Defaults to latest.
        """
        table = "dim_crypto_id"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table) and version is None:
            self.log_section(title=f"Loading Skipped for {table}")
            return

        status = self._load_stable_dim_table(
            table_name=table,
            fk_presence=False,
            pk_columns=["crypto_id"],
            version=version,
            mode="append",
            notes="Load from transformed dim_crypto_id delta table",
        )
        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")

    def load_dim_crypto_info(self, version: int = None) -> None:
        """
        Loads the dim_crypto_info stable dimension table.

        This table depends on `dim_crypto_id`. Applies deduplication on `crypto_id`.
        Foreign key integrity is enforced before insert to ensure referential consistency.

        Args:
            version (int, optional): Delta version to load. Defaults to latest.
        """
        table = "dim_crypto_info"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table) and version is None:
            self.log_section(title=f"Loading Skipped for {table}")
            return

        status = self._load_stable_dim_table(
            table_name=table,
            fk_presence=True,
            fks_ref_table={"crypto_id": "dim_crypto_id"},
            pk_columns=["crypto_id"],
            version=version,
            mode="append",
            notes="Load from transformed dim_crypto_info delta table",
        )
        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")

    def load_dim_crypto_category(self, version: int = None) -> None:
        """
        Loads the dim_crypto_category stable dimension table.

        This table is a reference dimension used by the crypto-category link table.
        Applies deduplication on 'category_id'.

        Args:
            version (int, optional): Delta version to load. Defaults to latest.
        """
        table = "dim_crypto_category"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table) and version is None:
            self.log_section(title=f"Loading Skipped for {table}")
            return

        status = self._load_stable_dim_table(
            table_name=table,
            fk_presence=False,
            pk_columns=["category_id"],
            version=version,
            mode="append",
            notes="Load from transformed dim_crypto_category delta table",
        )
        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")

    def load_crypto_category_link(self, version: int = None) -> None:
        """
        Loads the crypto_category_link table.

        This is a many-to-many association table linking cryptos and categories.
        Deduplication is applied on the composite key (category_id, crypto_id).
        Foreign key constraints are validated against both dimension tables to ensure data consistency.

        Additionally, all distinct category_ids currently in dim_crypto_category are collected
        and stored in the load metadata for downstream usage (e.g., forecasting, filtering, etc.).

        Args:
            version (int, optional): Delta version to load. Defaults to latest.
        """
        table = "crypto_category_link"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table) and version is None:
            self.log_section(title=f"Loading Skipped for {table}")
            return

        # Read all category IDs from dim_crypto_category table
        df_category = self.read_from_dw("dim_crypto_category", columns=["category_id"])
        if df_category is not None:
            category_list = [row["category_id"] for row in df_category.select("category_id").collect()]
            self.load_metadata.update({"categories_count": len(category_list)})
            self.load_metadata.update({"category_ids": category_list})

        status = self._load_stable_dim_table(
            table_name=table,
            fk_presence=True,
            fks_ref_table={"category_id": "dim_crypto_category", "crypto_id": "dim_crypto_id"},
            pk_columns=["category_id", "crypto_id"],
            version=version,
            mode="append",
            notes="Load from transformed crypto_category_link delta table",
        )
        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")

    def load_dim_crypto_map(self, version: int = None) -> None:
        """
        Loads the dim_crypto_map variant dimension table.

        This table contains dynamic mapping metadata for cryptos.
        No deduplication is applied -- the entire transformed snapshot is appended as-is.
        Foreign key integrity is enforced on referenced IDs before loading.

        Args:
            version (int, optional): Delta version to load. Defaults to latest.
        """
        table = "dim_crypto_map"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table) and version is None:
            self.log_section(title=f"Loading Skipped for {table}")
            return

        status = self._load_fact_or_variant_table(
            table_name=table,
            fk_presence=True,
            fks_ref_table={"crypto_id": "dim_crypto_id"},
            version=version,
            mode="append",
            notes="Load from transformed dim_crypto_map delta table",
        )

        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")

    def load_fact_crypto_category(self, version: int = None) -> None:
        """
        Loads the fact_crypto_category fact table.

        This table captures aggregated metrics per crypto-category snapshot.
        All rows from the transformed Delta table are appended at each load.
        Foreign key integrity is enforced before insertion to ensure consistency.

        Args:
            version (int, optional): Delta version to load. Defaults to latest.
        """
        table = "fact_crypto_category"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table) and version is None:
            self.log_section(title=f"Loading Skipped for {table}")
            return

        status = self._load_fact_or_variant_table(
            table_name=table,
            fk_presence=True,
            fks_ref_table={"category_id": "dim_crypto_category"},
            version=version,
            mode="append",
            notes="Load from transformed fact_crypto_category delta table",
        )

        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")

    def load_fact_crypto_market(self, version: int = None) -> None:
        """
        Loads the fact_crypto_market fact table.

        This is the core table containing market metrics for individual cryptos.
        All transformed rows are appended at each execution (no deduplication).
        Foreign key constraints are validated to ensure referential integrity.

        Args:
            version (int, optional): Delta version to load. Defaults to latest.
        """
        table = "fact_crypto_market"
        self.log_section(title=f"Loading Started for {table}")

        if not self.should_load(table) and version is None:
            self.log_section(title=f"Loading Skipped for {table}")
            return

        status = self._load_fact_or_variant_table(
            table_name=table,
            fk_presence=True,
            fks_ref_table={"crypto_id": "dim_crypto_id"},
            version=version,
            mode="append",
            notes="Load from transformed fact_crypto_market delta table",
        )

        if status:
            self.log(f"Loaded operation for {table} succeeded", table_name=table)
        else:
            self.log(f"[ERROR] Load failed or incomplete for {table}", table_name=table)

        self.log_section(title=f"Loading Ended for {table}")

    def load_all(self, version: int = None) -> None:
        """
        Sequentially loads all crypto-related tables in dependency order.

        Intended for testing, demonstrations or batch reloads only.
        Not designed for production scheduling (where tables are loaded independently).

        Args:
            version (int, optional): Delta version to use for all tables. If None, latest is used.
        """
        self.log_section("Crypto Loader - Start Full Domain Load")
        self.load_dim_crypto_id(version=version)
        self.load_dim_crypto_info(version=version)
        self.load_dim_crypto_category(version=version)
        self.load_crypto_category_link(version=version)
        self.load_dim_crypto_map(version=version)
        self.load_fact_crypto_market(version=version)
        self.load_fact_crypto_category(version=version)
        self.log_section("Crypto Loader - End Full Doamin Load")
