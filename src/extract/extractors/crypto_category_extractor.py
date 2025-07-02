from typing import List, Generator, Tuple, Optional
from pandas import DataFrame
from pathlib import Path
import time
import json
import sys

# Resolve  path dynamically
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from extract.base_extractor import BaseExtractor


class CryptoCategoryExtractor(BaseExtractor):
    """
    CryptoCategoryExtractor:

    Extracts crypto-to-category mappings from the `/v1/cryptocurrency/category` endpoint of CoinMarketCap.

    This extractor identifies the links between cryptocurrencies and their respective thematic categories
    (e.g., "DeFi", "A16Z Portfolio", etc.), and stores them in the `link_crypto_category` DF for analytical purposes.

    Execution Logic:
    - Compares current list of available categories (from `/categories` snapshot)
    with already fetched ones (from its own snapshot).
    - Only fetches new or missing category mappings.
    - Implements retry and rate-limit handling for each API call.

    Use Case:
    - Enables relational joins between assets and thematic groups in downstream BI tools or dashboards.
    - Supports incremental updates while maintaining low API usage.

    The result is a lean but comprehensive many-to-many relationship table.
    """

    def __init__(self):
        super().__init__(
            name="crypto_category", endpoint="/v1/cryptocurrency/category", output_dir="crypto_category_data"
        )

        self.status = ""
        self.MAX_RETRIES = 3
        self.params = {"id": None, "start": "1", "limit": "1000"}
        self.snapshot_info = {
            "source_endpoint": self.endpoint,
            "extract_snapshot_ref": "",
            "load_snapshot_ref": "",
            "num_available_categories": "",
            "num_loaded_categories": "",
            "category_ids_to_fetch": "",
            "num_categories_to_fetch": "",
        }

    def find_category_ids_to_fetch(self) -> List[str]:
        """
        Determines which category IDs need to be fetched from the /category endpoint.

        Logic:
        - Step 1: Load the last list of active category IDs from the latest /categories snapshot
        (stored in `metadata/extract/crypto_categories/snapshot_info.jsonl`).
        - Step 2: Load the last list of category IDs already extracted and linked to cryptos
        (from `metadata/load/crypto/crypto_category_link.jsonl`).
        - Step 3: Compare the two snapshots to find the missing IDs.
            - If no snapshot is found (first run), all categories will be fetched.
            - If snapshots are found, only the delta is processed.

        Returns:
            List[str]: Sorted list of category IDs to be fetched from the /category endpoint.
        """

        extract_snapshot_path = self.PROJECT_ROOT / "metadata/extract/crypto_categories/snapshot_info.jsonl"
        load_snapshot_path = self.PROJECT_ROOT / "metadata/load/crypto/crypto_category_link.jsonl"

        category_ids_from_extract = set()
        category_ids_already_fetched = set()

        # Step 1: Load extract snapshot (list of all categories)
        try:
            with open(extract_snapshot_path, "r", encoding="utf-8") as f:
                for line in reversed(f.readlines()):
                    if line.strip():
                        extract_snapshot = json.loads(line.strip())
                        category_ids_from_extract = set(extract_snapshot.get("category_ids", []))
                        self.snapshot_info["extract_snapshot_ref"] = extract_snapshot.get("snapshot_date")
                        self.snapshot_info["num_available_categories"] = len(category_ids_from_extract)
                        break
        except Exception as e:
            self.log(f"[WARNING] Could not load extract snapshot: {e}")

        if not self.read_last_snapshot():
            # First run no metadata in metadata/extract/crypto_category/ : fetch everything
            self.log(f"No previous load snapshot found --> full initialization mode.")
            ids_to_fetch = category_ids_from_extract

        else:
            # Step 2: Load load snapshot (categories already fetched)
            try:
                with open(load_snapshot_path, "r", encoding="utf-8") as f:
                    for line in reversed(f.readlines()):
                        if line.strip():
                            load_snapshot = json.loads(line.strip())
                            category_ids_already_fetched = set(load_snapshot.get("category_ids", []))
                            self.snapshot_info["load_snapshot_ref"] = load_snapshot.get("started_at")
                            self.snapshot_info["num_loaded_categories"] = len(category_ids_already_fetched)
                            break
            except Exception as e:
                self.log(f"[WARNING] Could not load load snapshot: {e}")

            # Refresh mode: compute difference
            ids_to_fetch = category_ids_from_extract - category_ids_already_fetched
            self.log(
                f"{len(category_ids_from_extract)} categories available in /categories snapshot, "
                f"{len(category_ids_already_fetched)} already fetched, "
                f"{len(ids_to_fetch)} remaining to fetch."
            )
            if len(ids_to_fetch) == 0:
                ids_to_fetch = ["skip"]
                self.status = "skip"

        # Log and trace metadata
        self.snapshot_info["category_ids_to_fetch"] = sorted(ids_to_fetch)
        self.snapshot_info["num_categories_to_fetch"] = len(ids_to_fetch)

        return list(ids_to_fetch)

    def fetch_crypto_category(self, category_ids: List[str]) -> Generator[Tuple[dict, int], None, None]:
        """
        Generator that fetches crypto-category link data for each category ID.
        Implements retry logic with exponential backoff and logs progress.

        Args:
            category_ids (List[str]): List of category IDs to fetch.

        Yields:
            Tuple[dict, int]: Tuple of raw data dictionary and index in iteration.
        """

        for i, category_id in enumerate(category_ids, start=1):
            success = False

            for attempt in range(1, self.MAX_RETRIES + 1):
                self.params["id"] = category_id
                response = self.get_data(params=self.params)

                if response and response.get("data", {}):
                    self.log(f"Fetching category {i}/{len(category_ids)} (id={category_id[:6]}...)")
                    self.log("Waiting 2s to respect API rate limit")
                    yield response.get("data", {}), i
                    success = True
                    time.sleep(2)
                    break
                else:
                    self.log(
                        f"Attempt {attempt} failed for category {i} (id={category_id[:6]}...). Retrying in {2**attempt}s..."
                    )
                    time.sleep(2**attempt)

            if not success:
                self.log(f"Failed to fetch category {i} (id={category_id[:6]}...) after {self.MAX_RETRIES} attempts.")

    # Override of BaseExtractor.parse
    def parse(self, raw_category_data: dict, id_pos: int) -> Optional[List[Tuple[int, str]]]:
        """
        Parses a category response to extract (crypto_id, category_id) links.

        Args:
            raw_category_data (dict): Response from /cryptocurrency/category endpoint.
            id_pos (int): Index of the category in the batch (for logging).

        Returns:
            Optional[List[Tuple[int, str]]]: List of (crypto_id, category_id) pairs.
        """

        if not raw_category_data:
            self.log(f"No category data found for the {id_pos}th ID.")
            return

        category_id = raw_category_data.get("id")
        if not category_id:
            self.log(f"No category_id found in response at position {id_pos}.")
            return None

        links = []

        if raw_category_data.get("coins", []):
            for crypto in raw_category_data.get("coins", []):
                try:
                    crypto_id = crypto.get("id")
                    if crypto.get("id") is not None:
                        links.append((crypto_id, category_id))
                except Exception as e:
                    self.log(f"Error parsing crypto_id in category {id_pos}: {str(e)}")
                    continue

        return links if links else None

    # Override of BaseExtractor.run
    def run(self, debug: bool = False) -> None:
        """
        Main execution method for the extractor.

        Steps:
        - Identify missing category IDs to process via snapshot comparison.
        - Fetch data for each category ID from the CoinMarketCap API.
        - Parse and flatten the (crypto_id, category_id) relationships.
        - Log and optionally save raw data if debug mode is enabled.
        - Create DataFrame, add snapshot timestamp.
        - Save the data to Parquet and update snapshot metadata.

        Ensures:
        - Idempotent behavior (only new categories fetched).
        - Efficient API usage with retry and delay handling.
        - Traceable and structured data lineage.
        """

        self.log_section("START CryptoCategoryExtractor")

        # Step 1: Identify category IDs to fetch
        category_ids = self.find_category_ids_to_fetch()

        if not category_ids:
            self.log("No category IDs to fetch. Skipping extraction.")
            self.log_section("END CryptoCategoryExtractor")
            return

        if self.status == "skip":
            self.log("All categories are already fetched. Nothing left to process.")
            self.write_snapshot_info(self.snapshot_info)
            self.log_section("END CryptoCategoryExtractor")
            return

        # Step 2: Fetch and parse data for each category
        all_links = []
        for raw_data, pos in self.fetch_crypto_category(category_ids=category_ids):

            parsed_category = self.parse(raw_category_data=raw_data, id_pos=pos)
            if parsed_category:
                all_links.extend(parsed_category)

        # Step 3: Validate results
        if not all_links:
            self.log("No valid cryptocurrency-category links found.")
            self.log_section("END CryptoCategoryExtractor")
            return

        # Step 4 (optional): Save raw data for debugging
        if debug:
            self.save_raw_data(all_links, filename="debug_crypto_category.json")
            self.log("Debug mode: Raw parsed records saved.")

        # Step 5: Convert to DataFrame and add snapshot timestamp
        df = DataFrame(all_links, columns=["crypto_id", "category_id"])
        df["date_snapshot"] = self.df_snapshot_date
        self.log(f"Snapshot timestamp: {self.df_snapshot_date}")

        # Step 6: write snapshot metadata
        self.write_snapshot_info(self.snapshot_info)

        # Step 7: Save as Parquet
        self.save_parquet(df, filename="link_crypto_category")
        self.log(f"DataFrame saved with {len(df)} rows.")

        self.log_section("END CryptoCategoryExtractor")
