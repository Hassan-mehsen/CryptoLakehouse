from extract.base_extractor import BaseExtractor
from typing import List, Generator, Tuple, Optional
from pandas import DataFrame
import time
import json


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
        super().__init__(name="crypto_category", endpoint="/v1/cryptocurrency/category", output_dir="crypto_category_data")

        self.MAX_RETRIES = 3
        self.params = {"id": None, "start": "1", "limit": "1000"}
        self.snapshot_info = {
            "source_endpoint": self.endpoint,
            "crypto_categories_snapshot_ref": None,
            "total_category_fetched": None,
            "category_ids": []
        }

    def find_category_ids_to_fetch(self) -> List[str]:
        """
        Identifies which category IDs still need to be fetched based on snapshot comparisons.

        - If no snapshot from `/category` exists (first run), all active category IDs from
        the `/categories` snapshot are returned (full initialization).
        - If a snapshot exists, only the category IDs missing from it are returned (refresh mode).

        Returns:
            List[str]: List of category IDs to be fetched from the `/category` endpoint.
        """

        crypto_categories_path = self.PROJECT_ROOT / "src/api_clients/crypto_categories/snapshot_info.jsonl"

        # Load last crypto categories snapshot
        try:
            with open(crypto_categories_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in reversed(lines):
                    if line.strip():
                        crypto_categories_snapshot = json.loads(line.strip())
                        available_category_ids = set(crypto_categories_snapshot.get("category_ids", []))
                        self.snapshot_info["crypto_categories_snapshot_ref"] = crypto_categories_snapshot.get("snapshot_date")
                        break
        except Exception as e:
            self.log(f"Error reading crypto_categories snapshot: {e}")
            return []

        # Load last snapshot from /category (already fetched category IDs)
        # if not found or empty, treat as initialization (fetch all crypto categories).
        crypto_info_snapshot = self.read_last_snapshot()
        if crypto_info_snapshot and crypto_info_snapshot.get("category_ids", []):
            crypto_category_ids = set(crypto_info_snapshot.get("category_ids", []))
        else:
            crypto_category_ids = set()

        # Compute the missing ones
        ids_to_fetch = available_category_ids - crypto_category_ids
        self.log(
            f"{len(available_category_ids)} categories available from snapshot, "
            f"{len(crypto_category_ids)} categories already fetched. "
            f"{len(ids_to_fetch)} remaining to fetch."
        )
        self.snapshot_info["category_ids"] = sorted(list(crypto_category_ids.union(ids_to_fetch)))
        self.snapshot_info["total_category_fetched"] = len(ids_to_fetch)

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
                    self.log(f"Attempt {attempt} failed for category {i} (id={category_id[:6]}...). Retrying in {2**attempt}s...")
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
        Main execution method:

        - Identifies category IDs still to be processed using snapshot comparison.
        - For each missing category, fetches the list of coins it contains from the API.
        - Parses and flattens the (crypto_id, category_id) relationships.
        - Writes results to a Parquet file and updates snapshot metadata.
        - Supports debug mode to save raw parsed output for inspection.

        The pipeline ensures idempotence and efficient API usage, avoiding redundant calls and duplicate storage.
        """

        self.log_section("START CryptoCategoryExtractor")

        category_ids = self.find_category_ids_to_fetch()

        if not category_ids:
            self.log("No category IDs to fetch. Skipping extraction.")
            self.log_section("END CryptoCategoryExtractor")
            return

        all_links = []

        for raw_data, pos in self.fetch_crypto_category(category_ids=category_ids):
            parsed_category = self.parse(raw_category_data=raw_data, id_pos=pos)
            if parsed_category:
                all_links.extend(parsed_category)

        if not all_links:
            self.log("No valid cryptocurrency-category links found.")
            self.log_section("END CryptoCategoryExtractor")
            return

        if debug:
            self.save_raw_data(all_links, filename="debug_crypto_category.json")
            self.log("Debug mode: Raw parsed records saved.")

        df = DataFrame(all_links, columns=["crypto_id", "category_id"])
        df["date_snapshot"] = self.df_snapshot_date
        self.log(f"Snapshot timestamp: {self.df_snapshot_date}")

        self.snapshot_info["total_category_fetched"] = len(self.snapshot_info["category_ids"])
        self.write_snapshot_info(self.snapshot_info)

        self.save_parquet(df, filename="link_crypto_category")
        self.log(f"DataFrame saved with {len(df)} rows.")

        self.log_section("END CryptoCategoryExtractor")
