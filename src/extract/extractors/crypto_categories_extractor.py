from extract.base_extractor import BaseExtractor
from pandas import DataFrame, to_numeric
from typing import List, Optional
import time


class CryptoCategoriesExtractor(BaseExtractor):
    """
    Extractor class for pulling high-level cryptocurrency category data from
    CoinMarketCap's `/v1/cryptocurrency/categories` endpoint.

    Categories group coins under shared characteristics (e.g., DeFi, Gaming, A16Z Portfolio).
    This extractor:
    - Retrieves the full list of categories (up to 5000) in a single API call.
    - Parses financial and structural metadata per category.
    - Tracks snapshot info (category count, IDs) for traceability, though not used for filtering.
    - Intended for complete refreshes (e.g., daily) rather than incremental updates.
    """

    def __init__(self):
        super().__init__(name="crypto_categories", endpoint="/v1/cryptocurrency/categories", output_dir="crypto_categories_data")

        self.params = {"start": "1", "limit": "5000"}
        self.snapshot_info = {
            "source_endpoint": self.endpoint, 
            "total_categories": None,
            "category_ids": None
        }
        self.MAX_RETRIES = 3
        self.category_ids = []

    def fetch_all_categories(self) -> Optional[dict]:
        """
        Performs a single API call (with retries) to retrieve all cryptocurrency categories.

        Returns:
            dict | None: Raw API response containing a list of category entries, or None on failure.
        """
        attempt = 1
        while attempt <= self.MAX_RETRIES:
            raw_data = self.get_data(params=self.params)

            if raw_data and raw_data.get("data"):
                self.log(f"Successfully fetched {len(raw_data['data'])} categories on attempt {attempt}.")
                return raw_data

            self.log(f"Attempt {attempt} failed to fetch categories. Retrying in {2 ** attempt}s...")
            time.sleep(2**attempt)
            attempt += 1

        self.log(f"Failed to fetch categories after {self.MAX_RETRIES} attempts.")
        return None

    # Override of BaseExtractor.parse
    def parse(self, raw_data: dict) -> Optional[List[dict]]:
        """
        Parses raw API response into a list of flat category records.

        Args:
            raw_data (dict): The full raw response returned by the fetch method.

        Returns:
            list[dict] | None: Flattened category records, or None if parsing fails or data is missing.
        """
        if not raw_data:
            self.log("No categories found in response.")
            return None

        if not isinstance(raw_data.get("data"), list):
            self.log("Unexpected response structure: 'data' is not a list.")
            return

        record = []

        for cat in raw_data.get("data", []):
            if isinstance(cat, dict):
                try:
                    self.category_ids.append(cat.get("id"))
                    record.append(
                        {
                            "category_id": cat.get("id"),
                            "name": cat.get("name"),
                            "title": cat.get("title"),
                            "description": cat.get("description"),
                            "num_tokens": cat.get("num_tokens"),
                            "avg_price_change": cat.get("avg_price_change"),
                            "market_cap": cat.get("market_cap"),
                            "market_cap_change": cat.get("market_cap_change"),
                            "volume": cat.get("volume"),
                            "volume_change": cat.get("volume_change"),
                            "last_updated": cat.get("last_updated"),
                        }
                    )
                except Exception as e:
                    self.log(f"Failed parsing category: {e}")
                    continue

        self.log(f"Parsed {len(record)} categories.")
        return record

    def normalize_numeric_columns(self, df: DataFrame) -> DataFrame:
        """
        Converts numeric columns to float64 to ensure safe and accurate Parquet serialization.

        Args:
            df (DataFrame): The DataFrame to normalize.

        Returns:
            DataFrame: Normalized DataFrame.
        """
        numeric_cols = ["num_tokens", "avg_price_change", "market_cap", "market_cap_change", "volume", "volume_change"]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = to_numeric(df[col], errors="coerce").astype("float64")

        self.log("Numeric columns normalized to float64 for Parquet compatibility.")
        return df

    # Override of BaseExtractor.run
    def run(self, debug: bool = False) -> None:
        """
        Main execution method for the extractor.

        Steps:
        - Fetch all category data from the API.
        - Parse and validate the response.
        - Log and optionally save raw data if debug is enabled.
        - Convert to DataFrame and normalize numerical fields.
        - Append snapshot timestamp and metadata (total category count, IDs).
        - Save the data to Parquet and write snapshot info.
        """
        self.log_section("START CryptoCategoriesExtractor")

        # Step 1: Fetch raw category data
        raw_data = self.fetch_all_categories()
        parsed_records = self.parse(raw_data)

        # Step 2: Validate parsed records
        if not parsed_records:
            self.log("No categories data to save. Skipping.")
            self.log_section("END CryptoCategoriesExtractor")
            return

        # Step 3 (optional): Save raw data for debugging
        if debug:
            self.save_raw_data(parsed_records, filename="debug_crypto_categories.json")
            self.log("Debug mode: Raw parsed records saved.")

        # Step 4: Convert to DataFrame and normalize numerical columns
        df = DataFrame(parsed_records)
        df = self.normalize_numeric_columns(df)

        # Step 5: Add snapshot timestamp
        df["date_snapshot"] = self.df_snapshot_date
        self.log(f"Snapshot timestamp: {self.df_snapshot_date}")

        # Step 6: Update snapshot metadata
        self.snapshot_info["total_categories"] = len(self.category_ids)
        self.snapshot_info["category_ids"] = self.category_ids

        # Step 7: Save Parquet file and snapshot
        self.save_parquet(df, filename="crypto_categories")
        self.write_snapshot_info(self.snapshot_info)

        self.log(f"DataFrame saved with {len(df)} rows.")

        self.log_section("END CryptoCategoriesExtractor")
