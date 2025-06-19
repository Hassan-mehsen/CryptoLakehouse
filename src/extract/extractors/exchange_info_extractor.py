from typing import List, Optional, Tuple
from pandas import DataFrame
from pathlib import Path
import json
import math
import time
import sys

# Resolve  path dynamically
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from extract.base_extractor import BaseExtractor


class ExchangeInfoExtractor(BaseExtractor):
    """
    Extractor class responsible for retrieving detailed metadata about each exchange
    using the /v1/exchange/info endpoint from CoinMarketCap API.

    This extractor depends on the exchange IDs already retrieved by ExchangeMapExtractor.
    It retrieves detailed fields (logo, description, URLs, etc.) and stores them in a DataFrame.
    """

    def __init__(self):
        super().__init__(name="exchange_info", endpoint="/v1/exchange/info", output_dir="exchange_info_data")
        self.params = {
            "id": None,
            "aux": "urls,logo,description,date_launched,notice,status",
        }
        self.snapshot_info = {
            "total_count": None,
            "source_endpoint": "/v1/exchange/info",
            "exchange_map_snapshot_ref": None,
        }

    def get_exchange_ids_from_snapshot(self) -> List[int]:
        """
        Reads exchange_ids from the latest snapshot of ExchangeMapExtractor.
        Helps determine which exchanges to query for detailed info.

        Returns :
        - list: List of exchange IDs previously fetched from /exchange/map
        """
        path = self.PROJECT_ROOT / "metadata/extract/exchange_map/snapshot_info.jsonl"
        try:
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in reversed(lines):
                    if line.strip():
                        snapshot = json.loads(line.strip())
                        self.log(f"Loaded exchange_ids from exchange_map snapshot: {path}")
                        self.snapshot_info["exchange_map_snapshot_ref"] = snapshot.get("snapshot_date")
                        return snapshot.get("exchange_ids", [])
        except Exception as e:
            self.log(f"Could not load exchange_map snapshot: {e}")
            return []

    def fetch_exchanges_info(self, ids: List[int]) -> dict:
        """
        Fetch detailed exchange info by querying the API in chunks of 100 IDs at a time.
        This chunking is required due to a constraint imposed by the API.

        Param:
        - ids (list): List of exchange IDs to query.
        Returns:
        - dict: Aggregated result from all chunked API responses.
        """

        chunk_size = 100
        total_chunks = math.ceil(len(ids) / chunk_size)
        raw_data = {}

        for i in range(total_chunks):
            chunk = ids[i * chunk_size : chunk_size * (i + 1)]
            self.params["id"] = ",".join(map(str, chunk))
            raw_data.update(self.get_data(params=self.params).get("data", {}))
            self.log(f"Fetching chunk {i+1}/{total_chunks} -> {len(chunk)} IDs")
            self.log("Waiting 2s to respect API rate limit")
            time.sleep(2)

        return raw_data

    # Override of BaseExtractor.parse
    def parse(self, raw_data: dict) -> Optional[Tuple[DataFrame, int]]:
        """
        Parses valid exchange info entries from raw API data.

        Iterates over each entry, extracts relevant fields, handles missing data gracefully,
        and constructs a cleaned DataFrame. Logs and skips malformed entries.

        Args:
            raw_data (dict): Raw JSON response from the CoinMarketCap /v1/exchange/info endpoint.

        Returns:
        Optional[Tuple[DataFrame, int]]:
            - A tuple containing:
                - A cleaned pandas DataFrame of valid exchange info (or None if parsing failed),
                - The number of successfully parsed entries.
            - Returns (None, 0) if parsing fails or input data is empty.
        """

        if not raw_data:
            self.log("No data returned to parse")
            return None, 0

        cleaned_exchange_info_data = []
        invalid_data = []

        for k, v in raw_data.items():
            if isinstance(v, dict):
                try:
                    # Prevent pipeline crashes when nested fields are absent
                    urls = v.get("urls") or {}
                    fiats = v.get("fiats") or []

                    cleaned_exchange_info_data.append(
                        {
                            "id": v.get("id"),
                            "name": v.get("name"),
                            "slug": v.get("slug"),
                            "description": v.get("description"),
                            "date_launched": v.get("date_launched"),
                            "notice": v.get("notice"),
                            "logo": v.get("logo"),
                            "weekly_visits": v.get("weekly_visits"),
                            "spot_volume_usd": v.get("spot_volume_usd"),
                            "maker_fee": v.get("maker_fee"),
                            "taker_fee": v.get("taker_fee"),
                            "urls": urls,
                            "fiats": ", ".join(fiats),
                        }
                    )
                except Exception as e:
                    self.log(f"Error parsing exchange info id {v.get('id')}: {str(e)}")
                    continue
            else:
                invalid_data.append((k, v))

        if invalid_data:
            self.log(
                f"Ignored {len(invalid_data)} malformed entries in exchanges_info : {[k for k, _ in invalid_data][:5]}..."
            )

        return DataFrame(cleaned_exchange_info_data), len(cleaned_exchange_info_data)

    def fetch_and_parse_with_recovery(self, ids: List[int]) -> Tuple[DataFrame, int]:
        """
        Fetches data in chunks, parses valid entries, and if incomplete,
        triggers a refresh of exchange_map and retries the fetch+parse logic.

        Returns:
        - tuple: (DataFrame, valid_count)
        """
        expected_count = len(ids)
        raw_data = self.fetch_exchanges_info(ids)
        df_clean, valid_count = self.parse(raw_data)

        if valid_count != expected_count:
            self.log(f"Only {valid_count}/{expected_count} exchanges parsed. Refreshing exchange_map...")

            from extract.extractors.exchange_map_extractor import ExchangeMapExtractor

            ExchangeMapExtractor().run(debug=True)

            self.log("Re-fetching exchange info with updated exchange_map snapshot...")
            ids = self.get_exchange_ids_from_snapshot()
            raw_data = self.fetch_exchanges_info(ids)
            df_clean, valid_count = self.parse(raw_data)

        return df_clean, valid_count

    # Override of BaseExtractor.run
    def run(self, debug: bool = False) -> None:
        """
        Main execution method for the extractor.

        Steps:
        - Load exchange IDs from the latest exchange_map snapshot.
        - Fetch and parse exchange metadata with fallback logic.
        - Optionally save parsed records for debugging.
        - Add snapshot timestamp and write snapshot metadata.
        - Save the result to a Parquet file.
        """
        self.log_section("START ExchangeInfoExtractor")

        # Step 1: Load IDs from snapshot
        ids = self.get_exchange_ids_from_snapshot()
        if not ids:
            self.log("No exchange IDs to fetch. Skipping extraction.")
            self.log_section("END ExchangeInfoExtractor")
            return

        # Step 2: Fetch and parse with fallback logic
        df_clean, valid_count = self.fetch_and_parse_with_recovery(ids)

        # Step 3: Save debug data (optional)
        if debug and df_clean is not None:
            self.save_raw_data(df_clean.to_dict(orient="records"), filename="debug_exchange_info.json")

        # Step 4: Validate and persist
        if df_clean is not None:
            df_clean["date_snapshot"] = self.df_snapshot_date
            self.log(f"Snapshot timestamp: {self.df_snapshot_date}")

            # write the snapshot
            self.snapshot_info["total_count"] = valid_count
            self.write_snapshot_info(self.snapshot_info)

            # save the df in .parquet
            self.save_parquet(df_clean, filename="exchange_info")
            self.log(f"Final DataFrame saved with {valid_count} exchanges.")
        else:
            self.log("Final DataFrame is empty after fallback recovery.")

        self.log_section("END ExchangeInfoExtractor")
