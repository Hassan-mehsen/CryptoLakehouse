from datetime import date, timedelta, datetime
from typing import Optional, List, Generator
from pandas import DataFrame
from pathlib import Path
import json
import time
import math
import sys

# Resolve  path dynamically
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from extract.base_extractor import BaseExtractor


class CryptoInfoExtractor(BaseExtractor):
    """
    CryptoInfoExtractor:

    Extracts stable and complementary cryptocurrency metadata (description, logos, URLs, tags)
    from CoinMarketCap's `/v2/cryptocurrency/info` endpoint.

    This data is intended to enrich dashboards and BI reports by providing contextual information
    on each listed cryptocurrency (branding, platform, social links, etc.).

    ---

    Key Features:
    - **Differential extraction**: compares current `crypto_map` snapshot with existing info to fetch only missing entries.
    - **Initial full initialization**: occurs only once, fetching all active crypto IDs (~5000 entries).
    - **Subsequent refreshes**: fetch only newly listed cryptos (typically 0-200 entries).
    - **Efficient retry strategy** with exponential backoff on API failures.
    - **Safe parsing** and flattening of nested metadata structures (including URLs and tags).
    - **Snapshot tracking** for reproducibility, incremental logic, and auditability.

    ---

    Why we do not implement chunked Parquet saving:
    - The **full initialization** is rare (only once) and fits comfortably in memory.
    - **Daily refreshes** are lightweight and involve a small number of new cryptos.
    - Holding all parsed data in memory improves simplicity and readability of the pipeline.
    - If the volume or frequency increases, the class structure allows easy refactoring to stream and save in batches.
    """

    def __init__(self):
        super().__init__(name="crypto_info", endpoint="/v2/cryptocurrency/info", output_dir="crypto_info_data")

        self.params = {
            "id": None,
            "skip_invalid": "true",
            "aux": "urls,logo,description,tags,platform,date_added,notice,status",
        }
        self.snapshot_info = {
            "source_endpoint": "/v1/cryptocurrency/map",
            "date_of_next_full_scan": None,
            "crypto_map_snapshot_ref": None,
            "total_info_crypto": None,
            "ids_available_info": None,
        }
        self.MAX_RETRIES = 3
        self.available_crypto = []

    def find_crypto_ids_to_fetch(self) -> List[int]:
        """
        Determines which crypto IDs to fetch based on the availability of crypto_info snapshot.
        - If crypto_info snapshot does not exist, returns all active crypto_ids (full initialization).
        - If crypto_info snapshot exists, returns only missing crypto_ids (refresh).

        Returns:
            list: List of crypto IDs to fetch.
        """
        crypto_map_path = self.PROJECT_ROOT / "metadata/extract/crypto_map/snapshot_info.jsonl"

        # Load last crypto_map snapshot
        try:
            with open(crypto_map_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in reversed(lines):
                    if line.strip():
                        crypto_map_snapshot = json.loads(line.strip())
                        crypto_ids = set(crypto_map_snapshot.get("crypto_ids", []))
                        self.snapshot_info["crypto_map_snapshot_ref"] = crypto_map_snapshot.get("snapshot_date")
                        break
        except Exception as e:
            self.log(f"Error reading crypto_map snapshot: {e}")
            return []

        # Load crypto_info snapshot if available;
        # if not found or empty, treat as initialization (full crypto_info extraction).
        # if today is the date of the full scan refetch all crypto info to be uptodate
        today_str = date.today()
        crypto_info_snapshot = self.read_last_snapshot()

        last_scan_str = crypto_info_snapshot.get("date_of_next_full_scan")
        last_scan_date = datetime.strptime(last_scan_str, "%Y-%m-%d").date() if last_scan_str else None

        if crypto_info_snapshot and crypto_info_snapshot.get("ids_available_info", []) and today_str != last_scan_date:
            available_crypto_ids = set(crypto_info_snapshot.get("ids_available_info", []))
            self.snapshot_info["date_of_next_full_scan"] = crypto_info_snapshot.get("date_of_next_full_scan")
        elif not crypto_info_snapshot or today_str >= last_scan_date:
            available_crypto_ids = set()
            self.snapshot_info["date_of_next_full_scan"] = (date.today() + timedelta(days=30)).isoformat()

        # Compute missing IDs
        ids_to_fetch = crypto_ids - available_crypto_ids

        # update snapshote metadata
        self.snapshot_info["total_info_crypto"] = len(list(available_crypto_ids)) + len(ids_to_fetch)
        self.snapshot_info["ids_available_info"] = list(available_crypto_ids) + list(ids_to_fetch)
        self.write_snapshot_info(self.snapshot_info)

        return list(ids_to_fetch)

    def fetch_crypto_info(self, ids: List[int]) -> Generator[dict, None, None]:
        """
        Fetches cryptocurrency info from the API in chunks of 100 IDs with retry and exponential backoff.

        Args:
            ids (List[int]): List of crypto IDs to fetch.

        Yields:
            dict: Partial 'data' dictionary for each chunk of fetched cryptocurrencies.
        """
        chunk_size = 100
        total_chunks = math.ceil(len(ids) / chunk_size)
        success = False

        for i in range(total_chunks):
            chunk = ids[i * chunk_size : chunk_size * (i + 1)]
            self.params["id"] = ",".join(map(str, chunk))

            for attempt in range(1, self.MAX_RETRIES + 1):
                raw_response = self.get_data(params=self.params)
                raw_data = raw_response.get("data", {}) if raw_response else {}

                if raw_data:
                    self.log(f"Fetching chunk {i+1}/{total_chunks} -> {len(chunk)} IDs (attempt {attempt})")
                    self.log("Waiting 2s to respect API rate limit")
                    yield raw_data
                    success = True
                    time.sleep(2)
                    break  # stop retrying for this chunk
                else:
                    self.log(f"Attempt {attempt} failed for chunk {i+1}. Retrying in {2**attempt}s...")
                    time.sleep(2**attempt)

            if not success:
                self.log(f"Failed to fetch chunk {i+1}/{total_chunks} after {self.MAX_RETRIES} attempts.")

    def safe_first(self, urls: dict, key: str) -> Optional[str]:
        """
        Safely extracts the first element from a URL field if it exists.

        Args:
            urls (dict): Dictionary containing URL lists from the API response.
            key (str): Specific URL field to extract (e.g., "website", "twitter", "explorer").

        Returns:
        str or None: The first URL found for the given key, or None if the key is missing or empty.
        """
        return urls.get(key, [None])[0] if urls.get(key) else None

    # Override of BaseExtractor.parse
    def parse(self, chunk_number: int, raw_data: dict) -> List[dict]:
        """
        Parses a single API response from /v2/cryptocurrency/info into a list of cleaned crypto metadata records.

        Args:
            chunk_number (int): The current chunk number being parsed (for logging purposes).
            raw_data (dict): Partial 'data' dictionary returned by the API ( already extracted by fetch_crypto_info() ).

        Returns:
            list[dict]: List of flattened crypto information records ready for DataFrame creation.
        """

        if not raw_data:
            self.log(f"No cryptocurrency info found for chunk {chunk_number}.")
            return []

        result = []

        # Loop over each cryptocurrency info in the chunk
        for crypto_id, info in raw_data.items():
            if isinstance(info, dict):
                try:
                    urls = info.get("urls", {})
                    record = {
                        "id": int(crypto_id),
                        "name": info.get("name"),
                        "symbol": info.get("symbol"),
                        "slug": info.get("slug"),
                        "logo": info.get("logo"),
                        "description": info.get("description"),
                        "date_added": info.get("date_added"),
                        "date_launched": info.get("date_launched"),
                        "website": self.safe_first(urls, "website"),
                        "technical_doc": self.safe_first(urls, "technical_doc"),
                        "twitter": self.safe_first(urls, "twitter"),
                        "reddit": self.safe_first(urls, "reddit"),
                        "explorer": self.safe_first(urls, "explorer"),
                        "source_code": self.safe_first(urls, "source_code"),
                        "category": info.get("category"),
                        "tags": ",".join(info.get("tags", [])) if info.get("tags") else None,
                    }

                    result.append(record)
                    self.available_crypto.append(int(crypto_id))
                except Exception as e:
                    self.log(f"Error parsing crypto_id {crypto_id} in chunk {chunk_number}: {str(e)}")
                    continue

        self.log(f"Parsed {len(result)} cryptocurrencies for chunk {chunk_number}.")

        return result

    # Override of BaseExtractor.run
    def run(self, debug: bool = False) -> None:
        """
        Main execution method for the extractor.

        Steps:
        - Identify crypto IDs to fetch using snapshot comparison.
        - Fetch cryptocurrency info data in chunks.
        - Parse and aggregate all results.
        - Log and optionally save raw parsed records if debug mode is enabled.
        - Create DataFrame and append snapshot timestamp.
        - Save the data to Parquet and update snapshot metadata.

        Ensures:
        - Traceable data lineage.
        - Clean logging for each chunk and snapshot.
        - Consistency with the overall ELT framework.
        """
        self.log_section("START CryptoInfoExtractor")

        # Step 1: Identify IDs to fetch
        crypto_ids = self.find_crypto_ids_to_fetch()
        if not crypto_ids:
            self.log("No crypto IDs to fetch. Skipping extraction.")
            self.log_section("END CryptoInfoExtractor")
            return

        # Step 2: Fetch and parse data chunk by chunk
        parsed_records = []
        chunk_number = 1
        for raw_data in self.fetch_crypto_info(ids=crypto_ids):
            parsed_chunk = self.parse(chunk_number, raw_data)
            parsed_records.extend(parsed_chunk)
            chunk_number += 1

        # Step 3: Validate parsed data
        if not parsed_records:
            self.log("No valid cryptocurrency info records found.")
            self.log_section("END CryptoInfoExtractor")
            return

        # Step 4 (optional): Save parsed raw data for debugging
        if debug and parsed_records:
            self.save_raw_data(parsed_records, filename="debug_crypto_info.json")
            self.log(f"Debug mode: Raw parsed records saved.")

        # Step 5: Create DataFrame and add snapshot timestamp
        df = DataFrame(parsed_records)
        df["date_snapshot"] = self.df_snapshot_date
        self.log(f"Snapshot timestamp: {self.df_snapshot_date}")

        # Step 6: Save as Parquet
        self.save_parquet(df, filename="crypto_info")
        self.log(f"DataFrame saved with {len(df)} rows.")

        self.log_section("END CryptoInfoExtractor")
