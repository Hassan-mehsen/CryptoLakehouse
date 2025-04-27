from extract.base_extractor import BaseExtractor
from typing import Optional, List, Generator
from pandas import DataFrame
import json
import time
import math


class CryptoInfoExtractor(BaseExtractor):
    """
    CryptoInfoExtractor:

    Extracts stable and complementary cryptocurrency metadata (description, logos, URLs, tags)
    from CoinMarketCap's /v2/cryptocurrency/info endpoint.

    This data is intended to enrich dashboards and BI reports.

    Handles initial full extraction and monthly differential refresh based on available information snapshots.

    Ensures data integrity, minimizes API usage, and maintains an up-to-date reference dataset.
    """

    def __init__(self):
        super().__init__(name="crypto_info", endpoint="/v2/cryptocurrency/info")

        self.params = {"id": None, "skip_invalid": "true", "aux": "urls,logo,description,tags,platform,date_added,notice,status"}

        self.snapshot_info = {
            "source_endpoint": "/v1/cryptocurrency/map",
            "crypto_map_snapshot_ref": None,
            "total_info_crypto": None,
            "ids_available_info": None,
        }

        self.available_crypto = []

    def find_crypto_ids_to_fetch(self) -> List[int]:
        """
        Determines which crypto IDs to fetch based on the availability of crypto_info snapshot.
        - If crypto_info snapshot does not exist, returns all active crypto_ids (full initialization).
        - If crypto_info snapshot exists, returns only missing crypto_ids (refresh).

        Returns:
            list: List of crypto IDs to fetch.
        """
        crypto_map_path = self.PROJECT_ROOT / "src/api_clients/crypto_map/snapshot_info.jsonl"
        crypto_info_path = self.PROJECT_ROOT / "src/api_clients/crypto_info/snapshot_info.jsonl"

        crypto_ids = set()
        available_crypto_ids = set()

        # Load crypto_map snapshot
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
            print(f"Error reading crypto_map snapshot: {e}")
            return []

        # Load crypto_info snapshot if available;
        # if not found or empty, treat as initialization (full crypto_info extraction).
        crypto_info_snapshot = self.read_last_snapshot()
        if crypto_info_snapshot and crypto_info_snapshot.get("ids_available_info", []):
            available_crypto_ids = set(crypto_info_snapshot.get("ids_available_info", []))
        else:
            available_crypto_ids = set()

        # Compute missing IDs
        ids_to_fetch = crypto_ids - available_crypto_ids

        return list(ids_to_fetch)

    def fetch_crypto_info(self, ids: List[int]) -> Generator[dict, None, None]:
        """
        Fetches cryptocurrency info from the API in chunks of 100 IDs to optimize API usage.

        Args:
            ids (List[int]): List of crypto IDs to fetch.

        Yields:
            dict: Partial 'data' dictionary for each chunk of fetched cryptocurrencies.
        """

        chunk_size = 100
        total_chunks = math.ceil(len(ids) / chunk_size)
        raw_data = {}

        for i in range(total_chunks):
            chunk = ids[i * chunk_size : chunk_size * (i + 1)]
            self.params["id"] = ",".join(map(str, chunk))

            raw_response = self.get_data(params=self.params)
            raw_data = raw_response.get("data", {}) if raw_response else {}

            if raw_data:
                self.log(f"Fetching chunk {i+1}/{total_chunks} -> {len(chunk)} IDs")
                self.log("Waiting 2s to respect API rate limit")
                yield raw_data
            else:
                self.log(f"No data returned for chunk {i+1}/{total_chunks}")

            time.sleep(2)

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
        Executes the full extraction pipeline:
        - Fetches cryptocurrency info data.
        - Parses and aggregates the results.
        - Writes snapshots and saves the DataFrame as Parquet.
        - Supports debug mode to save raw parsed records.
        """
        self.log_section("START CryptoInfoExtractor")

        crypto_ids = self.find_crypto_ids_to_fetch()
        parsed_records = []
        chunk_number = 1

        for raw_data in self.fetch_crypto_info(ids=crypto_ids):
            parsed_chunk = self.parse(chunk_number, raw_data)
            parsed_records.extend(parsed_chunk)
            chunk_number += 1

        if debug and parsed_records:
            self.save_raw_data(parsed_records, filename="debug_crypto_info.json")
            self.log(f"Debug mode: Raw parsed records saved.")

        if parsed_records:
            df = DataFrame(parsed_records)
            self.snapshot_info["total_info_crypto"] = len(self.available_crypto)
            self.snapshot_info["ids_available_info"] = self.available_crypto
            self.write_snapshot_info(self.snapshot_info)
            self.save_parquet(df, filename="crypto_info")
            self.log(f"DataFrame saved with {len(df)} rows.")
        else:
            self.log("No valid cryptocurrency info records found.")

        self.log_section("END CryptoInfoExtractor")
