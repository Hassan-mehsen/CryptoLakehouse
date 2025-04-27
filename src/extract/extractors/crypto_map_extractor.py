from extract.base_extractor import BaseExtractor
from pandas import DataFrame
import json
import time


class CryptoMapExtractor(BaseExtractor):
    """
    Extracts and processes active cryptocurrency mapping data from the CoinMarketCap API.

    Detects updates based on crypto IDs compared to a previous snapshot and saves data only if changes are found.
    Implements a retry mechanism with exponential backoff for API resilience.

    Inherits from:
        BaseExtractor
    """

    def __init__(self):
        super().__init__(name="crypto_map", endpoint="/v1/cryptocurrency/map")

        self.params = {
            "listing_status": "active",
            "start": "1",
            "limit": "5000",
            "sort": "id",
            "aux": "platform,first_historical_data,last_historical_data,is_active",
        }
        self.snapshot_info = {
            "source_endpoint": "/v1/cryptocurrency/map",
            "total_active_crypto": None,
            "crypto_ids": None,
        }

        self.MAX_RETRIES = 3
        self.crypto_ids = []
        self.is_updated = False

    # Override of BaseExtractor.parse
    def parse(self, raw_data) -> DataFrame:
        """
        Parses the raw API response from /v1/cryptocurrency/map into a cleaned DataFrame.

        The function checks if the list of active crypto IDs has changed compared to the last snapshot.
        If a change is detected or if no snapshot exists, it processes and saves the new data.
        Otherwise, it skips saving to avoid redundancy.

        Args:
            raw_data (dict): API response from /v1/cryptocurrency/map.

        Returns:
            DataFrame: Cleaned crypto map information, or None if no update was detected.
        """
        cryptos_list = raw_data.get("data", [])
        self.crypto_ids = [x.get("id") for x in cryptos_list if isinstance(x, dict)]

        cleaned_crypto_map_data = []
        invalid_data = []

        last_snapshot = self.read_last_snapshot()
        # Check if this is not the first run (a snapshot was already saved)
        if last_snapshot:
            # Check if the lists are equal, maybe one or more IDs have been replaced
            if last_snapshot["total_active_crypto"] == len(self.crypto_ids):
                for id in last_snapshot["crypto_ids"]:
                    if id not in self.crypto_ids:
                        self.is_updated = True
                        break
            else:
                # If the lengths of the lists are different, then an update definitely occurred
                self.is_updated = True

        # If no snapshot exists, this is the first run, so treat it as an update
        else:
            self.is_updated = True

        # Check if there is an update, else stop the process
        if not self.is_updated:
            self.log("No changes detected in Crypto map --> Skipping save.")
            return None

        self.snapshot_info["crypto_ids"] = self.crypto_ids
        self.snapshot_info["total_active_crypto"] = len(self.crypto_ids)
        self.write_snapshot_info(self.snapshot_info)

        for x in cryptos_list:
            if isinstance(x, dict):
                # Protect the pipeline, if platform is null 
                platform_data = x.get("platform") or {}
                cleaned_crypto_map_data.append(
                    {
                        "id": x.get("id"),
                        "rank": x.get("rank"),
                        "symbol": x.get("symbol"),
                        "slug": x.get("slug"),
                        "is_active": x.get("is_active"),
                        "first_historical_data": x.get("first_historical_data"),
                        "last_historical_data": x.get("last_historical_data"),
                        "platform_id": platform_data.get("id"),
                        "platform_name": platform_data.get("name"),
                        "platform_symbol": platform_data.get("symbol"),
                        "platform_slug": platform_data.get("slug"),
                        "platform_token_address": platform_data.get("token_address"),
                    }
                )
            else:
                invalid_data.append(x)

        if invalid_data:
            self.log(f"Ignored {len(invalid_data)} malformed entries in cryptos_list.")

        return DataFrame(cleaned_crypto_map_data)

    # Override of BaseExtractor.run
    def run(self, debug: bool = False) -> None:
        """
        Executes the full extraction pipeline with retry mechanism:
        - Fetches cryptocurrency mapping data from the API
        - Retries up to MAX_RETRIES times if fetching fails
        - Detects updates based on crypto_ids
        - Parses and stores new data only if changed
        - Logs the entire process for traceability

        Args:
            debug (bool): If True, saves raw JSON response to a debug file
        """
        self.log_section("START CryptoMapExtractor")

        attempts = 0
        raw_data = None

        while attempts < self.MAX_RETRIES:
            raw_data = self.get_data(params=self.params)

            if raw_data and raw_data.get("data"):
                break  # Success, we exit the retry loop

            attempts += 1
            backoff = attempts
            self.log(f"Attempt {attempts}/{self.MAX_RETRIES} failed to fetch valid data. Retrying after {2**backoff} seconds...")
            time.sleep(2**backoff)  # Exponential backoff

        if not raw_data or not raw_data.get("data"):
            self.log("All retries failed. Skipping run.")
            self.log_section("END CryptoMapExtractor")
            return

        if debug:
            self.save_raw_data(raw_data, filename="debug_crypto_map.json")

        df_clean = self.parse(raw_data)

        if df_clean is not None:
            self.save_parquet(df_clean, filename="crypto_map")

        self.log_section("END CryptoMapExtractor")
