from extract.base_extractor import BaseExtractor
from pandas import DataFrame
from typing import Optional
import time


class ExchangeMapExtractor(BaseExtractor):
    """
    Extractor class for fetching and tracking the list of active cryptocurrency exchanges
    from the /v1/exchange/map endpoint of the CoinMarketCap API.

    This extractor builds the dimension table (dim_exchange) and detects additions or removals
    between API calls by comparing exchange IDs using snapshot tracking.

    Inherits from:
        BaseExtractor
    """

    def __init__(self):
        super().__init__(name="exchange_map", endpoint="/v1/exchange/map", output_dir="exchange_map_data")
        self.exchanges_id = []
        self.is_updated = False
        self.snapshot_info = {
            "exchange_ids": None,
            "total_count": None,
            "source_endpoint": "/v1/exchange/map",
        }
        self.params = {"start": "1", "limit": "5000"}
        self.MAX_RETRIES = 3  # retry in case of API failures

    # Override of BaseExtractor.parse
    def parse(self, raw_data: dict) -> Optional[DataFrame]:
        """
        Parses the raw API response from /v1/exchange/map into a cleaned DataFrame.

        Checks if the list of exchange IDs has changed compared to the last snapshot.
        Only processes and saves new data if a change is detected.

        Args:
            raw_data (dict): API response from /v1/exchange/map.

        Returns:
            DataFrame: Cleaned exchange map information, or None if no update was detected.
        """
        exchanges_list = raw_data.get("data", [])
        self.exchanges_id = sorted([exchange["id"] for exchange in exchanges_list])

        cleaned_exchange_map_data = []
        invalid_data = []

        last_snapshot = self.read_last_snapshot()
        # Check if this is not the first run (a snapshot was already saved)
        if last_snapshot:
            # Check if the lists are equal, maybe one or more IDs have been replaced
            if last_snapshot["total_count"] == len(self.exchanges_id):
                for id in last_snapshot["exchange_ids"]:
                    if id not in self.exchanges_id:
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
            self.log("No changes detected in exchange map --> Skipping save.")
            return None

        self.snapshot_info["exchange_ids"] = self.exchanges_id
        self.snapshot_info["total_count"] = len(self.exchanges_id)

        for x in exchanges_list:
            try:
                if isinstance(x, dict):
                    cleaned_exchange_map_data.append(
                        {
                            "id": x.get("id"),
                            "name": x.get("name"),
                            "slug": x.get("slug"),
                            "is_active": x.get("is_active"),
                        }
                    )
                else:
                    raise ValueError("Entry is not a dictionary")

            except Exception as e:
                self.log(f"Ignored malformed entry: {e}")
                invalid_data.append(x)

        if invalid_data:
            self.log(f"Ignored {len(invalid_data)} malformed entries in exchanges_list.")

        return DataFrame(cleaned_exchange_map_data)

    # Override of BaseExtractor.run
    def run(self, debug: bool = False) -> None:
        """
        Executes the full extraction pipeline with retry mechanism:
        - Fetches exchange mapping data from the API
        - Retries up to MAX_RETRIES times if fetching fails
        - Parses and stores new data only if changed
        - Logs the entire process for traceability

        Args:
            debug (bool): If True, saves raw JSON response to a debug file
        """
        self.log_section("START ExchangeMapExtractor")

        attempts = 0
        raw_data = None

        while attempts < self.MAX_RETRIES:
            raw_data = self.get_data(params=self.params)

            if raw_data and raw_data.get("data"):
                break  # Success

            attempts += 1
            self.log(f"Attempt {attempts}/{self.MAX_RETRIES} failed to fetch valid data. Retrying after {2**attempts} seconds...")
            time.sleep(2**attempts)

        if not raw_data or not raw_data.get("data"):
            self.log("All retries failed. Skipping run.")
            self.log_section("END ExchangeMapExtractor")
            return

        if debug:
            self.save_raw_data(raw_data, filename="debug_exchange_map.json")

        df_clean = self.parse(raw_data)

        if df_clean is not None:
            # Adding timestamp column to the df for better tracking
            df_clean["date_snapshot"] = self.df_snapshot_date
            self.log(f"Snapshot timestamp: {self.df_snapshot_date}")

            # write the snapshot
            self.write_snapshot_info(self.snapshot_info)

            # save the df in .parquet
            self.save_parquet(df_clean, filename="exchange_map")

        self.log_section("END ExchangeMapExtractor")
