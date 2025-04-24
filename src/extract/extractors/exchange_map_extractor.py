from extract.base_extractor import BaseExtractor
import sys
import json
from pandas import DataFrame


class ExchangeMapExtractor(BaseExtractor):
    """
    Extractor class for fetching and tracking the list of available cryptocurrency exchanges
    using the /v1/exchange/map endpoint from the CoinMarketCap API.

    This extractor is used to build a dimension table of exchange platforms (dim_exchange),
    and to detect additions or removals between API calls by comparing exchange IDs.

    It also manages snapshot tracking for versioning and update control.
    """

    def __init__(self):
        super().__init__(name="exchange_map", endpoint="/v1/exchange/map")
        self.exchanges_id = []
        self.is_updated = False
        self.snapshot_info = {
            "exchange_ids": None,
            "total_count": None,
            "source_endpoint": "/v1/exchange/map",
        }

    # Override of BaseExtractor.parse
    def parse(self, raw_data) -> DataFrame:
        """
        Parses the raw API response into a clean DataFrame.

        If the list of exchange IDs has not changed compared to the last snapshot,
        it skips further processing to avoid redundant storage.

        Param:
        - raw_data (dict): The full API response from /v1/exchange/map

        Returns:
        - DataFrame: Cleaned exchange info, or None if no update was detected
        """
        exchanges_list = raw_data.get("data", [])
        self.exchanges_id = sorted([exchange["id"] for exchange in exchanges_list])

        cleaned_exchange_map_data = []
        invalid_data = []

        last_snapshot = self.read_last_snapshot()

        if last_snapshot["total_count"] == len(self.exchanges_id):
            for id in self.exchanges_id:
                if id not in last_snapshot["exchange_ids"]:
                    self.is_updated = True
                    break
        else:
            self.is_updated = True

        if not self.is_updated:
            self.log("No changes detected in exchange map. Skipping save.")
            return None

        self.snapshot_info["exchange_ids"] = self.exchanges_id
        self.snapshot_info["total_count"] = len(self.exchanges_id)
        self.write_snapshot_info(self.snapshot_info)

        for x in exchanges_list:
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
                invalid_data.append(x)

        if invalid_data:
            self.log(f"Ignored {len(invalid_data)} malformed entries in exchanges_list.")

        return DataFrame(cleaned_exchange_map_data)

    # Override of BaseExtractor.run
    def run(self, debug: bool = False) -> None:
        """
        Executes the full extraction pipeline:
        - Fetches exchange data from the API
        - Detects updates based on exchange_ids
        - Parses and stores new data only if changed
        - Logs the entire process for traceability

        Param:
        - debug (bool): If True, saves raw JSON response to debug file
        """
        self.log_section("START ExchangeMapExtractor")

        parameters = {"start": "1", "limit": "5000"}
        raw_data = self.get_data(params=parameters)

        if not raw_data.get("data"):
            self.log("Empty data received from API --> Skipping run.")
            return

        if debug:
            self.save_raw_data(raw_data, filename="debug_exchange_map.json")

        df_clean = self.parse(raw_data)
        if df_clean is not None:
            self.save_parquet(df_clean, filename="exchange_map")

        self.log_section("END ExchangeMapExtractor")
