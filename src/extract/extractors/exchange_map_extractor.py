from extract.base_extractor import BaseExtractor
import sys
import json
from pandas import DataFrame


class ExchangeMapExtractor(BaseExtractor):
    def __init__(self):
        super().__init__(name="exchange_map", endpoint="/v1/exchange/map")
        self.exchanges_id = []
        self.is_updated = False
        self.snapshot_info = {
            "exchange_ids": None,
            "total_count": None,
            "source_endpoint": "/v1/exchange/map"
        }

    # Override of BaseExtractor.parse
    def parse(self, raw_data):
        exchanges_list = raw_data.get("data", [])
        self.exchanges_id = sorted([exchange["id"]
                                   for exchange in exchanges_list])

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

        cleaned_exchange_map_data = [
            {
                "id": x["id"],
                "name": x["name"],
                "slug": x["slug"],
                "is_active": x["is_active"]
            }
            for x in exchanges_list
        ]

        return DataFrame(cleaned_exchange_map_data)

    # Override of BaseExtractor.run
    def run(self, debug: bool = False):
        self.log(style="\n" + "=" * 50 + "\n")
        self.log(style="START ExchangeMapExtractor".center(50))
        self.log(style="\n" + "=" * 50 + "\n")

        parameters = {'start': '1', 'limit': '5000'}
        raw_data = self.get_data(params=parameters)

        if not raw_data.get("data"):
            self.log("Empty data received from API --> Skipping run.")
            return

        if debug:
            self.save_raw_data(raw_data, filename="debug_exchange_map.json")

        df_clean = self.parse(raw_data)
        if df_clean is not None:
            self.save_parquet(df_clean, filename="exchange_map")

        self.log(style="\n" + "=" * 50 + "\n")
        self.log(style="END ExchangeMapExtractor".center(50))
        self.log(style="\n" + "=" * 50 + "\n")
