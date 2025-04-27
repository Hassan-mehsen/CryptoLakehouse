from extract.base_extractor import BaseExtractor
from pandas import DataFrame
import json
import math
import time


class ExchangeInfoExtractor(BaseExtractor):
    """
    Extractor class responsible for retrieving detailed metadata about each exchange
    using the /v1/exchange/info endpoint from CoinMarketCap API.

    This extractor depends on the exchange IDs already retrieved by ExchangeMapExtractor.
    It retrieves detailed fields (logo, description, URLs, etc.) and stores them in a DataFrame.
    """

    def __init__(self):
        super().__init__(name="exchange_info", endpoint="/v1/exchange/info")
        self.params = {
            "id": None,
            "aux": "urls,logo,description,date_launched,notice,status",
        }
        self.snapshot_info = {
            "total_count": None,
            "source_endpoint": "/v1/exchange/info",
            "exchange_map_snapshot_ref": None,
        }

    def get_exchange_ids_from_snapshot(self) -> list:
        """
        Reads exchange_ids from the latest snapshot of ExchangeMapExtractor.
        Helps determine which exchanges to query for detailed info.

        Returns :
        - list: List of exchange IDs previously fetched from /exchange/map
        """
        path = self.PROJECT_ROOT / "src/api_clients/exchange_map/snapshot_info.jsonl"
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

    def fetch_exchanges_info(self, ids: list) -> dict:
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
    def parse(self, raw_data) -> tuple[DataFrame, int]:
        """
        Parses valid entries and returns both the DataFrame and count of valid items.
        Ignores malformed entries and logs them.
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
            self.log(f"Ignored {len(invalid_data)} malformed entries in exchanges_info :  {[k for k, _ in invalid_data][:5]}...")

        return DataFrame(cleaned_exchange_info_data), len(cleaned_exchange_info_data)

    def fetch_and_parse_with_recovery(self, ids: list) -> tuple[DataFrame, int]:
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
        Main execution method:
        - Loads exchange IDs from exchange_map
        - Fetches and parses data with fallback if needed
        - Saves result as parquet + snapshot info
        """
        self.log_section("START ExchangeInfoExtractor")

        ids = self.get_exchange_ids_from_snapshot()
        df_clean, valid_count = self.fetch_and_parse_with_recovery(ids)

        if debug and df_clean is not None:
            self.save_raw_data(df_clean.to_dict(orient="records"), filename="debug_exchange_info.json")

        if df_clean is not None and not df_clean.empty:
            self.snapshot_info["total_count"] = valid_count
            self.write_snapshot_info(self.snapshot_info)
            self.save_parquet(df_clean, filename="exchange_info")
            self.log(f"Final DataFrame saved with {valid_count} exchanges.")
        else:
            self.log("Final DataFrame is empty after fallback recovery.")

        self.log_section("END ExchangeInfoExtractor")
