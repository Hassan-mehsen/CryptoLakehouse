from extract.base_extractor import BaseExtractor
from pandas import DataFrame
import json
import time
from datetime import date, timedelta


class ExchangeAssetsExtractor(BaseExtractor):
    def __init__(self):
        super().__init__(name="exchange_assets", endpoint="/v1/exchange/assets")

        self.MAX_RETRIES = 3
        self.params = {"id": None}
        self.snapshot_info = {
            "source_endpoint": "/v1/exchange/assets",
            "exchange_map_snapshot_ref": None,
            "date_of_next_full_scan": None,
            "total_actif_exchanges": None,
            "actif_exchanges": None,
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

    def fetch_assets_per_exchange_with_recovery(self, ids: list):
        """
        Generator that yields valid asset data per exchange_id by querying the /v1/exchange/assets endpoint.

        Implements fault-tolerant logic:
        - On the first full scan (or once per month), attempts all exchange_ids.
        - On regular runs, reuses the filtered list of previously validated active exchanges.
        - Retries failed calls up to 3 times (technical errors only), with linear backoff.

        Updates the active exchange list by removing those who no longer return data.

        NOTE:
        An "active exchange" asset is defined as:
        - An exchange with status 'active' from /exchange/map
        - AND that holds > $100,000 USD in wallet value (per CMC filtering rules)

        Parameters:
        - ids (list): All exchange_ids fetched from /exchange/map

        Yields:
        - dict: Valid response from the API (with asset data)
        """

        failed_ids = []
        active_exchanges = []
        inactive_exchanges = []
        last_snapshot = self.read_last_snapshot()

        today_str = date.today().isoformat()
        is_full_scan = not last_snapshot or today_str == last_snapshot.get("date_of_next_full_scan")

        # Use all IDs on full scan, or reuse active subset from last run
        target_ids = ids if is_full_scan else last_snapshot.get("actif_exchanges", [])
        self.log(f"{'Full scan' if is_full_scan else 'Partial scan'} on {len(target_ids)} exchanges")

        for ex_id in target_ids:
            for attempt in range(1, self.MAX_RETRIES + 1):
                self.params["id"] = ex_id
                self.log(f"Fetching exchange {ex_id} (attempt {attempt}/{self.MAX_RETRIES})")

                response = self.get_data(params=self.params)
                status = response.get("status", {})

                # Business case: valid but empty response -> do not retry
                if status.get("error_code") == 0:
                    if response.get("data"):
                        if is_full_scan:
                            active_exchanges.append(ex_id)
                        yield (ex_id, response)
                    else:
                        self.log(f"Exchange {ex_id} has no visible assets (OK status).")
                    break  # Success or business case -> exit the retry loop

                else:
                    # Technical/API case -> retry possible
                    self.log(f"Error from API for exchange {ex_id} (code {status.get('error_code')})")

                    if attempt < self.MAX_RETRIES:
                        backoff = attempt  # Linear backoff: 1s, 2s, 3s, take rehealing time
                        self.log(f"Retrying in {backoff}s...")
                        time.sleep(backoff)
                    else:
                        self.log(f"Failed to fetch exchange {ex_id} after {self.MAX_RETRIES} attempts.")
                        failed_ids.append(ex_id)

            # Waiting 2s to respect API rate limit
            time.sleep(2)

        # Update the list of valid exchanges
        if is_full_scan:
            self.snapshot_info["total_actif_exchanges"] = len(active_exchanges)
            self.snapshot_info["actif_exchanges"] = active_exchanges
            self.snapshot_info["date_of_next_full_scan"] = (date.today() + timedelta(days=30)).isoformat()
        else:
            # Cleanup if active exchanges no longer return anything
            self.snapshot_info["actif_exchanges"] = [
                ex for ex in target_ids if ex not in failed_ids and ex not in inactive_exchanges
            ]
            self.snapshot_info["total_actif_exchanges"] = len(self.snapshot_info["actif_exchanges"])
            self.snapshot_info["date_of_next_full_scan"] = last_snapshot.get("date_of_next_full_scan")

        if failed_ids:
            self.log(f"{len(failed_ids)} exchanges failed permanently: {failed_ids[:5]}...")

    # Override of BaseExtractor.parse
    def parse(self, exchange_id, raw_data):
        """
        Parse a single API response into a list of flat asset records.

        Param:
        - exchange_id (int): ID of the exchange (explicitly passed, not returned by API)
        - raw_data (dict): Raw API response data
        Returns:
        - list[dict]: List of flattened asset records ready for DataFrame creation
        """

        if not raw_data.get("data"):
            self.log(f"No asset data found for exchange_id {exchange_id}.")
            return []

        result = []

        # Loop over each asset in the response data
        for item in raw_data["data"]:
            try:
                record = {
                    "exchange_id": exchange_id,
                    "wallet_address": item.get("wallet_address"),
                    "balance": item.get("balance"),
                    "platform_crypto_id": item.get("platform", {}).get("crypto_id"),
                    "platform_symbol": item.get("platform", {}).get("symbol"),
                    "platform_name": item.get("platform", {}).get("name"),
                    "currency_crypto_id": item.get("currency", {}).get("crypto_id"),
                    "currency_symbol": item.get("currency", {}).get("symbol"),
                    "currency_name": item.get("currency", {}).get("name"),
                    "currency_price_usd": item.get("currency", {}).get("price_usd"),
                }

                result.append(record)

            except Exception as e:
                self.log(f"Failed parsing asset for exchange_id {exchange_id}: {e}")
                continue

        self.log(f"Parsed {len(result)} assets for exchange_id {exchange_id}.")

        return result

    # Override of BaseExtractor.run
    def run(self, debug=False):
        self.log_section("START ExchangeAssetsExtractor")

        ids = self.get_exchange_ids_from_snapshot()
        parsed_records = []

        for exchange_id, raw_data in self.fetch_assets_per_exchange_with_recovery(ids):
            parsed_chunk = self.parse(exchange_id, raw_data)
            parsed_records.extend(parsed_chunk)

        if debug and parsed_records:
            self.save_raw_data(parsed_records, filename="debug_exchange_assets.json")
            self.log(f"Debug mode: Raw parsed records saved.")

        if parsed_records:
            df = DataFrame(parsed_records)
            self.write_snapshot_info(self.snapshot_info)
            self.save_parquet(df, filename="exchange_assets")
            self.log(f"DataFrame saved with {len(df)} rows.")
        else:
            self.log("No valid asset records found.")

        self.log_section("END ExchangeAssetsExtractor")
