from extract.base_extractor import BaseExtractor
from datetime import datetime, timezone
from typing import Optional, List
from pandas import DataFrame
from time import sleep


class FearAndGreedHistoricalExtractor(BaseExtractor):
    """
    Extractor for Alternative.me's Fear and Greed Index (historical).
    Collects daily market sentiment scores to analyze crypto market psychology over time.
    """

    def __init__(self):
        super().__init__(name="fear_and_greed_historical", endpoint="/v3/fear-and-greed/historical", output_dir="fear_and_greed_data")
        self.params = {"start": 1, "limit": 500}
        self.snapshot_info = {
            "source_endpoint": self.endpoint,
        }
        self.MAX_RETRIES = 3

    def parse(self, raw_data):
        pass

    def fetch_market_sentiment(self) -> Optional[List[dict]]:
        """
        Fetches historical sentiment data with retry logic.

        Returns:
            List[dict] or None
        """

        for attempt in range(1, self.MAX_RETRIES + 1):
            response = self.get_data(params=self.params)
            data = response.get("data", [])
            if response and data:
                self.log(f"Successfully fetched {len(data)} fields on attempt {attempt}.")
                return response.get("data", [])

            self.log(f"Attempt {attempt} failed. Retrying after {2**attempt}s...")
            sleep(2**attempt)

        self.log("Failed to fetch fear and greed data after maximum retries.")
        return

    # Override of BaseExtractor.run
    def run(self, debug: bool = False) -> None:
        """
        Main execution method:
        - Fetches sentiment data
        - Converts to DataFrame
        - Adds snapshot timestamp
        - Saves result to Parquet
        """
        self.log_section("START FearAndGreedHistoricalExtractor")

        # Step 1: Fetch
        parsed_data = self.fetch_market_sentiment()
        if not parsed_data:
            self.log("No sentiment data retrieved. Skipping the process.")
            self.log_section("END FearAndGreedHistoricalExtractor")
            return

        # Step 2 (optional): Save raw data for debugging
        if debug:
            self.save_raw_data(parsed_data, filename="debug_fear_and_greed.json")

        # Step 3: Convert to DataFrame
        df = DataFrame(parsed_data)
        if df.empty:
            self.log("Parsed DataFrame is empty. Aborting process.")
            self.log_section("END FearAndGreedHistoricalExtractor")
            return

        # Step 4 : add snapshot timestamp
        df["date_snapshot"] = self.df_snapshot_date
        self.log(f"Snapshot timestamp: {self.df_snapshot_date}")

        # Step 5: Save Parquet file and snapshot info
        self.save_parquet(df=df, filename="fear_and_greed")
        self.log(f"DataFrame saved with {len(df)} rows.")

        first_ts = int(df["timestamp"].min())
        last_ts = int(df["timestamp"].max())

        first_dt = datetime.fromtimestamp(first_ts, tz=timezone.utc)
        last_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc)

        self.snapshot_info.update(
            {
                "records_fetched": len(df),
                "first_timestamp": str(first_ts),
                "last_timestamp": str(last_ts),
                "first_date_utc": first_dt.strftime("%Y-%m-%d"),
                "last_date_utc": last_dt.strftime("%Y-%m-%d"),
            }
        )
        self.write_snapshot_info(self.snapshot_info)

        self.log_section("END FearAndGreedHistoricalExtractor")
