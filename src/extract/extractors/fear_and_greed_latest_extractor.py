from typing import Optional
from pandas import DataFrame
from pathlib import Path
from time import sleep
import sys

# Resolve  path dynamically
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from extract.base_extractor import BaseExtractor


class FearAndGreedLatestExtractor(BaseExtractor):
    """
    Extractor for Alternative.me's Fear and Greed Index (latest).
    This endpoint provides real-time market sentiment data, updated every few minutes.
    """

    def __init__(self):
        super().__init__(
            name="fear_and_greed_latest", endpoint="/v3/fear-and-greed/latest", output_dir="latest_fear_and_greed_data"
        )
        self.snapshot_info = {
            "source_endpoint": self.endpoint,
        }
        self.MAX_RETRIES = 3

    def parse(self, raw_data):
        pass

    def fetch_latest_market_sentiment(self) -> Optional[dict]:
        """
        Fetches the latest fear and greed sentiment (single data point).
        Implements retry with exponential backoff.

        Returns:
            dict or None: Latest sentiment record or None on failure.
        """
        for attempt in range(1, self.MAX_RETRIES + 1):
            response = self.get_data()
            data = response.get("data", {})
            if response and data:
                self.log(f"Successfully fetched latest sentiment on attempt {attempt}.")
                return data

            self.log(f"Attempt {attempt} failed. Retrying after {2**attempt}s...")
            sleep(2**attempt)

        self.log("Failed to fetch fear and greed latest data after maximum retries.")
        return

    def run(self, debug: bool = False) -> None:
        """
        Main execution method:
        - Fetches the latest sentiment
        - Converts to DataFrame
        - Adds snapshot timestamp
        - Saves to Parquet and writes snapshot metadata
        """
        self.log_section("START FearAndGreedLatestExtractor")

        # Step 1: Fetch
        parsed_data = self.fetch_latest_market_sentiment()
        if not parsed_data:
            self.log("No latest sentiment data retrieved. Skipping the process.")
            self.log_section("END FearAndGreedLatestExtractor")
            return

        # Step 2 (optional): Save raw data for debugging
        if debug:
            self.save_raw_data(parsed_data, filename="debug_fear_and_greed_latest.json")

        # Step 3: Convert to DataFrame
        df = DataFrame([parsed_data])
        if df.empty:
            self.log("Parsed DataFrame is empty. Aborting process.")
            self.log_section("END FearAndGreedLatestExtractor")
            return

        # Step 4: Add snapshot timestamp
        df["date_snapshot"] = self.df_snapshot_date
        self.log(f"Snapshot timestamp: {self.df_snapshot_date}")

        # Step 5: Save Parquet + snapshot info
        self.save_parquet(df=df, filename="fear_and_greed_latest")
        self.log(f"DataFrame saved with {len(df)} rows.")

        self.snapshot_info.update({"records_fetched": len(df)})
        self.write_snapshot_info(self.snapshot_info)

        self.log_section("END FearAndGreedLatestExtractor")
