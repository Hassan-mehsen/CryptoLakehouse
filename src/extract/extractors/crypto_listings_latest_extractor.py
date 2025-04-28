from extract.base_extractor import BaseExtractor
from datetime import datetime, timezone
from typing import List, Generator
import pandas as pd
import json
import time
import math


class CryptoListingsLatestExtractor(BaseExtractor):
    """
    CryptoListingsLatestExtractor:

    Extracts **live market data** (price, volume, market cap, supply...) for **active cryptocurrencies**
    using CoinMarketCap's `/v1/cryptocurrency/listings/latest` endpoint.

    ---
    Key Features:
    - **Dynamic data** updated every 60 seconds — no dependency on snapshots.
    - **Pagination** with `start` and `limit=200`, optimized for API credit usage (1 credit per 200 cryptos).
    - **Data enrichment** with extended fields (`aux`).
    - **Fault tolerance** with retry and backoff on temporary API errors.
    - **Memory optimization** by processing data chunk by chunk.
    """

    def __init__(self):
        super().__init__(name="crypto_listings_latest", endpoint="/v1/cryptocurrency/listings/latest")
        self.params = {
            "start": None,
            "limit": None,
            "aux": (
                "num_market_pairs,cmc_rank,date_added,tags,platform,max_supply,"
                "circulating_supply,total_supply,market_cap_by_total_supply,"
                "volume_24h_reported,volume_7d,volume_7d_reported,volume_30d,"
                "volume_30d_reported,is_market_cap_included_in_calc"
            ),
        }
        self.MAX_RETRIES = 3

    def fetch_crypto_listings(self) -> Generator[dict, None, None]:
        """
        Fetches live crypto listings in chunks of 200 from the API.
        Implements retry mechanism with MAX_RETRIES in case of temporary API failures.

        Yields:
            dict: Partial data chunk from the API.
        """

        chunk_size = 200
        total_cryptos = 5000  # Based on the API
        iteration_number = total_cryptos // chunk_size

        for i in range(iteration_number):
            start = i * chunk_size + 1
            attempt = 1

            while attempt <= self.MAX_RETRIES:
                self.params["start"] = start
                self.params["limit"] = chunk_size

                raw_data = self.get_data(params=self.params)

                if raw_data and raw_data.get("data"):
                    self.log(f"Chunk {i+1}/{iteration_number} fetched successfully (start={start}).")
                    yield raw_data
                    self.log("Waiting 2s to respect API rate limit")
                    time.sleep(2)
                    break
                else:
                    self.log(f"Attempt {attempt} failed for chunk starting at {start}. Retrying...")
                    attempt += 1
                    time.sleep(2**attempt)  # Exponential backoff

            if attempt > self.MAX_RETRIES:
                self.log(f"Failed to fetch chunk starting at {start} after {self.MAX_RETRIES} attempts.")

    # Override of BaseExtractor.parse
    def parse(self, raw_data_chunk: dict) -> List[dict]:
        """
        Parses a chunk of cryptocurrency listing data.

        Args:
            raw_data_chunk (dict): Chunk of raw data returned by the API.

        Returns:
            List[dict]: List of flattened crypto records.
        """

        crypto_data = []

        for crypto in raw_data_chunk.get("data", []):
            if isinstance(crypto, dict):
                quote_usd = crypto.get("quote", {}).get("USD", {})
                record = {
                    "id": crypto.get("id"),
                    "name": crypto.get("name"),
                    "symbol": crypto.get("symbol"),
                    "slug": crypto.get("slug"),
                    "cmc_rank": crypto.get("cmc_rank"),
                    "num_market_pairs": crypto.get("num_market_pairs"),
                    "circulating_supply": crypto.get("circulating_supply"),
                    "total_supply": crypto.get("total_supply"),
                    "max_supply": crypto.get("max_supply"),
                    "infinite_supply": crypto.get("infinite_supply"),
                    "self_reported_circulating_supply": crypto.get("self_reported_circulating_supply"),
                    "self_reported_market_cap": crypto.get("self_reported_market_cap"),
                    "self_reported_tags": (
                        ",".join(crypto.get("self_reported_tags", [])) if crypto.get("self_reported_tags") else None
                    ),
                    "date_added": crypto.get("date_added"),
                    "tags": ",".join(crypto.get("tags", [])) if crypto.get("tags") else None,
                    "platform_id": crypto.get("platform", {}).get("id") if crypto.get("platform") else None,
                    # Quote (USD)
                    "price_usd": quote_usd.get("price"),
                    "volume_24h_usd": quote_usd.get("volume_24h"),
                    "volume_change_24h": quote_usd.get("volume_change_24h"),
                    "percent_change_1h_usd": quote_usd.get("percent_change_1h"),
                    "percent_change_24h_usd": quote_usd.get("percent_change_24h"),
                    "percent_change_7d_usd": quote_usd.get("percent_change_7d"),
                    "market_cap_usd": quote_usd.get("market_cap"),
                    "market_cap_dominance_usd": quote_usd.get("market_cap_dominance"),
                    "fully_diluted_market_cap_usd": quote_usd.get("fully_diluted_market_cap"),
                    "last_updated_usd": quote_usd.get("last_updated"),
                }
                crypto_data.append(record)

        return crypto_data

    def normalize_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize all selected numeric columns to Pandas float64 type for safe Parquet saving.

        Args:
            df (DataFrame): The DataFrame to normalize.

        Returns:
            DataFrame: The DataFrame with all specified columns casted to float64.
        """

        numeric_cols = [
            "num_market_pairs",
            "circulating_supply",
            "total_supply",
            "max_supply",
            "price_usd",
            "volume_24h_usd",
            "volume_change_24h",
            "percent_change_1h_usd",
            "percent_change_24h_usd",
            "percent_change_7d_usd",
            "market_cap_usd",
            "market_cap_dominance_usd",
            "fully_diluted_market_cap_usd",
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        self.log("Numeric columns normalized to float64 for Parquet compatibility.")
        return df

    # Override of BaseExtractor.run
    def run(self, debug: bool = False) -> None:
        """
        Main execution method:
        - Fetches all active crypto listings chunk by chunk
        - Parses and aggregates all data
        - Adds a date_snapshot for tracking
        - Saves the results to a Parquet file
        - Optionally saves raw parsed data in debug mode
        """
        self.log_section("START CryptoListingsExtractor")

        parsed_records = []
        chunk_number = 1

        for raw_data_chunk in self.fetch_crypto_listings():
            parsed_chunk = self.parse(raw_data_chunk)
            parsed_records.extend(parsed_chunk)
            self.log(f"Chunk {chunk_number} parsed with {len(parsed_chunk)} cryptocurrencies.")
            chunk_number += 1

        if not parsed_records:
            self.log("No cryptocurrency data parsed. Skipping save.")
            self.log_section("END CryptoListingsExtractor")
            return

        # Convert the data to a DataFrame and add a snapshote date column
        df = pd.DataFrame(parsed_records)

        # Convert all important columns to float64 before saving df to .parquet
        df = self.normalize_numeric_columns(df)

        # Adding timestamp column
        snapshot_date_utc = datetime.now(timezone.utc)
        df["date_snapshot"] = snapshot_date_utc
        self.log(f"Snapshot timestamp: {snapshot_date_utc}")

        if debug:
            self.save_raw_data(parsed_records, filename="debug_crypto_listings.json")
            self.log(f"Debug mode: Raw parsed records saved.")

        self.save_parquet(df, filename="crypto_listings")
        self.log(f"DataFrame saved with {len(df)} rows.")

        self.log_section("END CryptoListingsExtractor")
