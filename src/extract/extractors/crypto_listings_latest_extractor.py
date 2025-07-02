from pandas import DataFrame, to_numeric
from typing import List, Generator
from pathlib import Path
import time
import sys

# Resolve  path dynamically
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from extract.base_extractor import BaseExtractor


class CryptoListingsLatestExtractor(BaseExtractor):
    """
    CryptoListingsLatestExtractor:

    Extracts **live market data** (price, volume, market cap, supply...) for **active cryptocurrencies**
    using CoinMarketCap's `/v1/cryptocurrency/listings/latest` endpoint.

    This extractor is designed to handle high-frequency, high-volume market data in a memory-efficient
    and resilient way. It breaks down the data fetch into chunks, processes and normalizes each group,
    and saves them incrementally in Parquet format.

    ---

    Key Features:
    - **Dynamic data** updated every 60 seconds, no dependency on historical snapshots.
    - **Pagination** with `start` and `limit=200`, optimized for API credit usage (1 credit per 200 cryptos).
    - **Data enrichment** with extended fields (`aux`).
    - **Fault tolerance** with retry and exponential backoff on temporary API errors.
    - **Memory optimization**: groups multiple chunks together (e.g. 5) and saves each group
    as a separate `.parquet` file, ideal for processing large volumes without overloading memory.
    - **Final aggregation** is avoided to improve scalability and ensure partial results are preserved
    even if the process fails mid-run.

    """

    def __init__(self):
        super().__init__(
            name="crypto_listings_latest",
            endpoint="/v1/cryptocurrency/listings/latest",
            output_dir="crypto_listings_latest_data",
        )
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
        self.snapshot_info = {"total_listed_cryptos": None, "source_endpoint": self.endpoint}
        self.MAX_RETRIES = 3

    def fetch_crypto_listings(self) -> Generator[dict, None, None]:
        """
        Fetches live crypto listings in chunks of 200 from the API.
        Implements retry mechanism with MAX_RETRIES in case of temporary API failures.

        Yields:
            dict: Partial data chunk from the API.
        """

        chunk_size = 200
        total_cryptos = 5000
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
                    self.log(f"Attempt {attempt} failed for chunk starting at {start}. Retrying after {2**attempt}s...")
                    attempt += 1
                    time.sleep(2**attempt)  # Exponential backoff

            if attempt > self.MAX_RETRIES:
                self.log(f"Failed to fetch chunk starting at {start} after {self.MAX_RETRIES} attempts.")

    # Override of BaseExtractor.parse
    def parse(self, raw_data_chunk: dict, chunk_number: int) -> List[dict]:
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
                try:
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
                        "tags": (",".join(crypto.get("tags", [])) if crypto.get("tags") else None),
                        "platform_id": (crypto.get("platform", {}).get("id") if crypto.get("platform") else None),
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
                except Exception as e:
                    self.log(f"Failed parsing listings for chunk {chunk_number}: {e}")
                    continue

        self.log(f"Parsed {len(crypto_data)} cryptocurrencies for the chunk {chunk_number}.")
        return crypto_data

    def normalize_numeric_columns(self, df: DataFrame) -> DataFrame:
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
                df[col] = to_numeric(df[col], errors="coerce").astype("float64")

        self.log("Numeric columns normalized to float64 for Parquet compatibility.")
        return df

    def save_group(self, df: DataFrame, group_index: int, debug: bool):
        """
        Saves a grouped DataFrame of crypto listings to a Parquet file with metadata.

        - Normalizes numeric columns for compatibility.
        - Adds tracking columns: snapshot timestamp and batch index.
        - Optionally saves the data in raw JSON format if debug mode is enabled.

        Args:
            df (DataFrame): The group of parsed crypto records to save.
            group_index (int): The current batch index used for naming and traceability.
            debug (bool): If True, saves the data as a raw JSON file for inspection.
        """

        df = self.normalize_numeric_columns(df)
        df["date_snapshot"] = self.df_snapshot_date
        df["batch_index"] = group_index
        self.log(f"Snapshot timestamp: {self.df_snapshot_date}")
        self.save_parquet(df, filename=f"crypto_listings_batch{group_index}")
        self.log(f"Batch {group_index} saved with {len(df)} rows.")
        if debug:
            self.save_raw_data(df.to_dict(orient="records"), filename="debug_crypto_listings.json")

    # Override of BaseExtractor.run
    def run(self, debug: bool = False) -> None:
        """
        Main execution method for the extractor.

        Steps:
        - Fetch all active cryptocurrency listings in chunks.
        - Parse and group the results incrementally.
        - Save each group as a separate Parquet file to reduce memory usage.
        - Append snapshot metadata and optionally save debug JSON.
        """
        self.log_section("START CryptoListingsExtractor")

        grouped_records = []
        group_index = 1
        group_size = 5  # number of chunks per group before saving
        chunk_number = 1
        total_crypto = 0

        # Step 1: Fetch and parse chunk by chunk
        for raw_data_chunk in self.fetch_crypto_listings():
            parsed_chunk = self.parse(raw_data_chunk, chunk_number)

            if parsed_chunk:
                grouped_records.extend(parsed_chunk)
                total_crypto += len(parsed_chunk)

            if chunk_number % group_size == 0 and grouped_records:
                # Step 2: Save each group as a separate Parquet
                df = DataFrame(grouped_records)
                self.save_group(df, group_index, debug)

                grouped_records.clear()
                group_index += 1

            chunk_number += 1

        # Step 3: Save remaining records not yet written
        if grouped_records:
            df = DataFrame(grouped_records)
            self.save_group(df, group_index, debug)

        # Step 4: Update and write snapshot metadata
        self.snapshot_info["total_listed_cryptos"] = total_crypto
        self.write_snapshot_info(self.snapshot_info)

        self.log_section("END CryptoListingsExtractor")
