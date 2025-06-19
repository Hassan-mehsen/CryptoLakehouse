from pandas import DataFrame, to_numeric
from typing import Optional
from pathlib import Path
from time import sleep
import sys

# Resolve  path dynamically
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from extract.base_extractor import BaseExtractor


class GlobalMetricsQuotesLatestExtractor(BaseExtractor):
    """
    Extractor for CoinMarketCap's `/v1/global-metrics/quotes/latest` endpoint.

    It retrieves aggregated market indicators such as total cryptocurrencies,
    dominance ratios, DeFi metrics, stablecoins, and derivatives data.

    The extractor supports retry logic, debug mode, snapshot tracking, and Parquet export.
    """

    def __init__(self):
        super().__init__(
            name="global_metrics_quotes_latest",
            endpoint="/v1/global-metrics/quotes/latest",
            output_dir="global_metrics_data",
        )

        self.snapshot_info = {
            "source_endpoint": self.endpoint,
        }
        self.MAX_RETRIES = 3

    def fetch_global_metrics(self) -> Optional[dict]:
        """
        Fetches the latest global market metrics from the CoinMarketCap API.
        Implements retry logic with exponential backoff.

        Returns:
            dict or None: Parsed "data" section from the API response, or None if failed.
        """

        for attempt in range(1, self.MAX_RETRIES + 1):

            response = self.get_data()
            data = response.get("data", {})
            if response and data:
                self.log(f"Successfully fetched {len(data)} feild on attempt {attempt}.")
                return response.get("data", {})

            self.log(f"Attempt {attempt} failed. Retrying after {2**attempt}s...")
            sleep(2**attempt)

        self.log("Failed to fetch global market metrics after maximum retries.")
        return

    # Override of BaseExtractor.parse
    def parse(self, raw_data: dict) -> Optional[dict]:
        """
        Parses raw API response into a flat, structured record.

        Args:
            raw_data (dict): Raw dictionary data from the API.

        Returns:
            dict or None: Cleaned and flattened data or None if parsing fails.
        """

        if not raw_data:
            self.log(f"No Global Metrics data found.")
            return

        try:
            # Prevent pipeline crashes when nested fields are absent
            if raw_data.get("quote", {}) and raw_data.get("quote", {}).get("USD", {}):
                quote_USD = raw_data.get("quote", {}).get("USD", {})
            else:
                quote_USD = {}

            tracked_yearly_number = (
                raw_data.get("tracked_yearly_number", {}) if raw_data.get("tracked_yearly_number", {}) else None
            )

            record = {
                # High-level market activity
                "active_cryptocurrencies": raw_data.get("active_cryptocurrencies"),
                "total_cryptocurrencies": raw_data.get("total_cryptocurrencies"),
                "active_market_pairs": raw_data.get("active_market_pairs"),
                "active_exchanges": raw_data.get("active_exchanges"),
                "total_exchanges": raw_data.get("total_exchanges"),
                # Dominance & evolution
                "eth_dominance": raw_data.get("eth_dominance"),
                "btc_dominance": raw_data.get("btc_dominance"),
                "eth_dominance_yesterday": raw_data.get("eth_dominance_yesterday"),
                "btc_dominance_yesterday": raw_data.get("btc_dominance_yesterday"),
                "eth_dominance_24h_percentage_change": raw_data.get("eth_dominance_24h_percentage_change"),
                "btc_dominance_24h_percentage_change": raw_data.get("btc_dominance_24h_percentage_change"),
                # DeFi
                "defi_volume_24h": raw_data.get("defi_volume_24h"),
                "defi_volume_24h_reported": raw_data.get("defi_volume_24h_reported"),
                "defi_market_cap": raw_data.get("defi_market_cap"),
                "defi_24h_percentage_change": raw_data.get("defi_24h_percentage_change"),
                # Stablecoins
                "stablecoin_volume_24h": raw_data.get("stablecoin_volume_24h"),
                "stablecoin_volume_24h_reported": raw_data.get("stablecoin_volume_24h_reported"),
                "stablecoin_market_cap": raw_data.get("stablecoin_market_cap"),
                "stablecoin_24h_percentage_change": raw_data.get("stablecoin_24h_percentage_change"),
                # Derivatives
                "derivatives_volume_24h": raw_data.get("derivatives_volume_24h"),
                "derivatives_volume_24h_reported": raw_data.get("derivatives_volume_24h_reported"),
                "derivatives_24h_percentage_change": raw_data.get("derivatives_24h_percentage_change"),
                # Market growth indicators
                "total_crypto_dex_currencies": raw_data.get("total_crypto_dex_currencies"),
                "today_incremental_crypto_number": raw_data.get("today_incremental_crypto_number"),
                "past_24h_incremental_crypto_number": raw_data.get("past_24h_incremental_crypto_number"),
                "past_7d_incremental_crypto_number": raw_data.get("past_7d_incremental_crypto_number"),
                "past_30d_incremental_crypto_number": raw_data.get("past_30d_incremental_crypto_number"),
                "today_change_percent": raw_data.get("today_change_percent"),
                # Growth extremes (from tracked_yearly_number)
                "tracked_maxIncrementalNumber": tracked_yearly_number.get("maxIncrementalNumber"),
                "tracked_maxIncrementalDate": tracked_yearly_number.get("maxIncrementalDate"),
                "tracked_minIncrementalNumber": tracked_yearly_number.get("minIncrementalNumber"),
                "tracked_minIncrementalDate": tracked_yearly_number.get("minIncrementalDate"),
                # Quote in USD
                "quote_total_market_cap": quote_USD.get("total_market_cap"),
                "quote_total_volume_24h": quote_USD.get("total_volume_24h"),
                "quote_total_volume_24h_reported": quote_USD.get("total_volume_24h_reported"),
                "quote_altcoin_volume_24h": quote_USD.get("altcoin_volume_24h"),
                "quote_altcoin_volume_24h_reported": quote_USD.get("altcoin_volume_24h_reported"),
                "quote_altcoin_market_cap": quote_USD.get("altcoin_market_cap"),
                "quote_total_market_cap_yesterday": quote_USD.get("total_market_cap_yesterday"),
                "quote_total_volume_24h_yesterday": quote_USD.get("total_volume_24h_yesterday"),
                "quote_total_market_cap_yesterday_percentage_change": quote_USD.get(
                    "total_market_cap_yesterday_percentage_change"
                ),
                "quote_total_volume_24h_yesterday_percentage_change": quote_USD.get(
                    "total_volume_24h_yesterday_percentage_change"
                ),
                # Timestamp
                "last_updated": raw_data.get("last_updated"),
            }

        except Exception as e:
            self.log(f"Error parsing global market metrics data: {e}")
            return

        return record

    def normalize_numeric_columns(self, df: DataFrame) -> DataFrame:
        """
        Cast selected numeric columns to float64 for compatibility with Parquet format.

        Args:
            df (DataFrame): Raw dataframe.

        Returns:
            DataFrame: Normalized dataframe.
        """

        numeric_cols = [
            # Dominance & evolution
            "eth_dominance",
            "btc_dominance",
            "eth_dominance_yesterday",
            "btc_dominance_yesterday",
            "eth_dominance_24h_percentage_change",
            "btc_dominance_24h_percentage_change",
            # Market growth indicators
            "today_change_percent",
            # DeFi
            "defi_market_cap",
            "defi_volume_24h",
            "defi_volume_24h_reported",
            "defi_24h_change",
            # Stablecoins
            "stablecoin_market_cap",
            "stablecoin_volume_24h",
            "stablecoin_volume_24h_reported",
            "stablecoin_24h_change",
            # Derivatives
            "derivatives_volume_24h",
            "derivatives_volume_24h_reported",
            "derivatives_24h_change",
            # Quote in USD
            "quote_total_market_cap",
            "quote_total_volume_24h",
            "quote_total_volume_24h_reported",
            "quote_altcoin_volume_24h",
            "quote_altcoin_volume_24h_reported",
            "quote_altcoin_market_cap",
            "quote_total_market_cap_yesterday",
            "quote_total_volume_24h_yesterday",
            "quote_total_market_cap_yesterday_percentage_change",
            "quote_total_volume_24h_yesterday_percentage_change",
            "quote_last_updated",
        ]
        try:
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = to_numeric(df[col], errors="coerce").astype("float64")

            self.log("Numeric columns normalized to float64 for Parquet compatibility.")
            return df

        except Exception as e:
            self.log(f"Error when casting the columns : {e}")

    # Override of BaseExtractor.run
    def run(self, debug: bool = False) -> None:
        """
        Main execution method for the extractor.

        Steps:
        - Fetch data from the API with retry logic.
        - Parse and validate the response.
        - Log and optionally save raw data if debug is enabled.
        - Convert to DataFrame and normalize numerical fields.
        - Append snapshot timestamp and completeness metadata.
        - Save the data to Parquet and write snapshot info.
        """
        self.log_section("START GlobalMetricsQuotesLatestExtractor")

        # Step 1: Fetch raw data
        raw_data = self.fetch_global_metrics()
        if not raw_data:
            self.log("No valid global metrics data found. Skipping the process.")
            self.log_section("END GlobalMetricsQuotesLatestExtractor")
            return

        # Step 2: Parse and validate the data
        parsed_data = self.parse(raw_data)
        if not parsed_data:
            self.log("Parsing failed. Skipping the process.")
            self.log_section("END GlobalMetricsQuotesLatestExtractor")
            return

        # Step 3 (optional): Save raw data for debugging
        if debug:
            self.save_raw_data(raw_data, filename="debug_global_metrics.json")

        # Step 4: Convert to DataFrame
        df = DataFrame([parsed_data])
        if df.empty:
            self.log("Parsed dataframe is empty. Skipping normalization and save")
            self.log_section("END GlobalMetricsQuotesLatestExtractor")
            return

        # Step 5: Normalize numeric columns and add snapshot timestamp
        normalized_df = self.normalize_numeric_columns(df=df)
        normalized_df["date_snapshot"] = self.df_snapshot_date
        self.log(f"Snapshot timestamp: {self.df_snapshot_date}")

        # Step 6: Compute null/non-null field counts and update snapshot metadata
        non_null_fields = sum(v is not None for v in parsed_data.values())
        null_fields = len(parsed_data) - non_null_fields
        self.snapshot_info.update(
            {
                "non_null_fields": non_null_fields,
                "null_fields": null_fields,
            }
        )
        if null_fields > 0:
            self.log(f"Warning: {null_fields} fields are null in the parsed data.")

        # Step 7: Save Parquet file and snapshot info
        self.save_parquet(normalized_df, filename="global_metrics")
        self.log(f"DataFrame saved with {len(df)} rows.")

        self.write_snapshot_info(self.snapshot_info)

        self.log_section("END GlobalMetricsQuotesLatestExtractor")
