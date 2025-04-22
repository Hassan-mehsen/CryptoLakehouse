from abc import ABC, abstractmethod
from datetime import datetime, date
from pathlib import Path
import requests
import json
import os


class BaseExtractor(ABC):
    """
    Abstract base class for all extractors.
    Handles API connection, logging, snapshot tracking, and output file management.

    Parameters:
    - name (str): Name of the extractor (used in logging and folder structure)
    - endpoint (str): Specific API endpoint to call (e.g., /v1/exchange/map)
    - base_url (str): Base URL of the API (default: CoinMarketCap)
    - output_dir (str): Path where output .parquet files will be saved

    Attributes:  
    - log_path (Path): Path to the main extract log file
    - snapshot_info_path (Path): Path to the snapshot history file (in JSONL format)
    - snapshot_date (str): Current snapshot date (YYYY-MM-DD)
    - api_key (str): API key loaded from environment variables
    - session (requests.Session): Reusable session with headers configured
    """

    def __init__(self, name: str, endpoint: str, base_url: str = "https://pro-api.coinmarketcap.com", output_dir: str = "data/bronze"):
        # --------------------------------------------------------------------
        #                         Attributes
        # --------------------------------------------------------------------
        self.name = name
        self.endpoint = endpoint
        self.base_url = base_url
        self.snapshot_date = date.today().isoformat()

        self.api_key = os.getenv("CMC_API_KEY")
        self.session = requests.Session()
        self.session.headers.update({
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": self.api_key
        })

        # Dynamically resolves the project root path, ensuring portability regardless of where the script is executed from
        self.PROJECT_ROOT = Path(__file__).resolve().parents[2]
        self.output_dir = self.PROJECT_ROOT / output_dir
        self.log_path = self.PROJECT_ROOT / "logs" / "extract.log"
        self.snapshot_info_path = self.PROJECT_ROOT / f"src/api_clients/{self.name.lower()}/snapshot_info.jsonl"

        # Create directory for snapshot_info file if it doesn't exist
        self.snapshot_info_path.parent.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------------------
    #                           Methods
    # --------------------------------------------------------------------

    def log(self, message: str="", style :str=None):
        """Logs a message with timestamp and extractor name."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_message = f"[{timestamp}] [EXTRACT] [{self.name.upper()}] {message}"
        with open(self.log_path, "a") as f:
            if style :
                f.write(style)
            else :
                f.write(full_message + "\n")

    def get_data(self, params: dict = None) -> dict:
        """
        Makes a GET request to the configured endpoint with optional parameters.
        Returns:
        - dict: Parsed JSON response, or empty dict on error.
        """
        url = self.base_url + self.endpoint
        try:
            self.log(f"Sending request to {url} with params: {params}")
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            self.log(f"Response received with status {response.status_code}")
            return response.json()

        except requests.RequestException as exception:
            self.log(f"API request failed: {exception}")
            return {}


    def read_last_snapshot(self) -> dict:
        """
        Reads and returns the last non-empty line (as a dict) from the snapshot JSONL file.

        Returns:
        - dict: Last snapshot entry, or empty dict if file not found or empty
        """
        try:
            with open(self.snapshot_info_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in reversed(lines):
                    if line.strip():
                        snapshot = json.loads(line.strip())
                        self.log(
                            f"Last snapshot loaded from {self.snapshot_info_path}")
                        return snapshot

        except FileNotFoundError:
            self.log(f"Snapshot file not found at {self.snapshot_info_path}")
        except json.JSONDecodeError as e:
            self.log(f"Failed to decode snapshot line: {e}")
        return {}

    def write_snapshot_info(self, snapshot_data : dict):
        """
        Appends a new snapshot entry to the JSONL file (1 JSON object per line).

        Param:
        - snapshot_data (dict): Data to log (e.g., list of IDs fetched, metadata)
        """
        snapshot_entry = {
            "snapshot_date": self.snapshot_date,
            **snapshot_data
        }
        try:
            self.log(f"Writing snapshot entry to {self.snapshot_info_path}")
            with open(self.snapshot_info_path, "a", encoding="utf-8") as f:
                json.dump(snapshot_entry, f)
                f.write("\n")
            self.log("Snapshot entry written successfully.")

        except Exception as e:
            self.log(f"Failed to write snapshot info: {e}")

    def save_parquet(self, df, filename: str):
        """
        Saves a DataFrame to a timestamped .parquet file.

        Param:
        - df (DataFrame): Data to save
        - filename (str): Base name of the file (no extension)
        """
        filename_timestamped = f"{filename}-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.parquet"
        filepath = self.output_dir / filename_timestamped
        try:
            self.log(f"Saving DataFrame to {filepath}")
            df.to_parquet(filepath, index=False)
            self.log(f"Data saved to {filepath}")

        except Exception as e:
            self.log(f"Failed to save parquet file: {e}")


    @abstractmethod
    def parse(self, raw_data):
        """Must be implemented by child class: transforms raw JSON into a clean DataFrame."""
        pass

    @abstractmethod
    def run(self):
        """Must be implemented by child class: controls full extraction logic."""
        pass


    def save_raw_data(self, data: dict, filename: str = "raw_snapshot.json"):
            """
            Saves the raw API response to a JSON file (overwrite mode). 

            Param:
            - data (dict): Raw JSON data returned from the API
            - filename (str): Name of the output file inside the tmp/ directory

            NOTE : 
            This method is intended for local development and debugging only.
            It allows you to:
            - Inspect the raw structure of the API response
            - Replay parsing logic without repeating the API call
            - Analyze or troubleshoot failed runs offline

            Should NOT be used in Airflow DAGs:
            - DAGs should be stateless and reproducible
            - Temporary files are not guaranteed to persist between DAG retries
            """
            tmp_path = self.PROJECT_ROOT / "tmp" / filename
            try:
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                self.log(f"Raw data saved to {tmp_path}")
            except Exception as e:
                self.log(f"Failed to save raw data: {e}")