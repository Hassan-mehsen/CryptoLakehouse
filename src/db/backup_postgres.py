"""
-> backup_postgres.py

Performs a full PostgreSQL SQL dump of the Data Warehouse and saves it to a versioned location.

This script is meant to be executed manually or integrated into a DAG to ensure periodic full backups.

Backup Target:
    - data/gold/dumps/full_backup_<YYYY-MM-DD>.sql

Environment Requirements:
    - pg_dump must be available in the environment PATH
    - DB credentials must be present in .env file

Usage:
    python backup_postgres.py
"""

import os
import subprocess
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base project directory
BASE_DIR = Path(__file__).resolve().parents[2]
DUMP_PATH = BASE_DIR / "data" / "gold" / "dumps"
DUMP_PATH.mkdir(parents=True, exist_ok=True)

# Dynamic filename with timestamp
timestamp = datetime.now().strftime("%Y-%m-%d")
filename = f"full_backup_{timestamp}.sql"
backup_file = DUMP_PATH / filename

# DB config from .env or system env
db_url = os.getenv("DUMP_URL")

def run_backup():
    cmd = [
        "pg_dump",
        f"--dbname={db_url}",
        "--format=plain",
        "--no-owner",
        "--no-privileges",
        "-f", str(backup_file)
    ]
    try:
        print(f"\033[94m[INFO]\033[0m Starting backup to \033[1m{backup_file.name}\033[0m")
        subprocess.run(cmd, check=True)
        print(f"\033[92m[SUCCESS]\033[0m Backup completed.")
    except subprocess.CalledProcessError as e:
        print(f"\033[91m[ERROR]\033[0m Backup failed: {e}")

if __name__ == "__main__":
    run_backup()
