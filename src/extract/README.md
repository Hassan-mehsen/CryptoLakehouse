# `extract/` Folder - CryptoLakehouse ELT Extraction Layer

This folder contains all **extraction logic and base classes** responsible for fetching data from the CoinMarketCap API (and other sources), as the first step of the Crypto ELT pipeline.

It follows a **modular and extensible design** to support multiple endpoints, retry logic, snapshot tracking, and proper separation of extractor responsibilities.

---

## Core Structure

```bash
extract/
├── base_extractor.py # Abstract base class shared by all extractors
└── extractors/       # All individual extractor implementations
└── runners/          # Local test script 
```

Each extractor class corresponds to one **API endpoint**, with snapshot handling, output storage, and error resilience built in.

---

## `BaseExtractor` Capabilities

All extractors inherit from `BaseExtractor`, which provides:

- Standardized **API request handling** with retries
- Timestamped **snapshot metadata logging**
- Consistent **file output structure** (Parquet, with UTC timestamp)
- Debug mode to persist raw responses
- Safe fallback logic and structured logging

---

## Output Convention

- Data files are saved under `data/bronze/<extractor_name>_data/`
- Filenames include the UTC snapshot timestamp, for reproducibility
- Metadata about each run (e.g., number of records, source snapshot ref) is stored in `snapshot_info.jsonl` under `metadata/extract/<extractor_name>/`

Example:  
`data/bronze/crypto_map_data/crypto_map-2025-05-15_12-00-00.parquet`
`metadata/extract/exchange_map/snapshot_info.jsonl`

---

## Design Decisions

- Most extractors perform **full extractions** (or full refreshes) and store data once per execution
- In memory batching (e.g., `group_size = 5`) is only used for high-volume endpoints like `/listings/latest`
- Snapshot tracking is designed to support **differential extraction** (e.g., `crypto_info`, `exchange_assets`)
- Chunked saving is **intentionally avoided** where unnecessary, to keep the logic clean and performant

---

##  Summary

| File / Subfolder           | Purpose                                      |
|----------------------------|----------------------------------------------|
| `base_extractor.py`        | Common tools for all extractors              |
| `extractors/`              | One Python class per API endpoint logic      |
| `runners/`                 | Local test script                            |


---

## Next Steps

- This module is ready for integration into a DAG orchestration system (Airflow, Prefect, etc.)
- Data outputs are structured for downstream **transformation** into the Silver layer of the data lake
- Future enhancements may include streaming sources or real-time API feeds

---