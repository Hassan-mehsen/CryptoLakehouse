# `transform/` Folder Overview

This directory contains all PySpark-based logic that transforms raw CoinMarketCap snapshots into structured, analytics-ready Delta Lake tables.
It powers the Silver Layer and ensures compatibility with data warehousing, BI, and ML use cases.


---

## Responsibilities

* Apply strict schemas and normalize column types
* Clean raw data (nulls, types, deduplication, formatting)
* Enrich records with business KPIs (e.g. ratios, deltas, metrics)
* Produce structured fact and dimension tables
* Ensure compatibility with DWH & BI tools
* Support scalable, modular transformations with Spark
* Log every step for traceability

---

## Core Modules

### `ExchangeTransformer`

Processes all data related to exchanges and their activities.

* **Input sources:**

  * `exchange_map_data`
  * `exchange_info_data`
  * `exchange_assets_data`

* **Output tables:**

  * `dim_exchange_id`
  * `dim_exchange_info`
  * `dim_exchange_map`
  * `fact_exchange_assets`

---

### `CryptoTransformer`

Handles all transformations related to cryptocurrencies and their metadata.

* **Input sources:**

  * `crypto_map_data`
  * `crypto_info_data`
  * `crypto_listings_latest_data`
  * `crypto_categories_data`
  * `crypto_category_data`

* **Output tables:**

  * `dim_crypto_identity`
  * `dim_crypto_map`
  * `dim_crypto_info`
  * `dim_crypto_categories`
  * `fact_crypto_market`
  * `fact_crypto_category`
  * `dim_crypto_category_link`

---

### `GlobalMetricsTransformer`

Processes global market indicators from CoinMarketCap.

* **Input source:** 

  * `global_metrics_data`

* **Output table:** 

  * `fact_global_market`

---

### `FearAndGreedTransformer`

Transforms the Fear & Greed index (historical and live) into a market sentiment fact table.

* **Input sources:**

  * `fear_and_greed_data` (historical)
  * `latest_fear_and_greed_data` (live)

* **Output table:**

  * `fact_market_sentiment`

---

## Common Design

* Based on `BaseTransformer` class (shared logic)
* Logging and metadata tracking at every step
* Support for broadcast joins (dim -> fact)
* Robust null-safe transformations
* OPTIMIZE/VACUUM post-write options for Delta Lake

---

## Output Layer

All transformations write to:

```
/data/silver/<domain>/<table_name>
```

Format: **Delta Lake** (ACID, versioned)

These outputs are the final point of truth and are ready for:

* PostgreSQL DWH loading
* BI tools (Metabase, Power BI, Tableau)
* Data Science & ML pipelines requiring clean and structured features


## Possible Next Steps

The transformation layer is complete and functional. Suggested next steps for extending or leveraging this work include:

- Automate orchestration: Integrate transformation runners with Airflow to schedule daily/weekly jobs.

- Load Silver to DWH: Develop a loader module to move data from /data/silver/ into a PostgreSQL Data Warehouse.

- Connect BI tools: Expose Delta outputs to tools like Metabase for dashboarding and visual analytics.

- Machine Learning features: Use Silver tables (especially fact_market_sentiment, fact_crypto_market, fact_global_market) to engineer features for prediction models.

- Data quality checks: Add automated validations and assertions before writing Delta (row counts, null ratios, schema enforcement).

- Testing framework: Integrate unit tests and CI pipelines to ensure transformer reliability with each code change.
