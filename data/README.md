# `data/` Folder - CryptoLakehouse Storage Layout

This folder serves as the main **data lake** structure used by the CryptoLakehouse pipeline.  
It is organized into standard layers following the **Medallion Architecture**: Bronze and Silver.

---

## `bronze/` - Raw Ingested Snapshots

This layer stores **raw snapshot files** extracted from the CoinMarketCap API.

- Each subfolder corresponds to an API **endpoint** (e.g., `/cryptocurrency/map`, `/exchange/info`)
- Files inside are in **Parquet format** and named using a **timestamp of extraction**
- No transformation is applied; data is stored **as-is**

Each folder may contain one or more `.parquet` files named like:

Example :  
`exchange_info-2025-05-15_18-37-23.parquet`

---

## `silver/` - Cleaned & Enriched Tables

This layer stores **transformed and enriched** data that is ready for analysis or loading into a Data Warehouse.

- Organized by **business domain** (`crypto/`, `exchange/`, `fear_and_greed/`, `global_metrics/`)
- Each subfolder contains data for a **fact** or **dimension** or sometimes **both**, depending on the structure of the domain in the logical DWH   schema.
- Files are stored in **Delta Lake format** to support ACID transactions, schema evolution, and time travel


Each folder contains the **Delta Lake files** (e.g., `_delta_log/`, `part-*.parquet`) associated with the corresponding table.

---

## `gold/` - Final Exports & DWH Backups

This layer stores **final deliverables and backups** derived from the PostgreSQL Data Warehouse.

- `dumps/`: SQL dumps of the full DWH (e.g., `pg_dump` results)
- `snapshots/`: CSV exports of selected analytical tables (e.g., for external sharing or offline inspection)
- Files in this layer are **not versioned** in Git (ignored via `.gitignore`)
- Acts as a local **backup and export zone** for delivery, archiving, or BI integration outside PostgreSQL

All Backup logic is maintained in `src/db/backup_postgres.py` 

---

## Notes

- Only selected Delta tables with frequent and lightweight appends (e.g., fact_global_market) are regularly optimized using **file compaction**
 `(OPTIMIZE)`, to reduce small files and improve read performance
- Old and unused files are cleaned up using **Delta VACUUM**
- Metadata tracking is handled separately in the `metadata/` directory
- The **Silver layer** remains the **source of truth for transformations**
- The **Gold layer** serves as the **final export zone** (non-versioned)

---

##  Summary

| Layer  | Purpose                   | Format        | Usage                                     |
|--------|---------------------------|---------------|-------------------------------------------|
| Bronze | Raw API snapshots         | Parquet       | Backup, reproducibility                   |
| Silver | Cleaned & enriched tables | Delta Lake    | Analytics, BI, DWH integration            |
| Gold   | Final exports & backups   | SQL, CSV, etc | External sharing, PostgreSQL snapshotting |

