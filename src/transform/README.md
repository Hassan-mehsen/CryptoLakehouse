# `transform/` Folder Overview

This directory contains all Spark-based transformation logic for the CryptoLakhouse ELT pipeline.  
It is organized by domain and role to ensure modularity, clarity, and scalability.

---

## Folder Structure & Purpose

### `base/`
- Contains shared base classes and utilities.
- `base_transformer.py`: Defines `BaseTransformer`, which handles SparkSession injection, logging, and common write/metadata methods.

### `transformers/`
- Contains all transformation classes, each one dedicated to a business domain.
- Structure: **one domain = one file = one class**.
- Each class has public methods to build `dim_` and `fact_` tables from domain-specific raw data.

### `orchestrators/`
- Contains high-level pipeline coordinators used to manage multiple transformers together.
- `transform_orchestrator.py`: Defines `TransformationPipelineRunner`, the main orchestration class used to drive full or partial transformation pipelines (e.g., `run_daily_tasks()`).
- Designed for integration with Airflow, DAGs, or scheduled workflows.

### `runners/`
- Contains ad hoc scripts for local or isolated runs (e.g., `run_crypto_only.py`).
- Useful for debugging and manual validation of transformer logic.

### `__init__.py`
- Present in all folders to make them importable as Python packages.

---

## Notes
- All `SparkContext`/`SparkSession` creation is handled centrally in the orchestrator (`TransformationPipelineRunner`) — **not inside individual transformers**.
- Each transformer is strictly focused on:
  - reading domain-specific snapshots
  - cleaning/enriching data
  - persisting structured Delta tables
- Metadata tracking, optimization (`OPTIMIZE`), and cleanup (`VACUUM`) are handled automatically via the shared base logic.

