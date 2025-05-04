# 📁 `transform/` Folder Overview

This directory contains all Spark-based transformation logic for the ETL pipeline.
It is organized by domain and role to ensure modularity, clarity, and scalability.

## 📂 Folder Structure & Purpose

### `base/`
- Contains shared base classes and utilities.
- `base_transformer.py`: Defines `BaseTransformer`, which handles SparkSession injection, logging, and common methods.

### `transformers/`
- Contains all transformation classes, each one dedicated to a business domain.
- Follows the structure: **one domain = one file = one class**.
- Each class has a `run()` method, with specific sub-methods for each logical transformation (e.g., building `crypto_dim`, `crypto_data_fact`).

### `runners/`
- Contains scripts to manually test or run specific transformers in isolation.
- Useful during development and debugging.

### `main.py`
- The entrypoint used in production (e.g., called via Airflow or spark-submit).
- It initializes the SparkSession, instantiates transformer classes, and calls their `run()` methods.
- Ends by properly stopping the Spark session.

### `__init__.py`
- Present in all folders to ensure they are recognized as Python packages.

## ✅ Notes
- All SparkSession creation and lifecycle are handled in `main.py`, **not** inside individual transformers.
- Each transformer is focused only on its domain logic: **clean code, testable, and maintainable**.

