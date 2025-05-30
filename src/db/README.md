# Database Layer – PostgreSQL Models & Utilities

This module contains all the logic related to the **PostgreSQL data warehouse schema** and associated utilities.

It provides:
- ORM model declarations using SQLAlchemy (used by Alembic migrations and validation scripts)
- Manual or automated **full SQL dumps** of the data warehouse for backup or versioning

---

## 📁 Files Overview

### `models.py`
Defines the full data warehouse schema using **SQLAlchemy**.

It is used by:
- Alembic for migration tracking and schema evolution
- Internal consistency checks and documentation

### `backup_postgres.py`
Utility script to generate a **full SQL dump** of the entire data warehouse using `pg_dump`.

- Output file: `data/gold/dumps/full_backup.sql`
- Can be scheduled in Airflow or executed manually
- Reads credentials from `.env` via `DUMP_URL` variable

### `__init__.py`
Enables the directory to be recognized as a Python module for cleaner imports.

---

## Usage

### Run a full backup manually:
```bash
python src/db/backup_postgres.py
```
---

## Linked Components

- [Works in conjunction with `alembic/` for versioned migrations](../../alembic/versions)
- [Backup files are saved under `data/gold/dumps/`](../../data/gold/dumps/)



