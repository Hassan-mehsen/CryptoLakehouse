# Warehouse Directory

This folder contains all artifacts related to the structure, logic, and testing of the PostgreSQL Data Warehouse (DWH) for the crypto ETL project.
It is intended to help readers, collaborators, and recruiters quickly understand how SQL-based components are organized and maintained.

---

## Structure

- `README.md` - *You are here*
- `scripts/` - Hand-written SQL logic and utilities
  - `analysis/` - Business/KPI analysis queries (e.g. price trends, volatility)
  - `procedures/` - Stored procedures (PL/pgSQL or other database-side logic)
  - `tests/` - Data quality checks or schema validation queries
  - `views/` - SQL views (reusable virtual or materialized tables)
  - `dcl/` - Data Control Language scripts (e.g. user and access management)
    - `init_roles.sql` - Defines roles like `admin`, `etl`, and `analyst` with appropriate access rights

- Note: Backup logic is maintained in `src/db/backup_postgres.py` (not here, since it's executable code)

---

## 🎯 Purpose

This folder aims to:

- Track and organize SQL logic related to the PostgreSQL data warehouse
- Store reusable SQL components for analytics (views, procedures, etc.)
- Document test queries used for data validation and quality assurance
- Provide scripts for managing access and permissions (DCL layer)
- Keep operational Python scripts (e.g., backups) separated in the `src/` directory
- Help collaborators understand the DWH’s structure and behavior
