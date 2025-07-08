# Warehouse Directory

This folder contains all the SQL and administrative components that define, validate, and operate the PostgreSQL Data Warehouse for the CryptoLakehouse project.

> This directory is structured to support clear collaboration, technical evaluation, and long-term maintainability.

---

## Structure Overview

```bash
warehouse/
├── admin/                  # Initialization scripts for roles and permissions
│   ├── init_dw.sh          # Automates setup using variables from .env
│   └── set_passwords.sh    # Loads role passwords from .env and applies them
│
├── scripts/
│   ├── dcl/                # Data Control Language (role creation, grants, etc.)
│   │   └── init_roles.sql
│   ├── procedures/         # PL/pgSQL procedures for filling NULLs and ensuring quality
│   ├── tests/              # Manual SQL test suites for data quality (organized by domain)
│   └── views/              # Categorized views by purpose and domain:
│       ├── anomalies/      # Outlier detection and schema validation
│       ├── business/       # Aggregated insights, KPIs, trend tracking for BI
│       └── monitoring/     # Data freshness, update delays, NULL propagation
│
└── README.md               # You are here
```
- Note: Backup logic is maintained in `src/db/backup_postgres.py` (not here, since it's executable code)

---

## Purpose & Best Practices

This directory is meant to:
- Isolate SQL logic from the Python ELT system (`src/`)
- Centralize all **business intelligence** and **quality enforcement** layers
- Enable **automated and idempotent setup** of roles, schemas, and privileges via shell + SQL
- Enforce **domain-based modularity** (`exchange`, `crypto`, `global_market`, `market_sentiment`)
- Support versioned and testable development of your DWH logic

---
 
## Notes

- SQL scripts are organized by domain and function (test, procedure, view)
- Views are pre-joined, denormalized, and optimized for BI consumption (e.g., Metabase)
- Tests are manually executable, making them transparent and easy to debug
- Role and permission setup is .env-driven, supporting easy reuse across environments