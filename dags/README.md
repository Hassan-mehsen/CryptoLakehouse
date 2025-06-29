# DAGs Directory


This folder contains all Airflow DAGs responsible for orchestrating the ELT pipeline of the CryptoLakehouse project.

> Each DAG coordinates data ingestion, transformation, loading, and maintenance tasks, ensuring reliable and scheduled data flows with strong quality controls and optimized resource usage.

---

## Structure Overview

```bash
dags/
├── daily_elt_dag.py    # Daily ELT pipeline (runs once every weekday)
├── weekly_elt_dag.py   # Weekly ELT pipeline (runs every Monday)
├── 5x_elt_dag.py       # 5x per day ELT pipeline (intraday, high-frequency)
├── 10x_elt_dag.py      # 10x per day ELT pipeline (market indicators, hourly)
├── db_backup_dag.py    # Weekly PostgreSQL backup DAG
├── init_dag.py         # One-time full ELT bootstrap DAG
├── dag_utils.py        # Shared configuration, paths, and utility functions
└── README.md           # (this file)

```
---
## Purpose & Responsibilities

- **`weekly_elt_dag.py`**: Runs every Monday at 08:00 UTC. Handles slow-changing endpoints and large batch updates.
- **`daily_elt_dag.py`**: Runs every weekday at 08:30 UTC. Orchestrates full daily extraction, transformation, loading, and post-load cleaning/data-quality SQL for core business datasets.
- **`5x_elt_dag.py`**: Runs 5 times per day (09:00, 11:00, 13:00, 15:00, 17:00 UTC) for fast-changing endpoints that need frequent refresh (e.g., top listings).
- **`10x_elt_dag.py`**: Runs hourly (from 10:30 to 19:30 UTC) for market/business metrics and high-velocity indicators.
- **`init_dag.py`**: Manually triggered for the initial full load and warehouse bootstrap.
- **`db_backup_dag.py`**: Weekly full backup of the PostgreSQL warehouse (maintenance).
- **`dag_utils.py`**: Centralized configuration for paths, Spark options, SQL scripts, and utility functions to ensure DRY, maintainable code.

---
## Design Principles

- **Modular, frequency-based DAGs**: Each pipeline is encapsulated in its own DAG, tailored to its update cadence (daily, weekly, intraday).
- **Serialized extraction**: All API extractors run sequentially (not in parallel) to respect API call rates and prevent errors.
- **Spark-driven orchestration**: Each phase (transform/load) triggers only one Spark job per DAG run, minimizing driver startup overhead.
- **Clear phase separation**: Extraction (bronze), transformation (silver), load (warehouse), and post-load (quality SQL) are handled in discrete steps.
- **Production-ready reliability**: Robust retry policies, strong error handling, and support for all Airflow executors (Local, Celery, Kubernetes).

---
## Notes & Best Practices

- All DAG schedules use **UTC** to align with external API timestamps and avoid timezone ambiguity.
- **Post-load SQL scripts** are only triggered in the daily pipeline to ensure core table consistency.
- **Backups** are separated from the ELT flow for resilience and operational independence.
- **Configuration centralization** in `dag_utils.py` ensures easy maintenance and consistency across DAGs.

---
## Example: Adding a New DAG

To add a new DAG (e.g., for a new update frequency):

1. Create a new `*_elt_dag.py` file, using one of the existing DAGs as a template.
2. Register the appropriate extractor(s) and Spark commands.
3. Reference shared configs/utilities from `dag_utils.py`.
4. Define the schedule and retry policy as needed.
