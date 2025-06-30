# Alembic Migrations -- Crypto DWH

This folder contains all schema migration files for the PostgreSQL Data Warehouse used in the CryptoLakehouse ELT pipeline.
Each migration represents a versioned schema change, tracked via Alembic and aligned with the evolution of the models.py file.

## Structure

- `env.py` -- Loads metadata and database URL from `.env`, connects Alembic to SQLAlchemy models.
- `script.py.mako` -- Template for generated migration files.
- `versions/` -- Stores all versioned migration scripts.

## Migration History

| Version | Description                                           | File                                                         |
|---------|-------------------------------------------------------|--------------------------------------------------------------|
| v1      | Add exchange domain schema                            | `7c81f1d77f74_v1_exchange_domain_schema.py`                  |
| v2      | Add crypto domain schema (dims, facts, links)         | `4a4bafa7b8b7_v2_add_crypto_domain_schema_dimensions_.py`    |
| v3      | Add FK `crypto_id` to `fact_exchange_assets`          | `243445aead18_v3_add_fk_crypto_id_dim_crypto_id_in_.py`      |
| v4      | Add `fact_global_market` (global metrics domain)      | `7ec1855fabdb_v4_add_fact_global_market_table_global_.py`    |
| v5      | Add `fact_market_sentiment` (fear & greed)            | `c1918ae03efd_v5_add_fact_market_sentiment_table_fear_.py`   |
| v6      | Increase slug length to 255 in `dim_crypto_info`      | `62c26ff5ae6b_increase_slug_length_to_255_in_dim_.py`        |
| v7      | Add indexes on `name` columns (exchange & crypto ID)  | `c6016d1e5963_v7_add_index_on_name_columns.py`               |

## Notes

- Migrations are auto-generated from changes in `src/db/models.py`.
- The `sqlalchemy.url` is dynamically loaded from the `.env` file via `load_dotenv()` in `env.py`.
- To create a new migration:  
  `alembic revision --autogenerate -m "vX: description"`
- To apply latest schema changes:  
  `alembic upgrade head`