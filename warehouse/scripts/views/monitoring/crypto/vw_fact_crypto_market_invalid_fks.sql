-- ============================================================================
-- View      : vw_fact_crypto_market_invalid_fks
-- Purpose   : Identify crypto_id values in fact table that are missing from dim_crypto_id
-- Notes     : This ensures foreign key consistency between dimensions and facts.
--             Any rows returned may reflect ingestion misalignment or data drift.
-- ============================================================================

CREATE OR REPLACE VIEW vw_fact_crypto_market_invalid_fks AS
SELECT
    fm.crypto_id,
    fm.snapshot_timestamp
FROM fact_crypto_market AS fm
LEFT JOIN dim_crypto_id AS d ON fm.crypto_id = d.crypto_id
WHERE d.crypto_id IS NULL;
