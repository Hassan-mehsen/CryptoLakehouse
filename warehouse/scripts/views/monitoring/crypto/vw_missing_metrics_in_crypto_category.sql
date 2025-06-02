-- ============================================================================
-- View      : vw_missing_metrics_in_crypto_category
-- Purpose   : Track rows with NULL market cap, volume, or num_tokens
-- Notes     : These indicate partial ingestion or misalignment in extraction logic
-- ============================================================================

CREATE OR REPLACE VIEW vw_missing_metrics_in_crypto_category AS
SELECT *
FROM fact_crypto_category
WHERE market_cap IS NULL OR volume IS NULL OR num_tokens IS NULL;
