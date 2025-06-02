-- ============================================================================
-- View      : vw_crypto_missing_names_or_symbols
-- Purpose   : Detect crypto entries with missing name or symbol in dim_crypto_id
-- Notes     : These are identity fields fetched from /cryptocurrency/map. 
--             Their absence indicates broken or incomplete ETL ingestion.
-- ============================================================================

CREATE OR REPLACE VIEW vw_crypto_missing_names_or_symbols AS
SELECT crypto_id
FROM dim_crypto_id
WHERE name IS NULL OR symbol IS NULL;
