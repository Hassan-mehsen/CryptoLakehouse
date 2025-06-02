-- ============================================================================
-- View      : vw_missing_crypto_rank_or_status
-- Purpose   : Monitor crypto records with missing rank or activity status
-- Notes     : These fields are sourced from /cryptocurrency/map and are refreshed regularly.
--             Missing rank or is_active might indicate delays or failures in ingestion.
-- ============================================================================

CREATE OR REPLACE VIEW vw_missing_crypto_rank_or_status AS
SELECT
    crypto_id,
    snapshot_timestamp
FROM dim_crypto_map
WHERE rank IS NULL OR is_active IS NULL;
