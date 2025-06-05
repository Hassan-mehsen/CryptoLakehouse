-- ============================================================================
-- View      : vw_outdated_last_updated_timestamp
-- Purpose   : Monitor outdated last_updated_timestamp values in fact_global_market
-- Notes     : 
--   - The API updates data every 5 minutes.
--   - Any delay beyond 5 minutes between snapshot_timestamp and last_updated_timestamp 
--     may indicate ingestion lag or broken sync with upstream data.
-- ============================================================================

CREATE OR REPLACE VIEW vw_outdated_last_updated_timestamp AS
SELECT
    snapshot_timestamp,
    last_updated_timestamp,
    EXTRACT(
        EPOCH FROM snapshot_timestamp - last_updated_timestamp
    ) AS delay_seconds
FROM fact_global_market
WHERE
    last_updated_timestamp IS NOT NULL
    AND snapshot_timestamp - last_updated_timestamp > INTERVAL '5 minutes';
