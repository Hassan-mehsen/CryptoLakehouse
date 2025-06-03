-- ============================================================================
-- File      : warehouse/scripts/tests/fear_and_greed_data_tests.sql
-- Purpose   : Centralized quality tests for the 'fear & greed' sentiment domain in the DWH
-- Notes     : Run manually to inspect anomalies and ensure consistency of market sentiment data
-- ============================================================================

-- 1. Check for NULL fear_greed_score
SELECT *
FROM fact_market_sentiment
WHERE fear_greed_score IS NULL;

-- 2. Check for invalid score ranges (should be between 0 and 100)
SELECT *
FROM fact_market_sentiment
WHERE fear_greed_score < 0 OR fear_greed_score > 100;

-- 3. Check for NULL or invalid sentiment labels
SELECT *
FROM fact_market_sentiment
WHERE sentiment_label IS NULL;

-- 4. Check for duplicate snapshot_timestamp
SELECT
    snapshot_timestamp,
    COUNT(*)
FROM fact_market_sentiment
GROUP BY snapshot_timestamp
HAVING COUNT(*) > 1;
