-- ============================================================================
-- View      : vw_invalid_fear_greed_scores
-- Purpose   : Detect out-of-range values in fear_greed_score (should be 0–100)
-- Notes     : Useful to identify data ingestion bugs or upstream API issues
-- ============================================================================

CREATE OR REPLACE VIEW vw_invalid_fear_greed_scores AS
SELECT *
FROM fact_market_sentiment
WHERE fear_greed_score < 0 OR fear_greed_score > 100;
