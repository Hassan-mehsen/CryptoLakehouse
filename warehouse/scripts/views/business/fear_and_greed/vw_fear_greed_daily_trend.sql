-- ============================================================================
-- View      : vw_fear_greed_daily_trend
-- Purpose   : Provide daily evolution of market sentiment index
-- ============================================================================

CREATE OR REPLACE VIEW vw_fear_greed_daily_trend AS
SELECT
    snapshot_timestamp,
    fear_greed_score,
    sentiment_label
FROM fact_market_sentiment
WHERE fear_greed_score IS NOT NULL
ORDER BY snapshot_timestamp DESC;
