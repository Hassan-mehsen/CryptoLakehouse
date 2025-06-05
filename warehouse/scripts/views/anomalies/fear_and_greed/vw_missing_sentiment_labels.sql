-- ============================================================================
-- View      : vw_missing_sentiment_labels
-- Purpose   : Detect NULL sentiment labels in market sentiment snapshots
-- Notes     : A missing label can break sentiment categorization dashboards
-- ============================================================================

CREATE OR REPLACE VIEW vw_missing_sentiment_labels AS
SELECT *
FROM fact_market_sentiment
WHERE sentiment_label IS NULL;
