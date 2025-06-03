-- ============================================================================
-- View      : vw_crypto_supply_utilization
-- Purpose   : Analyze the structure and transparency of crypto token supply
-- ============================================================================

CREATE OR REPLACE VIEW vw_crypto_supply_utilization AS
SELECT
    f.crypto_id,
    c.name,
    f.snapshot_timestamp,

    -- Raw supply data
    f.circulating_supply,
    f.total_supply,
    f.max_supply,
    f.infinite_supply,
    f.self_reported_circulating_supply,

    -- Derived KPIs
    f.supply_utilization,
    f.missing_supply_ratio,
    f.fully_diluted_cap_ratio
FROM fact_crypto_market AS f
INNER JOIN dim_crypto_id AS c ON f.crypto_id = c.crypto_id
WHERE f.supply_utilization IS NOT NULL
ORDER BY f.supply_utilization DESC;
