-- ============================================================================
-- Procedure : fill_nulls_exchange_map_info
-- Purpose   : Fill missing maker_fee, taker_fee, weekly_visits in dim_exchange_map
--             using the latest known values per exchange_id before the current snapshot.
-- Usage     : CALL fill_nulls_exchange_map_info();
-- Notes     : Only the most recent snapshot per exchange_id is corrected,
--             and only if **all three fields** are NULL - this ensures we
--             target missing values caused by the pipeline (not the API).
-- ============================================================================

CREATE OR REPLACE PROCEDURE fill_nulls_exchange_map_info()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Step 1: Select the most recent snapshot per exchange_id
    -- where ALL 3 fields are NULL, typically caused by misaligned snapshot timing
    -- (e.g., /exchange/map triggered but /exchange/info not yet available).
    WITH latest_null_snapshot AS (
        SELECT DISTINCT ON (exchange_id)
            exchange_id,
            snapshot_timestamp
        FROM dim_exchange_map
        WHERE maker_fee IS NULL AND taker_fee IS NULL AND weekly_visits IS NULL
        ORDER BY exchange_id, snapshot_timestamp DESC
    ),

    -- Step 2: For each selected (exchange_id, snapshot), find the most recent
    -- valid historical record with all 3 fields filled in (non-NULL).
    latest_valid_info AS (
        SELECT
            n.exchange_id,
            n.snapshot_timestamp,
            prev.maker_fee,
            prev.taker_fee,
            prev.weekly_visits
        FROM latest_null_snapshot n
        JOIN LATERAL (
            SELECT
                m.maker_fee,
                m.taker_fee,
                m.weekly_visits
            FROM dim_exchange_map m
            WHERE m.exchange_id = n.exchange_id
              AND m.snapshot_timestamp < n.snapshot_timestamp
              AND m.maker_fee IS NOT NULL
              AND m.taker_fee IS NOT NULL
              AND m.weekly_visits IS NOT NULL
            ORDER BY m.snapshot_timestamp DESC
            LIMIT 1
        ) prev ON TRUE
    )

    -- Step 3: Update the NULL snapshot using recovered values,
    -- only if ALL 3 fields are still NULL at that snapshot.
    UPDATE dim_exchange_map m
    SET
        maker_fee = lvi.maker_fee,
        taker_fee = lvi.taker_fee,
        weekly_visits = lvi.weekly_visits
    FROM latest_valid_info lvi
    WHERE m.exchange_id = lvi.exchange_id
      AND m.snapshot_timestamp = lvi.snapshot_timestamp
      AND (
          m.maker_fee IS NULL AND
          m.taker_fee IS NULL AND
          m.weekly_visits IS NULL
      );

    RAISE NOTICE 'dim_exchange_map: missing fields filled from historical values.';
END;
$$;
