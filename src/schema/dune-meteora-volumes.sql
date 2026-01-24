-- Dune Query: Meteora Daily Volumes per Owner
-- Parameters:
--   start_date: DATE - fetch data from this date onwards (default: '2025-10-09')
--
-- Returns daily aggregated data per owner with all required fields
-- Cumulative fields are calculated in PostgreSQL, not here

WITH
params AS (
  SELECT TIMESTAMP '{{start_date}}' AS start_ts
),

target_pools AS (
  SELECT '7dVri3qjYD3uobSZL3Zth8vSCgU6r6R2nvFsh7uVfDte' AS pool -- Umbra / USDC
  UNION ALL SELECT '59WuweKV7DAg8aUgRhNytScQxioaFYNJdWnox5FxAXFq' -- Ranger / USDC
  UNION ALL SELECT '6F88Y6iukU9GuL8CMWnx6YT832vBymNPicJBikQWeYe4' -- Paystream / USDC
  UNION ALL SELECT 'BGg7WsK98rhqtTp2uSKMa2yETqgwShFAjyf1RmYqCF7n' -- Loyal / USDC
  UNION ALL SELECT '5gB4NPgFB3MHFHSeKN4sbaY6t9MB8ikCe9HyiKYid4Td' -- Avici / USDC
  UNION ALL SELECT '57SnL1dxJPgc6TH6DcbRn7Nn5jnYCdcrkpVTy9d5vRuP' -- ZKFG / USDC
  UNION ALL SELECT '2zsbECzM7roqnDcuv2TNGpfv5PAnuqGmMo5YPtqmUz5p' -- Solomon / USDC
),

target_owners AS (
  SELECT '6VsC8PuKkXm5xo54c2vbrAaSfQipkpGHqNuKTxXFySx6' AS owner -- Umbra
  UNION ALL SELECT '55H1Q1YrHJQ93uhG4jqrBBHx3a8H7TCM8kvf2UM2g5q3' -- Ranger
  UNION ALL SELECT 'BpXtB2ASf2Tft97ewTd8PayXCqFQ6Wqod33qrwwfK9Vz' -- Paystream
  UNION ALL SELECT 'AQyyTwCKemeeMu8ZPZFxrXMbVwAYTSbBhi1w4PBrhvYE' -- Loyal
  UNION ALL SELECT 'DGgYoUcu1aDZt4GEL5NQiducwHRGbkMWsUzsXh2j622G' -- Avici
  UNION ALL SELECT 'BNvDfXYG2FAyBDYD71Xr9GhKE18MbmhtjsLKsCuXho6z' -- ZKFG
  UNION ALL SELECT '98SPcyUZ2rqM2dgjCqqSXS4gJrNTLSNUAAVCF38xYj9u' -- Solomon
),

pool_map AS (
  -- pool, position, owner (one row per pool you want to track)
  SELECT '7dVri3qjYD3uobSZL3Zth8vSCgU6r6R2nvFsh7uVfDte' AS pool,
         '2b3fM2n9iTPG1xJrPevtdQ7Ju5QHuRbBmmA84k3UF4TA' AS position,
         '6VsC8PuKkXm5xo54c2vbrAaSfQipkpGHqNuKTxXFySx6' AS owner
  UNION ALL SELECT '59WuweKV7DAg8aUgRhNytScQxioaFYNJdWnox5FxAXFq',
                  'GyPSZcXCEGxHrcX5Trs131G13HbwDYZfr2pPAijzAEcg',
                  '55H1Q1YrHJQ93uhG4jqrBBHx3a8H7TCM8kvf2UM2g5q3'
  UNION ALL SELECT '6F88Y6iukU9GuL8CMWnx6YT832vBymNPicJBikQWeYe4',
                  'oawFz9eK6eqiTKDuoShc14Tt7sjzgjBY9VGGwpjdNGb',
                  'BpXtB2ASf2Tft97ewTd8PayXCqFQ6Wqod33qrwwfK9Vz'
  UNION ALL SELECT 'BGg7WsK98rhqtTp2uSKMa2yETqgwShFAjyf1RmYqCF7n',
                  '5xhd93HfYtsjvDki7ZWs2NSukfKdXzWPVvD7tnQ4Xkb5',
                  'AQyyTwCKemeeMu8ZPZFxrXMbVwAYTSbBhi1w4PBrhvYE'
  UNION ALL SELECT '5gB4NPgFB3MHFHSeKN4sbaY6t9MB8ikCe9HyiKYid4Td',
                  '3n3bY2XBcuqXDZ5kXZLKUzFSoSKPJjjZtyDa11CwfDqC',
                  'DGgYoUcu1aDZt4GEL5NQiducwHRGbkMWsUzsXh2j622G'
  UNION ALL SELECT '57SnL1dxJPgc6TH6DcbRn7Nn5jnYCdcrkpVTy9d5vRuP',
                  '6PW5FipH8374LuocEAfjLKUJ991hsyBR8UQ1CEYkJgAa',
                  'BNvDfXYG2FAyBDYD71Xr9GhKE18MbmhtjsLKsCuXho6z'
  UNION ALL SELECT '2zsbECzM7roqnDcuv2TNGpfv5PAnuqGmMo5YPtqmUz5p',
                  'w1BDxR4FvN4KryBuJwcEuohYKHkyDzD1beNH3AhF6Wn',
                  '98SPcyUZ2rqM2dgjCqqSXS4gJrNTLSNUAAVCF38xYj9u'
),

/* -----------------------------
   INIT LIQUIDITY (KEEP THIS)
   evtinitializepool lacks owner/position, so we attribute init liquidity
   to the first add_liquidity call AFTER init for that pool.
   ----------------------------- */
initial_liquidity AS (
  SELECT
    e.evt_block_time AS time,
    m.pool,
    m.position,
    m.owner,
    -- if DECIMAL(38,0) overflows for some pools, switch to TRY_CAST(... AS DOUBLE)
    CAST(e.liquidity AS DECIMAL(38,0)) AS liquidity_delta
  FROM meteora_solana.cp_amm_evt_evtinitializepool e
  JOIN pool_map m
    ON m.pool = e.pool
  WHERE e.evt_block_time >= (SELECT start_ts FROM params)
),

/* -----------------------------
   ADD/REMOVE LIQUIDITY (ROBUST JSON PATHS + NULL-SAFE)
   ----------------------------- */
add_liquidity AS (
  SELECT
    call_block_time AS time,
    account_pool AS pool,
    account_position AS position,
    account_owner AS owner,
    COALESCE(
      TRY_CAST(JSON_EXTRACT_SCALAR(params, '$.AddLiquidityParameters.liquidity_delta') AS DOUBLE),
      TRY_CAST(JSON_EXTRACT_SCALAR(params, '$.AddLiquidityParameters2.liquidity_delta') AS DOUBLE),
      TRY_CAST(JSON_EXTRACT_SCALAR(params, '$.liquidity_delta') AS DOUBLE),
      0
    ) AS liquidity_delta
  FROM meteora_solana.cp_amm_call_add_liquidity
  WHERE call_block_time >= (SELECT start_ts FROM params)
    AND account_pool IN (SELECT pool FROM target_pools)
),

remove_liquidity AS (
  SELECT
    call_block_time AS time,
    account_pool AS pool,
    account_position AS position,
    account_owner AS owner,
    -COALESCE(
      TRY_CAST(JSON_EXTRACT_SCALAR(params, '$.RemoveLiquidityParameters.liquidity_delta') AS DOUBLE),
      TRY_CAST(JSON_EXTRACT_SCALAR(params, '$.RemoveLiquidityParameters2.liquidity_delta') AS DOUBLE),
      TRY_CAST(JSON_EXTRACT_SCALAR(params, '$.liquidity_delta') AS DOUBLE),
      0
    ) AS liquidity_delta
  FROM meteora_solana.cp_amm_call_remove_liquidity
  WHERE call_block_time >= (SELECT start_ts FROM params)
    AND account_pool IN (SELECT pool FROM target_pools)
),

all_liquidity_changes AS (
  SELECT * FROM initial_liquidity
  UNION ALL SELECT * FROM add_liquidity
  UNION ALL SELECT * FROM remove_liquidity
),

liquidity_deltas_daily AS (
  SELECT
    DATE_TRUNC('day', time) AS day,
    pool,
    position,
    owner,
    SUM(COALESCE(liquidity_delta, 0)) AS liquidity_delta_day
  FROM all_liquidity_changes
  GROUP BY 1,2,3,4
),

position_liquidity_daily AS (
  SELECT
    day,
    pool,
    position,
    owner,
    SUM(COALESCE(liquidity_delta_day, 0)) OVER (
      PARTITION BY pool, position, owner
      ORDER BY day
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_liquidity
  FROM liquidity_deltas_daily
),

pool_liquidity_daily AS (
  SELECT
    day,
    pool,
    SUM(cumulative_liquidity) AS total_pool_liquidity
  FROM position_liquidity_daily
  WHERE cumulative_liquidity > 0
  GROUP BY 1,2
),

ownership_shares_daily AS (
  SELECT
    p.day,
    p.pool,
    p.position,
    p.owner,
    p.cumulative_liquidity,
    t.total_pool_liquidity,
    CAST(p.cumulative_liquidity AS DOUBLE) / NULLIF(CAST(t.total_pool_liquidity AS DOUBLE), 0) AS ownership_share
  FROM position_liquidity_daily p
  JOIN pool_liquidity_daily t
    ON p.day = t.day AND p.pool = t.pool
  WHERE p.cumulative_liquidity > 0
),

/* -----------------------------
   SWAPS (UNCHANGED LOGIC, JUST MULTI-POOL)
   ----------------------------- */
swaps_union AS (
  SELECT
    DATE_TRUNC('day', evt_block_time) AS day,
    pool,
    trade_direction,
    CASE WHEN trade_direction = 0 THEN CAST(JSON_EXTRACT_SCALAR(params, '$.SwapParameters.amount_in') AS DOUBLE) END AS usdc_in_raw,
    CASE WHEN trade_direction = 1 THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS DOUBLE) END AS usdc_out_raw,
    CASE WHEN trade_direction = 1 THEN CAST(JSON_EXTRACT_SCALAR(params, '$.SwapParameters.amount_in') AS DOUBLE) END AS token_in_raw,
    CASE WHEN trade_direction = 0 THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.output_amount') AS DOUBLE) END AS token_out_raw,
    CASE WHEN trade_direction = 0 THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.lp_fee') AS DOUBLE) END AS lp_fee_usdc_raw,
    CASE WHEN trade_direction = 1 THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult.lp_fee') AS DOUBLE) END AS lp_fee_token_raw
  FROM meteora_solana.cp_amm_evt_evtswap
  WHERE evt_block_time >= (SELECT start_ts FROM params)
    AND pool IN (SELECT pool FROM target_pools)

  UNION ALL

  SELECT
    DATE_TRUNC('day', evt_block_time) AS day,
    pool,
    trade_direction,
    CASE WHEN trade_direction = 0 THEN CAST(JSON_EXTRACT_SCALAR(params, '$.SwapParameters2.amount_0') AS DOUBLE) END AS usdc_in_raw,
    CASE WHEN trade_direction = 1 THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult2.output_amount') AS DOUBLE) END AS usdc_out_raw,
    CASE WHEN trade_direction = 1 THEN CAST(JSON_EXTRACT_SCALAR(params, '$.SwapParameters2.amount_0') AS DOUBLE) END AS token_in_raw,
    CASE WHEN trade_direction = 0 THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult2.output_amount') AS DOUBLE) END AS token_out_raw,
    CASE WHEN trade_direction = 0 THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult2.trading_fee') AS DOUBLE) END AS lp_fee_usdc_raw,
    CASE WHEN trade_direction = 1 THEN CAST(JSON_EXTRACT_SCALAR(swap_result, '$.SwapResult2.trading_fee') AS DOUBLE) END AS lp_fee_token_raw
  FROM meteora_solana.cp_amm_evt_evtswap2
  WHERE evt_block_time >= (SELECT start_ts FROM params)
    AND pool IN (SELECT pool FROM target_pools)
),

daily_fees AS (
  SELECT
    day,
    pool,
    COUNT(*) AS num_swaps,
    (SUM(COALESCE(usdc_in_raw, 0)) + SUM(COALESCE(usdc_out_raw, 0))) / 1e6 AS volume_usd_approx,
    SUM(COALESCE(lp_fee_usdc_raw, 0)) / 1e6 AS lp_fee_usdc,
    SUM(COALESCE(lp_fee_token_raw, 0)) / 1e6 AS lp_fee_token,

    1 / NULLIF(
      COALESCE(
        (SUM(COALESCE(token_in_raw, 0)) / 1e6) / NULLIF(SUM(COALESCE(usdc_out_raw, 0)) / 1e6, 0),
        (SUM(COALESCE(token_out_raw, 0)) / 1e6) / NULLIF(SUM(COALESCE(usdc_in_raw, 0)) / 1e6, 0)
      ),
      0
    ) AS token_per_usdc_raw,

    COALESCE(
      (SUM(COALESCE(token_in_raw, 0)) / 1e6) / NULLIF(SUM(COALESCE(usdc_out_raw, 0)) / 1e6, 0),
      (SUM(COALESCE(token_out_raw, 0)) / 1e6) / NULLIF(SUM(COALESCE(usdc_in_raw, 0)) / 1e6, 0)
    ) AS token_price_usdc,

    (SUM(COALESCE(lp_fee_token_raw, 0)) / 1e6) *
    COALESCE(
      (SUM(COALESCE(token_in_raw, 0)) / 1e6) / NULLIF(SUM(COALESCE(usdc_out_raw, 0)) / 1e6, 0),
      (SUM(COALESCE(token_out_raw, 0)) / 1e6) / NULLIF(SUM(COALESCE(usdc_in_raw, 0)) / 1e6, 0)
    ) AS lp_fee_token_usdc,

    (SUM(COALESCE(lp_fee_usdc_raw, 0)) / 1e6)
    +
    (
      (SUM(COALESCE(lp_fee_token_raw, 0)) / 1e6) *
      COALESCE(
        (SUM(COALESCE(token_in_raw, 0)) / 1e6) / NULLIF(SUM(COALESCE(usdc_out_raw, 0)) / 1e6, 0),
        (SUM(COALESCE(token_out_raw, 0)) / 1e6) / NULLIF(SUM(COALESCE(usdc_in_raw, 0)) / 1e6, 0)
      )
    ) AS lp_fee_total_usdc,

    -- Buy volume: sum of USDC in for trade_direction=0 (buy)
    SUM(CASE WHEN trade_direction = 0 THEN COALESCE(usdc_in_raw, 0) ELSE 0 END) / 1e6 AS buy_volume,
    -- Sell volume: sum of token in for trade_direction=1 (sell)
    SUM(CASE WHEN trade_direction = 1 THEN COALESCE(token_in_raw, 0) ELSE 0 END) / 1e6 AS sell_volume
  FROM swaps_union
  GROUP BY 1,2
),

/* -----------------------------
   AS-OF OWNERSHIP: last known share <= fee day
   This fixes "no results" when swaps happen on days without LP events.
   ----------------------------- */
ownership_shares_asof AS (
  SELECT day, pool, owner, position, ownership_share
  FROM (
    SELECT
      f.day,
      f.pool,
      o.owner,
      o.position,
      o.ownership_share,
      ROW_NUMBER() OVER (
        PARTITION BY f.day, f.pool, o.owner, o.position
        ORDER BY o.day DESC
      ) AS rn
    FROM daily_fees f
    JOIN ownership_shares_daily o
      ON o.pool = f.pool
     AND o.day <= f.day
    WHERE o.owner IN (SELECT owner FROM target_owners)
  ) x
  WHERE rn = 1
),

fees_earned AS (
  SELECT
    f.day,
    f.pool,
    o.owner,
    o.position,
    o.ownership_share,
    f.num_swaps,
    f.volume_usd_approx,
    f.lp_fee_usdc,
    f.lp_fee_token,
    f.token_per_usdc_raw,
    f.token_price_usdc,
    f.lp_fee_token_usdc,
    f.lp_fee_total_usdc,
    f.buy_volume,
    f.sell_volume,
    f.lp_fee_total_usdc * o.ownership_share AS earned_fee_usdc
  FROM daily_fees f
  JOIN ownership_shares_asof o
    ON f.day = o.day AND f.pool = o.pool
)

SELECT
  CAST(day AS DATE) AS day,
  owner,
  num_swaps,
  volume_usd_approx,
  lp_fee_usdc,
  lp_fee_token,
  token_per_usdc_raw,
  token_price_usdc,
  lp_fee_token_usdc,
  lp_fee_total_usdc,
  ownership_share,
  earned_fee_usdc,
  buy_volume,
  sell_volume
FROM fees_earned
ORDER BY day, owner;
