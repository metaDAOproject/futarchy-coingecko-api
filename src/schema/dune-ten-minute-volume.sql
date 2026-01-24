-- Extended 10-Minute Volume Query with start_time and end_time Parameters
-- Returns 10-minute bucket volume data for each token with buy/sell volumes and fees
-- Used for accurate rolling 24h calculations on /api/tickers
-- This is the SINGLE SOURCE OF TRUTH - all other aggregations (hourly, daily) are derived from this data
-- 
-- Parameters:
--   {{start_time}}: Start timestamp for data fetch (format: YYYY-MM-DD HH:MM:SS)
--   {{end_time}}: End timestamp for data fetch (format: YYYY-MM-DD HH:MM:SS) - optional, if not provided fetches until now
--   {{token_list}}: comma-separated token addresses in format 'token1', 'token2'
--                   Use '__ALL__' to fetch all tokens
--
-- Usage:
--   - For backfill: start_time = day start, end_time = day end (to limit data)
--   - For refresh: start_time = current 10-minute bucket start, end_time = current time
--
-- Schedule: Every 10 minutes at :00, :10, :20, :30, :40, :50 (144 queries/day)
--
-- Returns: token, bucket, base_volume, target_volume, buy_volume, sell_volume, high, low, 
--          average_price, trade_count, usdc_fees, token_fees, token_fees_usdc, sell_volume_usdc

WITH filtered_and_extracted AS (
    SELECT
        block_time,
        -- Truncate to 10-minute bucket: floor(minute/10)*10
        date_trunc('hour', block_time) + 
          (floor(extract(minute from block_time) / 10) * interval '10' minute) AS bucket_time,
        tx_id,
        CASE
            WHEN LENGTH(data) = 406 THEN to_base58(SUBSTR(data, 279, 32))
            WHEN LENGTH(data) = 670 THEN to_base58(SUBSTR(data, 543, 32))
        END AS token,
        CASE
            WHEN to_hex(SUBSTR(data, 105, 1)) = '00' THEN 'buy'
            WHEN to_hex(SUBSTR(data, 105, 1)) = '01' THEN 'sell'
        END AS swap_type,
        from_big_endian_64(reverse(SUBSTR(data, 106, 8))) / 1e6 AS input_amount,
        from_big_endian_64(reverse(SUBSTR(data, 114, 8))) / 1e6 AS output_amount,
        CASE
            WHEN LENGTH(data) = 406 THEN to_base58(SUBSTR(data, 311, 32))
            WHEN LENGTH(data) = 670 THEN to_base58(SUBSTR(data, 575, 32))
        END AS quote_mint
    FROM solana.instruction_calls
    WHERE 
        block_time >= TIMESTAMP '{{start_time}}'
        AND block_time < TIMESTAMP '{{end_time}}'
        AND tx_success = true
        AND executing_account = 'FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq'
        AND inner_executing_account = 'FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq'
        AND account_arguments[1] = 'DGEympSS4qLvdr9r3uGHTfACdN8snShk4iGdJtZPxuBC'
        AND is_inner = true
        AND cardinality(account_arguments) = 1
        AND CAST(data AS VARCHAR) LIKE '0xe445a52e51cb9a1d%'
        AND LENGTH(data) >= 300
        AND array_join(log_messages, ' ') LIKE '%SpotSwap%'
),
token_filtered AS (
    SELECT
        block_time,
        bucket_time,
        tx_id,
        token,
        quote_mint,
        swap_type,
        input_amount,
        output_amount,
        CASE
            WHEN swap_type = 'buy' THEN input_amount / NULLIF(output_amount, 0)
            WHEN swap_type = 'sell' THEN output_amount / NULLIF(input_amount, 0)
        END AS price
    FROM filtered_and_extracted
    WHERE 
        ('__ALL__' IN ({{token_list}}) OR token IN ({{token_list}}))
        AND swap_type IN ('buy', 'sell')
        AND input_amount > 0
        AND output_amount > 0
        AND token IS NOT NULL
),
aggregated AS (
    -- First aggregate to get base metrics and average_price
    SELECT
        token,
        bucket_time,
        SUM(CASE WHEN swap_type = 'buy' THEN output_amount ELSE input_amount END) AS base_volume,
        SUM(CASE WHEN swap_type = 'buy' THEN input_amount ELSE output_amount END) AS target_volume,
        SUM(CASE WHEN swap_type = 'buy' THEN input_amount ELSE 0 END) AS buy_volume,
        SUM(CASE WHEN swap_type = 'sell' THEN input_amount ELSE 0 END) AS sell_volume,
        MAX(price) AS high,
        MIN(price) AS low,
        AVG(price) AS average_price,
        COUNT(*) AS trade_count
    FROM token_filtered
    WHERE price IS NOT NULL AND price > 0
    GROUP BY token, bucket_time
)
-- Final SELECT with fee calculations using pre-calculated average_price
SELECT
    token,
    CAST(bucket_time AS VARCHAR) AS bucket,
    -- Core volume metrics
    CAST(base_volume AS VARCHAR) AS base_volume,
    CAST(target_volume AS VARCHAR) AS target_volume,
    -- Buy/sell volume breakdown
    CAST(buy_volume AS VARCHAR) AS buy_volume,
    CAST(sell_volume AS VARCHAR) AS sell_volume,
    -- Price metrics
    CAST(high AS VARCHAR) AS high,
    CAST(low AS VARCHAR) AS low,
    CAST(average_price AS VARCHAR) AS average_price,
    -- Trade count
    CAST(trade_count AS VARCHAR) AS trade_count,
    -- Fees (0.5% = 0.005)
    CAST(buy_volume * 0.005 AS VARCHAR) AS usdc_fees,
    CAST(sell_volume * 0.005 AS VARCHAR) AS token_fees,
    CAST(sell_volume * average_price * 0.005 AS VARCHAR) AS token_fees_usdc,
    -- Sell volume in USDC terms
    CAST(sell_volume * average_price AS VARCHAR) AS sell_volume_usdc
FROM aggregated
ORDER BY token ASC, bucket_time ASC;

