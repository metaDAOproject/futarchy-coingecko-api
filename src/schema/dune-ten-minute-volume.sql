-- 10-Minute Volume Query with start_time Parameter
-- Returns 10-minute bucket volume data for each token
-- Used for accurate rolling 24h calculations on /api/tickers
-- 
-- Parameters:
--   {{start_time}}: Start timestamp for data fetch (format: YYYY-MM-DD HH:MM:SS)
--   {{token_list}}: comma-separated token addresses in format 'token1', 'token2'
--                   Use '__ALL__' to fetch all tokens
--
-- Usage:
--   - For backfill: start_time = 24 hours ago
--   - For refresh: start_time = current 10-minute bucket start
--
-- Schedule: Every 10 minutes at :00, :10, :20, :30, :40, :50 (144 queries/day)

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
)
-- Aggregate by token AND 10-minute bucket
SELECT
    token,
    CAST(bucket_time AS VARCHAR) AS bucket,
    CAST(SUM(CASE WHEN swap_type = 'buy' THEN output_amount ELSE input_amount END) AS VARCHAR) AS base_volume,
    CAST(SUM(CASE WHEN swap_type = 'buy' THEN input_amount ELSE output_amount END) AS VARCHAR) AS target_volume,
    CAST(MAX(price) AS VARCHAR) AS high,
    CAST(MIN(price) AS VARCHAR) AS low,
    CAST(COUNT(*) AS VARCHAR) AS trade_count
FROM token_filtered
WHERE price IS NOT NULL AND price > 0
GROUP BY token, bucket_time
ORDER BY token ASC, bucket_time ASC;

