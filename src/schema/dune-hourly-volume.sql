-- Hourly Volume Query with start_time Parameter
-- Returns hourly volume data for each token, starting from a specified timestamp
-- Used for:
--   1. Backfilling historical hourly data
--   2. Refreshing the current incomplete hour every ~10 minutes
-- 
-- Parameters:
--   {{start_time}}: Start timestamp for data fetch (format: YYYY-MM-DD HH:MM:SS, e.g., '2025-10-09 00:00:00')
--   {{token_list}}: comma-separated token addresses in format 'token1', 'token2'
--                   Use '__ALL__' to fetch all tokens
--
-- Usage:
--   - For full backfill: start_time = '2025-10-09 00:00:00', token_list = '__ALL__'
--   - For current hour: start_time = current hour start (e.g., '2026-01-07 20:00:00')
--   - For incremental: start_time = last fetched hour

WITH filtered_and_extracted AS (
    SELECT
        block_time,
        -- Truncate to hour for grouping
        date_trunc('hour', block_time) AS trading_hour,
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
        -- Use parameterized start_time for incremental fetching
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
        trading_hour,
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
        -- When '__ALL__' is in the list, return all tokens; otherwise filter by token_list
        ('__ALL__' IN ({{token_list}}) OR token IN ({{token_list}}))
        AND swap_type IN ('buy', 'sell')
        AND input_amount > 0
        AND output_amount > 0
        AND token IS NOT NULL
)
-- Aggregate by token AND hour for hourly breakdown
SELECT
    token,
    -- Format as ISO timestamp string for easy parsing
    CAST(trading_hour AS VARCHAR) AS hour,
    CAST(SUM(CASE WHEN swap_type = 'buy' THEN output_amount ELSE input_amount END) AS VARCHAR) AS base_volume,
    CAST(SUM(CASE WHEN swap_type = 'buy' THEN input_amount ELSE output_amount END) AS VARCHAR) AS target_volume,
    CAST(MAX(price) AS VARCHAR) AS high,
    CAST(MIN(price) AS VARCHAR) AS low,
    CAST(COUNT(*) AS VARCHAR) AS trade_count
FROM token_filtered
WHERE price IS NOT NULL AND price > 0
GROUP BY token, trading_hour
ORDER BY token ASC, trading_hour ASC;

