-- Dune Query: Daily Buy/Sell Volume per Token
-- Parameters: 
--   start_date: DATE - fetch data from this date onwards
--   token_list: TEXT - comma-separated list of tokens, or '__ALL__' for all tokens
--
-- For incremental fetches, set start_date to last complete date
-- Cumulative values are calculated in PostgreSQL, not here (saves credits)

WITH filtered AS (
    SELECT
        block_time,
        tx_id,
        CASE
            WHEN varbinary_length(data) = 406 THEN to_base58(varbinary_substring(data, 279, 32))
            WHEN varbinary_length(data) = 670 THEN to_base58(varbinary_substring(data, 543, 32))
        END AS token,
        CASE varbinary_substring(data, 105, 1)
            WHEN 0x00 THEN 'buy'
            WHEN 0x01 THEN 'sell'
        END AS swap_type,
        varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 106, 8))) / 1e6 AS input_amount,
        varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 114, 8))) / 1e6 AS output_amount
    FROM solana.instruction_calls
    WHERE 
        -- Use start_date parameter for incremental fetching
        block_date >= DATE '{{start_date}}'
        AND block_date <= CURRENT_DATE
        AND tx_success = true
        AND is_inner = true
        AND executing_account = 'FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq'
        AND inner_executing_account = 'FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq'
        AND account_arguments[1] = 'DGEympSS4qLvdr9r3uGHTfACdN8snShk4iGdJtZPxuBC'
        AND cardinality(account_arguments) = 1
        AND varbinary_starts_with(data, 0xe445a52e51cb9a1d)
        AND varbinary_length(data) IN (406, 670)
        AND any_match(log_messages, x -> strpos(x, 'SpotSwap') > 0)
),
clean AS (
    SELECT
        token,
        date_trunc('day', block_time) AS trading_date,
        swap_type,
        input_amount,
        output_amount,
        CASE
            WHEN swap_type = 'buy'  THEN input_amount / NULLIF(output_amount, 0)
            WHEN swap_type = 'sell' THEN output_amount / NULLIF(input_amount, 0)
        END AS price
    FROM filtered
    WHERE
        token IS NOT NULL
        AND input_amount > 0
        AND output_amount > 0
        AND ('__ALL__' IN ({{token_list}}) OR token IN ({{token_list}}))
)
SELECT
    token,
    trading_date AS date,
    SUM(CASE WHEN swap_type = 'buy' THEN output_amount ELSE input_amount END) AS base_volume,
    SUM(CASE WHEN swap_type = 'buy' THEN input_amount ELSE output_amount END) AS target_volume,
    SUM(CASE WHEN swap_type = 'buy' THEN input_amount ELSE 0 END) AS buy_usdc_volume,
    SUM(CASE WHEN swap_type = 'sell' THEN input_amount ELSE 0 END) AS sell_token_volume,
    MAX(price) AS high,
    MIN(price) AS low,
    COUNT(*) AS trade_count
FROM clean
WHERE price > 0
GROUP BY 1, 2
ORDER BY token, trading_date;
