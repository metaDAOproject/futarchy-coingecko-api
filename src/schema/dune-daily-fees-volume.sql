-- Dune Query: Daily Fees and Volume per Token
-- Parameters: 
--   start_date: DATE - fetch data from this date onwards
--   token_list: TEXT - comma-separated list of tokens, or '__ALL__' for all tokens
--
-- For incremental fetches, set start_date to last complete date
-- Cumulative values are calculated in the query (stored in DB)

WITH filtered AS (
    -- Combine filtering and extraction in one pass to reduce I/O
    -- Extract only what we need, when we need it
    SELECT
        block_time,
        tx_id,
        -- Extract token immediately during scan
        CASE
            WHEN varbinary_length(data) = 406 THEN to_base58(varbinary_substring(data, 279, 32))
            WHEN varbinary_length(data) = 670 THEN to_base58(varbinary_substring(data, 543, 32))
        END AS token,
        -- Extract swap_type once
        CASE varbinary_substring(data, 105, 1)
            WHEN 0x00 THEN 'buy'
            WHEN 0x01 THEN 'sell'
        END AS swap_type,
        -- Pre-calculate amounts (divide by 1e6 once)
        varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 106, 8))) / 1e6 AS input_amount,
        varbinary_to_bigint(varbinary_reverse(varbinary_substring(data, 114, 8))) / 1e6 AS output_amount
    FROM solana.instruction_calls
    WHERE 
        -- partition pruning (huge)
        block_date >= DATE '{{start_date}}'
        AND block_date <= CURRENT_DATE

        AND tx_success = true
        AND is_inner = true
        AND executing_account = 'FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq'
        AND inner_executing_account = 'FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq'
        AND account_arguments[1] = 'DGEympSS4qLvdr9r3uGHTfACdN8snShk4iGdJtZPxuBC'
        AND cardinality(account_arguments) = 1

        -- cheap + selective varbinary checks
        AND varbinary_starts_with(data, 0xe445a52e51cb9a1d)
        AND varbinary_length(data) IN (406, 670)

        -- avoid array_join
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
    ('__ALL__' IN ({{token_list}}) OR token IN ({{token_list}}))
    AND input_amount > 0
    AND output_amount > 0
    AND token IS NOT NULL
),
daily AS (
  SELECT
    token,
    trading_date,
    SUM(CASE WHEN swap_type = 'buy' THEN output_amount ELSE input_amount END) AS base_volume,
    SUM(CASE WHEN swap_type = 'buy' THEN input_amount  ELSE output_amount END) AS target_volume,
    SUM(CASE WHEN swap_type = 'buy' THEN input_amount ELSE 0 END) AS usdc_volume,
    SUM(CASE WHEN swap_type = 'sell' THEN input_amount ELSE 0 END) AS token_volume,
    MAX(price) AS high,
    AVG(price) AS average_price,
    MIN(price) AS low
  FROM clean
  WHERE price > 0
  GROUP BY 1, 2
)
SELECT
  token,
  trading_date,
  base_volume,
  target_volume,
  COALESCE(usdc_volume * 0.005, 0) AS usdc_fees,
  COALESCE(token_volume * 0.005 * average_price, 0) AS token_fees_usdc,
  token_volume * 0.005 AS token_fees,
  usdc_volume AS buy_volume,
  token_volume AS sell_volume,
  token_volume * average_price AS sell_volume_usdc,
  SUM(usdc_volume) OVER (
    PARTITION BY token
    ORDER BY trading_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) * 0.005 AS cumulative_usdc_fees,
  SUM(token_volume * average_price) OVER (
    PARTITION BY token
    ORDER BY trading_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) * 0.005 AS cumulative_token_in_usdc_fees,
  SUM(target_volume) OVER (
    PARTITION BY token
    ORDER BY trading_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_target_volume,
  SUM(base_volume) OVER (
    PARTITION BY token
    ORDER BY trading_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_token_volume,
  high,
  average_price,
  low
FROM daily
ORDER BY token, trading_date;
