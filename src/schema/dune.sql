WITH filtered_and_extracted AS (
    -- Combine filtering and extraction in one pass to reduce I/O
    -- Extract only what we need, when we need it
    SELECT
        block_time,
        tx_id,
        -- Extract token immediately during scan
        CASE
            WHEN LENGTH(data) = 406 THEN to_base58(SUBSTR(data, 279, 32))
            WHEN LENGTH(data) = 670 THEN to_base58(SUBSTR(data, 543, 32))
        END AS token,
        -- Extract swap_type once
        CASE
            WHEN to_hex(SUBSTR(data, 105, 1)) = '00' THEN 'buy'
            WHEN to_hex(SUBSTR(data, 105, 1)) = '01' THEN 'sell'
        END AS swap_type,
        -- Pre-calculate amounts (divide by 1e6 once)
        from_big_endian_64(reverse(SUBSTR(data, 106, 8))) / 1e6 AS input_amount,
        from_big_endian_64(reverse(SUBSTR(data, 114, 8))) / 1e6 AS output_amount,
        -- Extract quote_mint if needed for debugging
        CASE
            WHEN LENGTH(data) = 406 THEN to_base58(SUBSTR(data, 311, 32))
            WHEN LENGTH(data) = 670 THEN to_base58(SUBSTR(data, 575, 32))
        END AS quote_mint
    FROM solana.instruction_calls
    WHERE 
        -- Most selective filters first (reduce cardinality early)
        block_time >= current_timestamp - interval '24' hour
        AND tx_success = true
        AND executing_account = 'FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq'
        AND inner_executing_account = 'FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq'
        AND account_arguments[1] = 'DGEympSS4qLvdr9r3uGHTfACdN8snShk4iGdJtZPxuBC'
        AND is_inner = true
        -- More expensive filters last
        AND cardinality(account_arguments) = 1
        AND CAST(data AS VARCHAR) LIKE '0xe445a52e51cb9a1d%'
        AND LENGTH(data) >= 300
        AND array_join(log_messages, ' ') LIKE '%SpotSwap%'
),
token_filtered AS (
    -- Apply token filter and quality checks
    SELECT
        block_time,
        tx_id,
        token,
        quote_mint,
        swap_type,
        input_amount,
        output_amount,
        -- Calculate price once per row
        CASE
            WHEN swap_type = 'buy' THEN input_amount / NULLIF(output_amount, 0)
            WHEN swap_type = 'sell' THEN output_amount / NULLIF(input_amount, 0)
        END AS price
    FROM filtered_and_extracted
    WHERE 
        -- Apply token filter early
        (COALESCE({{token_list}}, '') = '' OR token IN ({{token_list}}))
        -- Data quality filters
        AND swap_type IN ('buy', 'sell')
        AND input_amount > 0
        AND output_amount > 0
        AND token IS NOT NULL
)
-- Final aggregation with pre-calculated values
SELECT
    token,
    CAST(SUM(CASE WHEN swap_type = 'buy' THEN output_amount ELSE input_amount END) AS VARCHAR) AS base_volume_24h,
    CAST(SUM(CASE WHEN swap_type = 'buy' THEN input_amount ELSE output_amount END) AS VARCHAR) AS target_volume_24h,
    CAST(MAX(price) AS VARCHAR) AS high_24h,
    CAST(MIN(price) AS VARCHAR) AS low_24h
FROM token_filtered
WHERE price IS NOT NULL AND price > 0
GROUP BY token
ORDER BY CAST(SUM(CASE WHEN swap_type = 'buy' THEN input_amount ELSE output_amount END) AS DOUBLE) DESC;