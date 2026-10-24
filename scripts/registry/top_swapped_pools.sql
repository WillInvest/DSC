-- DuneSQL: Ethereum canonical Uniswap V2/V3, 50 pools per version.
-- One swap = one pool event, not one transaction or a dollar amount.
-- Fixed inclusive cutoff, matching the node snapshot in contracts/ethereum.
-- No TVL, volume, token, age, or current-activity filter.
WITH swap_events AS (
    SELECT DISTINCT
        version,
        project_contract_address AS pool_address,
        tx_hash,
        evt_index,
        block_number,
        block_time
    FROM dex.trades
    WHERE blockchain = 'ethereum'
      AND project = 'uniswap'
      AND version IN ('2', '3')
      AND block_number <= 26259428
), pool_counts AS (
    SELECT
        version,
        pool_address,
        COUNT(*) AS swap_count,
        MIN(block_number) AS first_swap_block,
        MAX(block_number) AS last_swap_block,
        MIN(block_time) AS first_swap_time,
        MAX(block_time) AS last_swap_time
    FROM swap_events
    GROUP BY version, pool_address
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY version ORDER BY swap_count DESC, pool_address ASC
    ) AS pool_rank
    FROM pool_counts
)
SELECT
    1 AS chain_id,
    CONCAT('uniswap-v', version) AS protocol,
    pool_rank,
    CONCAT('0x', LOWER(TO_HEX(pool_address))) AS address,
    swap_count,
    first_swap_block,
    last_swap_block,
    first_swap_time,
    last_swap_time,
    26259428 AS through_block
FROM ranked
WHERE pool_rank <= 50
ORDER BY version, pool_rank
