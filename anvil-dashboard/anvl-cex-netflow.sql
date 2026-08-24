-- ANVL CEX Netflow
-- Dune query 8424912: https://dune.com/queries/8424912
-- From dashboard: https://dune.com/anvil/anvil
--
-- Weekly ANVL flows between tracked centralized exchanges and everyone else,
-- over the token's full history.
-- withdrawals = ANVL leaving exchanges (positive), deposits = ANVL arriving on
-- exchanges (emitted negative for diverging columns), netflow = withdrawals
-- minus deposits (positive = net supply leaving exchanges).
-- Transfers between two CEX addresses are excluded (they change no aggregate).
-- Coverage: Dune's cex.addresses list only.
WITH
  cex_eth AS (
    SELECT DISTINCT address FROM cex.addresses WHERE blockchain = 'ethereum'
  )

SELECT
  CAST(DATE_TRUNC('week', tr.evt_block_time) AS DATE) AS week,
  SUM(CASE WHEN cf.address IS NOT NULL AND ct.address IS NULL
           THEN CAST(tr.value AS int256) ELSE CAST(0 AS int256) END) / 1e18  AS withdrawals,
  -SUM(CASE WHEN ct.address IS NOT NULL AND cf.address IS NULL
            THEN CAST(tr.value AS int256) ELSE CAST(0 AS int256) END) / 1e18 AS deposits,
  SUM(CASE WHEN cf.address IS NOT NULL AND ct.address IS NULL THEN  CAST(tr.value AS int256)
           WHEN ct.address IS NOT NULL AND cf.address IS NULL THEN -CAST(tr.value AS int256)
           ELSE CAST(0 AS int256) END) / 1e18                                AS netflow
FROM erc20_ethereum.evt_Transfer tr
LEFT JOIN cex_eth ct ON ct.address = tr."to"
LEFT JOIN cex_eth cf ON cf.address = tr."from"
WHERE tr.contract_address = 0xaeeaa594e7dc112d67b8547fe9767a02c15b5597
  -- scan bound: comfortably before the ANVL token launch
  AND tr.evt_block_time >= DATE '2024-06-01'
  AND (ct.address IS NOT NULL OR cf.address IS NOT NULL)
GROUP BY 1
ORDER BY 1
