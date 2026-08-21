-- Amp CEX Netflow
-- Dune query 8396295: https://dune.com/queries/8396295
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Weekly AMP flows between tracked centralized exchanges and everyone else.
-- withdrawals = AMP leaving exchanges (positive), deposits = AMP arriving on
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
WHERE tr.contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
  AND (ct.address IS NOT NULL OR cf.address IS NOT NULL)
GROUP BY 1
ORDER BY 1
