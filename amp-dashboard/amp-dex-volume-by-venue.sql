-- Amp DEX Volume by Venue
-- Dune query 8396297: https://dune.com/queries/8396297
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Weekly AMP trading volume in USD split by venue, year to date: Uniswap
-- V2/V3/V4, Sushi, and everything else folded into Other (fixed 5-series
-- cap). Built on dex.trades, so new venues land in Other automatically.
-- Venue colors match the DEX liquidity pie.
SELECT
  CAST(DATE_TRUNC('week', block_time) AS DATE) AS week,
  CASE
    WHEN project = 'uniswap'   AND version = '2' THEN 'Uniswap V2'
    WHEN project = 'uniswap'   AND version = '3' THEN 'Uniswap V3'
    WHEN project = 'uniswap'   AND version = '4' THEN 'Uniswap V4'
    WHEN project = 'sushiswap'                   THEN 'Sushi'
    ELSE 'Other'
  END                                          AS venue,
  SUM(amount_usd)                              AS volume_usd,
  COUNT(*)                                     AS trades
FROM dex.trades
WHERE blockchain = 'ethereum'
  AND block_time >= DATE_TRUNC('year', NOW())
  AND (   token_bought_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
       OR token_sold_address   = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2)
GROUP BY 1, 2
ORDER BY 1, 2
