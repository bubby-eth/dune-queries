-- ANVL DEX Volume by Venue
-- Dune query 8424911: https://dune.com/queries/8424911
-- From dashboard: https://dune.com/anvil/anvil
--
-- Weekly ANVL trading volume in USD split by venue, over the token's full
-- trading history: Uniswap V2/V3/V4, Sushi, and everything else folded into
-- Other (fixed 5-series cap). Built on dex.trades, so new venues land in
-- Other automatically. Today ANVL trades only on Uniswap V4; the venue split
-- future-proofs the chart for new listings.
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
  -- scan bound: ANVL's first DEX trade was 2025-10-08
  AND block_time >= DATE '2025-10-01'
  AND (   token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
       OR token_sold_address   = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597)
GROUP BY 1, 2
ORDER BY 1, 2
