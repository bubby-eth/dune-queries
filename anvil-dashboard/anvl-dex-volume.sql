-- ANVL DEX Volume
-- Dune query 8424910: https://dune.com/queries/8424910
-- From dashboard: https://dune.com/anvil/anvil
--
-- ANVL trading volume in USD across all DEXes over the last 24 hours, 7 days,
-- and 30 days, with trade counts. Built on the dex.trades spellbook table, so
-- every DEX Dune curates (Uniswap v2/v3/v4, Sushi, Curve, Balancer, ...) is
-- included automatically -- new venues and pools require no query changes.
SELECT
  SUM(CASE WHEN block_time >= NOW() - INTERVAL '1'  day THEN amount_usd END) AS volume_24h_usd,
  SUM(CASE WHEN block_time >= NOW() - INTERVAL '7'  day THEN amount_usd END) AS volume_7d_usd,
  SUM(amount_usd)                                                            AS volume_30d_usd,
  COUNT_IF(block_time >= NOW() - INTERVAL '1' day)                           AS trades_24h,
  COUNT_IF(block_time >= NOW() - INTERVAL '7' day)                           AS trades_7d,
  COUNT(*)                                                                   AS trades_30d
FROM dex.trades
WHERE blockchain = 'ethereum'
  AND (   token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
       OR token_sold_address   = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597)
  AND block_time >= NOW() - INTERVAL '30' day
