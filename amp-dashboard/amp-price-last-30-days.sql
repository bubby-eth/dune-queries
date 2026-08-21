-- Amp Price Last 30 Days
-- Dune query 457085: https://dune.com/queries/457085
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Hourly AMP price over the last 30 days (~720 rows). The previous version
-- returned every minute (~43,000 rows), far more points than the chart can
-- usefully draw. price = hourly close; low/high preserve the intra-hour range.
SELECT
  DATE_TRUNC('hour', minute) AS hour,
  MAX_BY(price, minute)      AS price,
  MIN(price)                 AS price_low,
  MAX(price)                 AS price_high
FROM prices.usd
WHERE blockchain = 'ethereum'
  AND contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
  AND minute >= CURRENT_TIMESTAMP - INTERVAL '30' day
GROUP BY 1
ORDER BY 1 DESC
