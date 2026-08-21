-- Amp Price
-- Dune query 456965: https://dune.com/queries/456965
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Daily AMP price history: low / high / average / close per day in a single
-- scan (the previous version scanned prices.usd twice and stacked MIN and MAX
-- as separate rows).
SELECT
  DATE_TRUNC('day', minute)  AS day,
  MIN(price)                 AS price_low,
  MAX(price)                 AS price_high,
  AVG(price)                 AS price_avg,
  MAX_BY(price, minute)      AS price_close
FROM prices.usd
WHERE blockchain = 'ethereum'
  AND contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
  AND minute > TIMESTAMP '2020-09-29 00:00'
GROUP BY 1
ORDER BY 1 DESC
