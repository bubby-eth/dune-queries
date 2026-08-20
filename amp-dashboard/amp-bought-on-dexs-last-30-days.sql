-- Amp Bought on DEXs Last 30 Days
-- Dune query 457147: https://dune.com/queries/457147
-- From dashboard: https://dune.com/ampdotxyz/amp-token

SELECT
  DATE_TRUNC('day', block_time) AS date,
  SUM(token_bought_amount) AS "AMP Bought"
FROM
  dex."trades"
WHERE
  blockchain = 'ethereum'
  AND block_time > CURRENT_TIMESTAMP - INTERVAL '30' day
  AND token_bought_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
GROUP BY
  1
ORDER BY
  1 DESC
