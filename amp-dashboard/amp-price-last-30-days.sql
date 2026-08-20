-- Amp Price Last 30 Days
-- Dune query 457085: https://dune.com/queries/457085
-- From dashboard: https://dune.com/ampdotxyz/amp-token

SELECT
  minute,
  contract_address,
  price
FROM
  prices.usd
WHERE
  blockchain = 'ethereum'
  AND minute >= CURRENT_TIMESTAMP - INTERVAL '30' day
  AND contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
ORDER BY
  1 DESC
