-- Amp Price
-- Dune query 456965: https://dune.com/queries/456965
-- From dashboard: https://dune.com/ampdotxyz/amp-token

WITH
  prices AS (
    SELECT
      DATE_TRUNC('day', minute) AS day,
      contract_address,
      MAX(price) AS price
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND minute > CAST('2020-09-29 00:00' AS TIMESTAMP)
      AND contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
    GROUP BY
      1,
      2
    UNION ALL
    SELECT
      DATE_TRUNC('day', minute) AS day,
      contract_address,
      MIN(price) AS price
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND minute > CAST('2020-09-29 00:00' AS TIMESTAMP)
      AND contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
    GROUP BY
      1,
      2
  )
SELECT
  *
FROM
  PRICES
ORDER BY
  1 DESC
