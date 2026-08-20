-- FXC + AMP Price
-- Dune query 461013: https://dune.com/queries/461013
-- From dashboard: https://dune.com/ampdotxyz/amp-token

WITH
  prices AS (
    SELECT
      DATE_TRUNC('day', minute) AS day,
      contract_address,
      MAX(price) AS price,
      CASE
        WHEN contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2 THEN 'AMP'
        WHEN contract_address = 0x4a57E687b9126435a9B19E4A802113e266AdeBde THEN 'FXC'
      END AS symbol
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND minute > CAST('2020-09-29 00:00' AS TIMESTAMP)
      AND contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
    GROUP BY
      1,
      2,
      4
    UNION ALL
    SELECT
      DATE_TRUNC('day', minute) AS day,
      contract_address,
      MIN(price) AS price,
      CASE
        WHEN contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2 THEN 'AMP'
        WHEN contract_address = 0x4a57E687b9126435a9B19E4A802113e266AdeBde THEN 'FXC'
      END AS symbol
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND minute > CAST('2020-09-29 00:00' AS TIMESTAMP)
      AND contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
    GROUP BY
      1,
      2,
      4
    UNION ALL
    SELECT
      DATE_TRUNC('day', minute) AS day,
      contract_address,
      AVG(price) AS price,
      CASE
        WHEN contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2 THEN 'AMP'
        WHEN contract_address = 0x4a57E687b9126435a9B19E4A802113e266AdeBde THEN 'FXC'
      END AS symbol
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND minute > CAST('2020-09-29 00:00' AS TIMESTAMP)
      AND contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
    GROUP BY
      1,
      2,
      4
    UNION ALL
    SELECT
      DATE_TRUNC('day', minute) AS day,
      contract_address,
      MAX(price) AS price,
      CASE
        WHEN contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2 THEN 'AMP'
        WHEN contract_address = 0x4a57E687b9126435a9B19E4A802113e266AdeBde THEN 'FXC'
      END AS symbol
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND minute <= CAST('2020-09-30 00:00' AS TIMESTAMP)
      AND minute > CAST('2019-02-15 00:00' AS TIMESTAMP)
      AND contract_address = 0x4a57E687b9126435a9B19E4A802113e266AdeBde
    GROUP BY
      1,
      2,
      4
    UNION ALL
    SELECT
      DATE_TRUNC('day', minute) AS day,
      contract_address,
      MIN(price) AS price,
      CASE
        WHEN contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2 THEN 'AMP'
        WHEN contract_address = 0x4a57E687b9126435a9B19E4A802113e266AdeBde THEN 'FXC'
      END AS symbol
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND minute <= CAST('2020-09-30 00:00' AS TIMESTAMP)
      AND minute > CAST('2019-02-15 00:00' AS TIMESTAMP)
      AND contract_address = 0x4a57E687b9126435a9B19E4A802113e266AdeBde
    GROUP BY
      1,
      2,
      4
    UNION ALL
    SELECT
      DATE_TRUNC('day', minute) AS day,
      contract_address,
      AVG(price) AS price,
      CASE
        WHEN contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2 THEN 'AMP'
        WHEN contract_address = 0x4a57E687b9126435a9B19E4A802113e266AdeBde THEN 'FXC'
      END AS symbol
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND minute <= CAST('2020-09-30 00:00' AS TIMESTAMP)
      AND minute > CAST('2019-02-15 00:00' AS TIMESTAMP)
      AND contract_address = 0x4a57E687b9126435a9B19E4A802113e266AdeBde
    GROUP BY
      1,
      2,
      4
  )
SELECT
  *
FROM
  PRICES
ORDER BY
  1 DESC
