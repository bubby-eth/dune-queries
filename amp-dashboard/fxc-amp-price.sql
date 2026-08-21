-- FXC + AMP Price
-- Dune query 461013: https://dune.com/queries/461013
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Combined price history across the FXC -> AMP transition: FXC daily prices
-- from 2019-02-15 until the token swap window closed (2020-09-30), AMP daily
-- prices from 2020-09-29 onward. One scan with one row per day and symbol
-- (the previous version unioned six scans and stacked MIN/MAX/AVG as
-- separate rows).
SELECT
  DATE_TRUNC('day', minute) AS day,
  CASE contract_address
    WHEN 0xfF20817765cB7f73d4bde2e66e067E58D11095C2 THEN 'AMP'
    ELSE 'FXC'
  END                        AS symbol,
  contract_address,
  MIN(price)                 AS price_low,
  MAX(price)                 AS price_high,
  AVG(price)                 AS price_avg,
  MAX_BY(price, minute)      AS price_close
FROM prices.usd
WHERE blockchain = 'ethereum'
  AND (
    (contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
     AND minute > TIMESTAMP '2020-09-29 00:00')
    OR
    (contract_address = 0x4a57E687b9126435a9B19E4A802113e266AdeBde
     AND minute > TIMESTAMP '2019-02-15 00:00'
     AND minute <= TIMESTAMP '2020-09-30 00:00')
  )
GROUP BY 1, 2, 3
ORDER BY 1 DESC
