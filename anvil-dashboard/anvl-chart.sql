-- ANVL Chart
-- Dune query 3846010: https://dune.com/queries/3846010
-- From dashboard: https://dune.com/anvil/anvil
--
-- Every ANVL DEX trade since launch: positive quantity = buy, negative =
-- sell, with the fill price. Single dex.trades scan (a trade is a buy or a
-- sell of ANVL, never both). The sentinel row anchors the chart's time axis
-- at launch.
WITH trades AS (
  SELECT
    block_time AS date,
    CASE WHEN token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
         THEN token_bought_amount ELSE -token_sold_amount END AS quantity,
    amount_usd / NULLIF(
      CASE WHEN token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
           THEN token_bought_amount ELSE token_sold_amount END, 0) AS price,
    CASE WHEN token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
         THEN 'buy' ELSE 'sell' END AS type
  FROM
    dex.trades
  WHERE
    block_time > TIMESTAMP '2025-10-08 20:50'
    AND (   token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
         OR token_sold_address   = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597)
)
SELECT
  date,
  quantity,
  price,
  type
FROM
  trades

UNION ALL

SELECT
  TIMESTAMP '2025-10-08 20:50' AS date,
  0 AS quantity,
  0 AS price,
  'none' AS type

ORDER BY
  date DESC;
