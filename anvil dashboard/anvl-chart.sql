WITH trades AS (
  SELECT
    block_time AS date,
    token_bought_amount AS quantity,
    amount_usd / token_bought_amount AS price,
    'buy' AS type
  FROM
    dex.trades
  WHERE
    block_time > TIMESTAMP '2025-10-08 20:50'
    AND token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
  
  UNION ALL
  
  SELECT
    block_time AS date,
    -token_sold_amount AS quantity,
    amount_usd / token_sold_amount AS price,
    'sell' AS type
  FROM
    dex.trades
  WHERE
    block_time > TIMESTAMP '2025-10-08 20:50'
    AND token_sold_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
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