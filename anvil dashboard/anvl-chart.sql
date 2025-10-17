WITH trades AS (
  SELECT
    block_time AS date,
    token_bought_amount AS quantity,
    amount_usd / token_bought_amount AS price,
    'buy' AS type
  FROM
    dex.trades
  WHERE
    block_time > TIMESTAMP '2024-06-17 13:00:00'
    AND token_bought_address = 0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc
  
  UNION ALL
  
  SELECT
    block_time AS date,
    -token_sold_amount AS quantity,
    amount_usd / token_sold_amount AS price,
    'sell' AS type
  FROM
    dex.trades
  WHERE
    block_time > TIMESTAMP '2024-06-17 13:00:00'
    AND token_sold_address = 0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc
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
  TIMESTAMP '2024-06-17 13:01:00' AS date,
  0 AS quantity,
  0 AS price,
  'none' AS type

ORDER BY
  date DESC;