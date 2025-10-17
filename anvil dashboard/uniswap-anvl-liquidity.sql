WITH weth_price AS (
  SELECT price
  FROM prices.usd_latest
  WHERE symbol = 'WETH'
  ORDER BY minute DESC
  LIMIT 1
),
anvl_price AS (
  SELECT
    amount_usd / token_bought_amount AS price
  FROM
    dex.trades
  WHERE
    token_bought_address = 0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc
    AND amount_usd IS NOT NULL
    AND amount_usd <> 0
  ORDER BY
    block_time DESC
  LIMIT
    1
),
main_query AS (
  SELECT
    b.block_time,
    b.token_symbol,
    b.balance,
    CASE
      WHEN b.token_symbol = 'WETH' THEN b.balance * wp.price
      WHEN b.token_symbol = 'ANVL' THEN b.balance * ap.price
      ELSE NULL
    END AS value
  FROM
    tokens_ethereum.balances b
  LEFT JOIN
    weth_price wp ON b.token_symbol = 'WETH'
  LEFT JOIN
    anvl_price ap ON b.token_symbol = 'ANVL'
  WHERE
    b.address = 0x5c2dee0f740dac8185667ca95308d0dfd69c635c
    AND b.block_time > TIMESTAMP '2024-06-17 13:00'an
  ORDER BY
    b.block_time DESC, b.token_symbol = 'ANVL' DESC
  LIMIT
    2
)

SELECT
  mq.*,
  (SELECT SUM(value) FROM main_query) AS total_liquidity
FROM
  main_query mq;