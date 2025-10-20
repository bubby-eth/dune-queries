WITH buys AS (
  SELECT
    DATE_TRUNC('day', block_time) AS date,
    SUM(token_bought_amount) AS "ANVL Bought",
    MAX(amount_usd / token_bought_amount) AS "High Price Buys",
    MIN(amount_usd / token_bought_amount) AS "Low Price Buys"
  FROM
    dex.trades
  WHERE
    block_time > CURRENT_TIMESTAMP - INTERVAL '30' day
    AND token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
  GROUP BY
    1
),
sells AS (
  SELECT
    DATE_TRUNC('day', block_time) AS date,
    SUM(token_sold_amount) AS "ANVL Sold",
    MAX(amount_usd / token_sold_amount) AS "High Price Sells",
    MIN(amount_usd / token_sold_amount) AS "Low Price Sells"
  FROM
    dex.trades
  WHERE
    block_time > CURRENT_TIMESTAMP - INTERVAL '30' day
    AND token_sold_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
  GROUP BY
    1
)
SELECT
  b.date,
  b."ANVL Bought",
  COALESCE(s."ANVL Sold", 0) AS "ANVL Sold",
  GREATEST(b."High Price Buys", s."High Price Sells") AS "High Price",
  LEAST(b."Low Price Buys", s."Low Price Sells") AS "Low Price"
FROM
  buys b
  LEFT JOIN sells s ON b.date = s.date
ORDER BY
  b.date DESC;
