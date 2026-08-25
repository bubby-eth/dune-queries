-- ANVL Buys/Sells vs. Price
-- Dune query 3854984: https://dune.com/queries/3854984
-- From dashboard: https://dune.com/anvil/anvil
--
-- Daily ANVL bought and sold on DEXes over the last 30 days with the day's
-- high/low fill price. Single dex.trades scan (previously two); high/low now
-- cover all fills that day, so days with only sells are included too.
WITH fills AS (
  SELECT
    DATE_TRUNC('day', block_time) AS date,
    CASE WHEN token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
         THEN token_bought_amount END AS bought,
    CASE WHEN token_sold_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
         THEN token_sold_amount END AS sold,
    amount_usd / NULLIF(
      CASE WHEN token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
           THEN token_bought_amount ELSE token_sold_amount END, 0) AS price
  FROM
    dex.trades
  WHERE
    block_time > CURRENT_TIMESTAMP - INTERVAL '30' day
    AND (   token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
         OR token_sold_address   = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597)
)
SELECT
  date,
  COALESCE(SUM(bought), 0) AS "ANVL Bought",
  COALESCE(SUM(sold), 0)   AS "ANVL Sold",
  MAX(price)               AS "High Price",
  MIN(price)               AS "Low Price"
FROM
  fills
GROUP BY
  1
ORDER BY
  date DESC;
