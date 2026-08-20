-- Amp Bought vs Amp Sold on DEXs
-- Dune query 698599: https://dune.com/queries/698599
-- From dashboard: https://dune.com/ampdotxyz/amp-token

WITH
  amp_sold AS (
    SELECT
      DATE_TRUNC('day', block_time) AS date,
      SUM(token_sold_amount) AS "sold"
    FROM
      dex."trades"
    WHERE
      blockchain = 'ethereum'
      AND block_time > CURRENT_TIMESTAMP - INTERVAL '7' day
      AND token_sold_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
    GROUP BY
      1
    ORDER BY
      1 DESC
  ),
  amp_bought AS (
    SELECT
      DATE_TRUNC('day', block_time) AS date,
      SUM(token_bought_amount) AS "bought"
    FROM
      dex."trades"
    WHERE
      blockchain = 'ethereum'
      AND block_time > CURRENT_TIMESTAMP - INTERVAL '7' day
      AND token_bought_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
    GROUP BY
      1
    ORDER BY
      1 DESC
  )
SELECT
  *
FROM
  amp_bought
  LEFT JOIN amp_sold ON amp_bought.date = amp_sold.date
ORDER BY
  1 DESC
