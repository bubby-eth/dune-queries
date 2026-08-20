-- Amp Sushiswap
-- Dune query 1054283: https://dune.com/queries/1054283
-- From dashboard: https://dune.com/ampdotxyz/amp-token

WITH
  prices AS (
    SELECT
      DATE_TRUNC('day', minute) AS day,
      contract_address,
      AVG(price) AS price
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND minute >= CURRENT_TIMESTAMP - INTERVAL '30' day
      AND contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
    GROUP BY
      1,
      2
    ORDER BY
      1 DESC
  ),
  amp_sold AS (
    SELECT
      DATE_TRUNC('day', block_time) AS date,
      SUM(token_sold_amount * a.price) AS "sold",
      project,
      project_contract_address,
      CASE
        WHEN project_contract_address = 0x15e86e6f65ef7ea1dbb72a5e51a07926fb1c82e3 THEN 'ETH-AMP'
        WHEN project_contract_address = 0xa17661e7f2cd96633c25edc5455eb29170105ff0 THEN 'DAI-AMP'
      END AS pair_address
    FROM
      dex."trades"
      LEFT JOIN prices a ON DATE_TRUNC('day', block_time) = a.day
    WHERE
      blockchain = 'ethereum'
      AND block_time > CURRENT_TIMESTAMP - INTERVAL '30' day
      AND token_sold_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
      AND project = 'sushiswap'
      AND (
        project_contract_address = 0x15e86e6f65ef7ea1dbb72a5e51a07926fb1c82e3 --ETH-AMP .3%
        OR project_contract_address = 0xa17661e7f2cd96633c25edc5455eb29170105ff0 --DAI-AMP .3%
      )
    GROUP BY
      1,
      3,
      4,
      5
    ORDER BY
      1 DESC
  ),
  amp_bought AS (
    SELECT
      DATE_TRUNC('day', block_time) AS date,
      SUM(token_bought_amount * a.price) AS "bought",
      project,
      project_contract_address,
      CASE
        WHEN project_contract_address = 0x15e86e6f65ef7ea1dbb72a5e51a07926fb1c82e3 THEN 'ETH-AMP'
        WHEN project_contract_address = 0xa17661e7f2cd96633c25edc5455eb29170105ff0 THEN 'DAI-AMP'
      END AS pair_address
    FROM
      dex."trades"
      LEFT JOIN prices a ON DATE_TRUNC('day', block_time) = a.day
    WHERE
      blockchain = 'ethereum'
      AND block_time > CURRENT_TIMESTAMP - INTERVAL '30' day
      AND token_bought_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
      AND project = 'sushiswap'
      AND (
        project_contract_address = 0x15e86e6f65ef7ea1dbb72a5e51a07926fb1c82e3 --ETH-AMP .3%
        OR project_contract_address = 0xa17661e7f2cd96633c25edc5455eb29170105ff0 --DAI-AMP .3%
      )
    GROUP BY
      1,
      3,
      4,
      5
    ORDER BY
      1 DESC
  )
SELECT
  amp_bought.date as date,
  COALESCE(bought, 0) + COALESCE(sold, 0) AS volume,
  CASE
    WHEN amp_bought.pair_address = 'ETH-AMP' THEN (COALESCE(bought, 0) + COALESCE(sold, 0)) * 0.003
    WHEN amp_bought.pair_address = 'DAI-AMP' THEN (COALESCE(bought, 0) + COALESCE(sold, 0)) * 0.003
  END AS fees,
  amp_bought.pair_address
FROM
  amp_bought
  LEFT JOIN amp_sold ON amp_bought.date = amp_sold.date
  AND amp_bought.project = amp_sold.project
  AND amp_bought.project_contract_address = amp_sold.project_contract_address
ORDER BY
  1 DESC
