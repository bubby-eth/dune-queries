WITH
  transfers AS (
    SELECT
      evt_block_time,
      tr."from" AS address,
      -CAST(tr.value AS INT256) AS amount,
      contract_address
    FROM
      erc20_ethereum.evt_Transfer AS tr
    WHERE
      contract_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597

    UNION ALL

    SELECT
      evt_block_time,
      tr."to" AS address,
      CAST(tr.value AS INT256) AS amount,
      contract_address
    FROM
      erc20_ethereum.evt_Transfer AS tr
    WHERE
      contract_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
  ),

  price_query AS (
    SELECT
      amount_usd / token_bought_amount AS price
    FROM
      dex.trades
    WHERE
      token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
      AND amount_usd IS NOT NULL
      AND amount_usd <> 0
    ORDER BY
      block_time DESC
    LIMIT 1
  )

SELECT
  CONCAT(
    SUBSTRING(CAST(address AS VARCHAR), 1, 5),
    '...',
    SUBSTRING(CAST(address AS VARCHAR), 39, 42)
  ) AS holder,
  SUM(amount) / 1e18 AS balance,
  (SUM(amount) * (SELECT MAX(price) FROM price_query)) / 1e18 AS value
FROM
  transfers AS tr
LEFT JOIN tokens.erc20 AS tok ON tr.contract_address = tok.contract_address
WHERE
  tok.blockchain = 'ethereum'
GROUP BY
  address
ORDER BY
  balance DESC
LIMIT 10;