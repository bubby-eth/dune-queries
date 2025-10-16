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
      contract_address = 0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc
    UNION ALL
    SELECT
      evt_block_time,
      tr."to" AS address,
      CAST(tr.value AS INT256) AS amount,
      contract_address
    FROM
      erc20_ethereum.evt_Transfer AS tr
    WHERE
      contract_address = 0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc
  ),
  price_query AS (
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
  )
SELECT
  CASE
    WHEN address = 0xeFd194D4Ff955E8958d132319F31D2aB9f7E29Ac THEN '🔒 Community Claim'
    WHEN address = 0x5c2Dee0F740dac8185667Ca95308d0Dfd69c635C THEN '🦄 Uniswap v2: ANVL'
    WHEN address = 0x122c2Dd41F10baf6dC9C86B89Ed4F963597d4cC4 THEN '💧 Liquidity Provisioner'
    ELSE CONCAT(
      SUBSTRING(CAST(address AS VARCHAR), 1, 5),
      '...',
      SUBSTRING(CAST(address AS VARCHAR), 39, 42)
    )
  END AS holder,
  SUM(amount) / 1e18 AS balance,
  (SUM(amount) * (SELECT MAX(price) FROM price_query)) / 1e18 AS value
FROM
  transfers AS tr
LEFT JOIN tokens.erc20 AS tok ON tr.contract_address = tok.contract_address
WHERE
  tok.blockchain = 'ethereum'
GROUP BY
  CASE
    WHEN address = 0xeFd194D4Ff955E8958d132319F31D2aB9f7E29Ac THEN '🔒 Community Claim'
    WHEN address = 0x5c2Dee0F740dac8185667Ca95308d0Dfd69c635C THEN '🦄 Uniswap v2: ANVL'
    WHEN address = 0x122c2Dd41F10baf6dC9C86B89Ed4F963597d4cC4 THEN '💧 Liquidity Provisioner'
    ELSE CONCAT(
      SUBSTRING(CAST(address AS VARCHAR), 1, 5),
      '...',
      SUBSTRING(CAST(address AS VARCHAR), 39, 42)
    )
  END
ORDER BY
  balance DESC
LIMIT
  10;