WITH
  transfers AS (
    SELECT
      evt_block_time,
      tr."from" AS address,
      - Cast(tr.value AS INT256) AS amount,
      contract_address
    FROM
      erc20_ethereum.evt_Transfer AS tr
    WHERE
      contract_address = 0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc
    UNION ALL
    SELECT
      evt_block_time,
      tr."to" AS address,
      Cast(tr.value AS INT256) AS amount,
      contract_address
    FROM
      erc20_ethereum.evt_Transfer AS tr
    WHERE
      contract_address = 0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc
  ),
  balances AS (
    SELECT
      CASE
        WHEN address = 0xeFd194D4Ff955E8958d132319F31D2aB9f7E29Ac THEN '🔒 Commuinty Claim'
        WHEN address = 0x5c2Dee0F740dac8185667Ca95308d0Dfd69c635C THEN '🦄 Uniswap v2: ANVL'
        WHEN address = 0x122c2Dd41F10baf6dC9C86B89Ed4F963597d4cC4 THEN '💧 Liquidity Provisioner'
        ELSE Concat(
          substring(CAST(address as varchar), 1, 5),
          '...',
          substring(CAST(address as varchar), 39, 42)
        )
      END AS address,
      Sum(amount) / 1e18 AS balance
    FROM
      transfers tr
      LEFT JOIN tokens.erc20 AS tok ON tr.contract_address = tok.contract_address
    GROUP BY
      1
    ORDER BY
      2 DESC
  ),
  top10 AS (
    SELECT
      CASE
        WHEN CAST(address as varbinary) = 0xeFd194D4Ff955E8958d132319F31D2aB9f7E29Ac THEN '🔒 Commuinty Claim'
        WHEN CAST(address as varbinary) = 0x5c2Dee0F740dac8185667Ca95308d0Dfd69c635C THEN '🦄 Uniswap v2: ANVL'
        WHEN CAST(address as varbinary) = 0x122c2Dd41F10baf6dC9C86B89Ed4F963597d4cC4 THEN '💧 Liquidity Provisioner'
        ELSE CAST(address as varchar)
      END AS address_alias,
      balance
    FROM
      balances
    ORDER BY
      2 DESC
    limit
      10
  )
SELECT
  *
FROM
  top10
UNION ALL
SELECT
  TRY_CAST('🌎 All Others' AS varchar) AS "address",
  sum(balance) AS balance
FROM
  balances
WHERE
  NOT TRY_CAST(address AS varchar) IN (
    SELECT
      TRY_CAST(address_alias AS varchar) AS address
    FROM
      top10
  )
  AND balance > 0