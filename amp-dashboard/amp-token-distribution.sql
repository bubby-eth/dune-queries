-- Amp Token Distribution
-- Dune query 465058: https://dune.com/queries/465058
-- From dashboard: https://dune.com/ampdotxyz/amp-token

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
      contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
    UNION ALL
    SELECT
      evt_block_time,
      tr."to" AS address,
      Cast(tr.value AS INT256) AS amount,
      contract_address
    FROM
      erc20_ethereum.evt_Transfer AS tr
    WHERE
      contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
  ),
  balances AS (
    SELECT
      CASE
        WHEN address = 0x9eDA92280965832466c15Cd17d66D5E58969FD62 THEN '🔒 Network Development Treasury'
        -- WHEN address = 0x780f9a570c1bec9f2dc761b9031c992cb3e2ae6e THEN '🔒 Network Development Treasury'
        WHEN address = 0x0c3a4a4416562ddccfda34e4fe681569fe60c7bd THEN '🔒 Network Development Treasury'
        WHEN address = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN '🥩 Flexa Capacity V2'
        WHEN address = 0x5d2725fde4d7aa3388da4519ac0449cc031d675f THEN '🥩 Flexa Capacity V3'
        WHEN address = 0xafcd96e580138cfa2332c632e66308eacd45c5da THEN 'Gemini: Storage Wallet'
        WHEN address = 0x46f80018211d5cbbc988e853a8683501fca4ee9b THEN 'BTCTurk: Internal Wallet'
        WHEN address = 0x28c6c06298d514db089934071355e5743bf21d60 THEN 'Binance 14'
        WHEN address = 0xf977814e90da44bfa03b6295a0616a897441acec THEN 'Binance 8'
        WHEN address = 0x5f65f7b609678448494de4c87521cdf6cef1e932 THEN 'Gemini: Hot Wallet'
        WHEN address = 0x8c54ebdd960056d2cff5998df5695daca1fc0190 THEN 'BTCTurk: Hot Wallet'
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
        WHEN CAST(address as varbinary) = 0x0c3a4a4416562ddccfda34e4fe681569fe60c7bd THEN '🔒 Network Development Treasury'
        WHEN CAST(address as varbinary) = 0x780f9a570c1bec9f2dc761b9031c992cb3e2ae6e THEN '🔒 Network Development Treasury'
        WHEN CAST(address as varbinary) = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN '🥩 Flexa Capacity V2'
        WHEN CAST(address as varbinary) = 0x5d2725fde4d7aa3388da4519ac0449cc031d675f THEN '🥩 Flexa Capacity V3'
        WHEN CAST(address as varbinary) = 0xafcd96e580138cfa2332c632e66308eacd45c5da THEN 'Gemini: Storage Wallet'
        WHEN CAST(address as varbinary) = 0x46f80018211d5cbbc988e853a8683501fca4ee9b THEN 'BTCTurk: Internal Wallet'
        WHEN CAST(address as varbinary) = 0x28c6c06298d514db089934071355e5743bf21d60 THEN 'Binance 14'
        WHEN CAST(address as varbinary) = 0xf977814e90da44bfa03b6295a0616a897441acec THEN 'Binance 8'
        WHEN CAST(address as varbinary) = 0x5f65f7b609678448494de4c87521cdf6cef1e932 THEN 'Gemini: Hot Wallet'
        WHEN CAST(address as varbinary) = 0x8c54ebdd960056d2cff5998df5695daca1fc0190 THEN 'BTCTurk: Hot Wallet'
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
  TRY_CAST('All others' AS varchar) AS "address",
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
