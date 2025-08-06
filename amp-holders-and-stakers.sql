WITH
  holder_data AS (
    SELECT
      "from" AS holder,
      - CAST(value AS INT256) AS amount,
      contract_address
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    UNION ALL
    SELECT
      "to" AS holder,
      CAST(value AS INT256) AS amount,
      contract_address
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
  ),
  holders_grouped AS (
    SELECT
      holder,
      SUM(amount) / 1e18 AS balance,
      contract_address
    FROM
      holder_data
    GROUP BY
      1,
      3
    HAVING
      CAST(SUM(amount) AS INT256) > CAST(0 AS INT256)
  ),
  stakers AS (
    SELECT
      "from" AS holder,
      CAST(value AS INT256) AS amount,
      contract_address
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      "to" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    UNION ALL
    SELECT
      "to" AS holder,
      - CAST(value AS INT256) AS amount,
      contract_address
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      "from" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
  ),
  stakers_v1_grouped AS (
    SELECT
      holder,
      SUM(amount) / 1e18 AS staked,
      contract_address
    FROM
      stakers
    GROUP BY
      1,
      3
    HAVING
      CAST(SUM(amount) AS INT256) > CAST(0 AS INT256)
  ),
  stakers_grouped AS(
    SELECT
        "from" AS holder,
        SUM(
            CASE
                WHEN varbinary_substring(data, 1, 4) = 0x3e12170f THEN varbinary_to_uint256(varbinary_substring(data, 37, 32)) / 1e18
                WHEN varbinary_substring(data, 1, 4) = 0x34048584 THEN varbinary_to_uint256(varbinary_substring(data, 69, 32)) / 1e18
                WHEN varbinary_substring(data, 1, 4) = 0xc2a672e0 THEN -varbinary_to_uint256(varbinary_substring(data, 37, 32)) / 1e18
                ELSE 0
            END
        ) AS amount,
        0xff20817765cb7f73d4bde2e66e067e58d11095c2 AS contract_address
    FROM ethereum.transactions
    WHERE
        "to" IN (
            0xd0415cf4558A0dBEE8242498D25284476bE3c8f2,
            0xA52125ced25602203BCeF6E78E865571306CaB2A,
            0xD57E335457b6f5d09ac69248230005a02F9B60CF,
            0xdB07414039F5e1618E3eCC8019C1C1ecb4b4C06A,
            0xE932d1a226E962D820a33363DF32FcC95D2559D2,
            0x9477dA44A61ceBCDD0383CD00Bf18A859FEb75b0,
            0xFF1D02F09A9C55cEFd37f57715FEe7E88278d34e,
            0x59e772F12938063bCa8A2B978791eBe225f5Bc3c,
            0xd80370093a305bbDA27B821bb6c6347989Bf709b,
            0x84706656fabFE15b2b77F292A656dD024607d332,
            0xa7f2B6aF8c536897f246B1EB62654cb9c886FA47,
            0x80E58Fe28F53CCbaD1c295ebAA6A8c13241D034b,
            0x1e73f41454D9806f0462Eb6C9FD2A3754cEE7Fc4,
            0xc163c2cC35e32350Aa92DEC2b53b68950942d72F,
            0x57F6f249DB02083362D43E2D02dD791068Df30C6,
            0xcfBbAE9DCE9a207BaB01E1589e345D3Edc65D842,
            0xCD234A11B26F42B391C2838Beb3DA3Bb3A590B66,
            0xB8706F2dd1Ce8A4328D254cF14271e0fbB5E268A,
            0x1693DeCE45b908Ed25244E8b7FFdE4760cB9Ca24,
            0x603f0200e863784e03cD262bB5266d819DD0eAf0
        )
        AND block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND varbinary_substring(data, 1, 4) IN (0x3e12170f, 0x34048584, 0xc2a672e0)
    GROUP BY 
        1,
        3
  ),
  stakers_grouped_all AS (
    SELECT holder, staked, contract_address FROM stakers_v1_grouped
    UNION ALL
    SELECT holder, amount AS staked, contract_address FROM stakers_grouped
  ),
  tables_combined AS (
    SELECT
        COALESCE(holders_grouped.holder, stakers_grouped_all.holder) AS holder,
        COALESCE(holders_grouped.balance, 0) AS balance,
        COALESCE(stakers_grouped_all.staked, 0) AS staked,
        COALESCE(holders_grouped.balance, 0) + COALESCE(stakers_grouped_all.staked, 0) AS total,
        COALESCE(
        holders_grouped.contract_address,
        stakers_grouped_all.contract_address
        ) AS contract_address
        FROM
        holders_grouped
        FULL OUTER JOIN stakers_grouped_all ON holders_grouped.holder = stakers_grouped_all.holder
    ORDER BY
      4 DESC
  ),
  price_query AS (
    SELECT
      *
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND "contract_address" = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    ORDER BY
      minute DESC
    LIMIT
      1
  ),
  unranked_table AS (
    SELECT
      CONCAT(
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST('<a href="https://zapper.fi/account/' AS VARCHAR),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST(
                  REPLACE(TRY_CAST(holder AS VARCHAR), '\', '0') AS VARCHAR
                ),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(
              COALESCE(TRY_CAST('" target="_blank">' AS VARCHAR), '') AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(
              COALESCE(
                TRY_CAST(
                  REPLACE(TRY_CAST(holder AS VARCHAR), '\', '0') AS VARCHAR
                ),
                ''
              ) AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        ),
        CAST(
          COALESCE(
            CAST(
              COALESCE(TRY_CAST('</a>' AS VARCHAR), '') AS VARCHAR
            ),
            ''
          ) AS VARCHAR
        )
      ) AS holder,
      total,
      balance,
      staked,
      total * price_query.price AS total_value,
      CASE
        WHEN holder = 0x780f9a570c1bec9f2dc761b9031c992cb3e2ae6e THEN '🔒 Network Development Treasury'
        WHEN holder = 0x0c3a4a4416562ddccfda34e4fe681569fe60c7bd THEN '🔒 Network Development Treasury'
        WHEN holder = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN '🥩 Flexa Capacity V2'
        WHEN holder = 0x5d2725fde4d7aa3388da4519ac0449cc031d675f THEN '🥩 Flexa Capacity V3'
        WHEN holder = 0xafcd96e580138cfa2332c632e66308eacd45c5da THEN 'Gemini: Storage Wallet'
        WHEN holder = 0x46f80018211d5cbbc988e853a8683501fca4ee9b THEN 'BTCTurk: Internal Wallet'
        WHEN holder = 0x8c54ebdd960056d2cff5998df5695daca1fc0190 THEN 'BTCTurk: Hot Wallet'
        WHEN holder = 0x0548f59fee79f8832c299e01dca5c76f034f558e THEN 'Genesis Trading: OTC'
        WHEN holder = 0xf3b0073e3a7f747c7a38b36b805247b222c302a3 THEN 'Crypto.com: Internal Wallet'
        WHEN holder = 0x75c80ce8fddfc61641bed16cd90c9123f0d9a020 THEN 'Uniswap: V3 USDC-AMP (1.0%) Liquidity Pool'
        WHEN holder = 0x5e8fb38b9a04dbc8e6488136e69ab40df3b3012f THEN 'GnosisSafeProxy'
        WHEN holder = 0x48ec5560bfd59b95859965cce48cc244cfdf6b0c THEN 'Bitstamp: Multisig Wallet'
        WHEN holder = 0xf584f8728b874a6a5c7a8d4d387c9aae9172d621 THEN 'Jump Trading'
        WHEN holder = 0x27fd43babfbe83a81d14665b1a6fb8030a60c9b4 THEN 'WazirX'
        WHEN holder = 0x3c02290922a3618a4646e3bbca65853ea45fe7c6 THEN 'Indodax'
        WHEN holder = 0xceb69f6342ece283b2f5c9088ff249b5d0ae66ea THEN 'Coinbase Custody: Hot Wallet'
        WHEN holder = 0x30741289523c2e4d2a62c7d6722686d14e723851 THEN 'Huobi'
        WHEN holder = 0x15e86e6f65ef7ea1dbb72a5e51a07926fb1c82e3 THEN 'SushiSwap: WETH-AMP Liquidity Pool'
        WHEN holder = 0x500a746c9a44f68fe6aa86a92e7b3af4f322ae66 THEN 'Voyager: Hot Wallet'
        WHEN holder = 0x08650bb9dc722c9c8c62e79c2bafa2d3fc5b3293 THEN 'Uniswap: V2 WETH-AMP Liquidity Pool'
        WHEN holder = 0xe74b28c2eae8679e3ccc3a94d5d0de83ccb84705 THEN 'Wintermute Exploiter: Wallet'
        WHEN holder = 0xdb6fdc30ab61c7cca742d4c13d1b035f3f82019a THEN 'Coinspot'
        WHEN holder = 0x64c5f5e2a6a719818930f85fa56c2010fd0e5336 THEN 'Binance US: Deposit'
        WHEN holder = 0x2407b9b9662d970ece2224a0403d3b15c7e4d1fe THEN 'CoinDCX'
        WHEN holder = 0x446b86a33e2a438f569b15855189e3da28d027ba THEN 'ERC1967Proxy'
        WHEN holder = 0x601a63c50448477310fedb826ed0295499baf623 THEN 'CoinEx'
        WHEN holder = 0x601a63c50448477310fedb826ed0295499baf623 THEN 'CoinEx'
        WHEN holder = 0x91dca37856240e5e1906222ec79278b16420dc92 THEN 'Indodax'
        WHEN holder = 0x674bdf20a0f284d710bc40872100128e2d66bd3f THEN 'Loopring: Default Deposit Contract'
        WHEN holder = 0x6a74941c1cf4151b3f15cdd84ee3abde713a999b THEN 'Bancor: AMP-BNT Liquidity Pool'
        WHEN holder = 0xff20817765cb7f73d4bde2e66e067e58d11095c2 THEN 'Amp: AMP Token Contract'
        WHEN holder = 0xf02e86d9e0efd57ad034faf52201b79917fe0713 THEN 'Alameda Research'
        WHEN holder = 0x649765821d9f64198c905ec0b2b037a4a52bc373 THEN 'Bancor: Master Vault'
        WHEN holder = 0x4f24e16f76ed603a29bca952d454333c431f4e31 THEN 'Gate.io: Deposit'
        WHEN holder = 0x1c727a55ea3c11b0ab7d3a361fe0f3c47ce6de5d THEN 'Uphold.com'
        WHEN holder = 0xf5bce5077908a1b7370b9ae04adc565ebd643966 THEN 'SushiSwap: BentoBoxV1'
        WHEN holder = 0xe567d569be19dd911937d90c912d538d48f270bc THEN 'Huobi: Deposit'
        WHEN holder = 0x39bad71bef4bb210b0dad17a3c66222d38c9ed93 THEN 'Binance US: Deposit'
        WHEN holder = 0x395265ca755f7bb7a8c5c89241d1b0178727163e THEN 'Huobi: Deposit'
        WHEN holder = 0x4e57f830b0b4a82321071ead6ffd1df1575a16e2 THEN 'Uniswap: V3 WETH-AMP (1.0%) Liquidity Pool'
        WHEN holder = 0xa2d20aae9fdacdd153753536e579904b91064bf6 THEN 'Gate.io: Deposit'
        WHEN holder = 0xf22981c5bf0a717c98781af04fdc8213fa789f1c THEN 'Jump Trading'
        WHEN holder = 0x120051a72966950b8ce12eb5496b5d1eeec1541b THEN 'LBank'
        WHEN holder = 0x0c6328224eb7f80ed1b5363a377725e2af06e6d9 THEN 'Binance US: Deposit'
        WHEN holder = 0x71127c1044086e8f4bc6056d3e085f97922df75a THEN 'Gate.io: Deposit'
        WHEN holder = 0x6f892f58e08fe670ef1d61c27c74268810031184 THEN 'Gate.io: Deposit'
        WHEN holder = 0x3ab28ecedea6cdb6feed398e93ae8c7b316b1182 THEN 'BitMart'
        WHEN holder = 0x2bfb4dfdefcc4a72f03b0a3464eb2eafa2633583 THEN 'Huobi: Deposit'
        WHEN holder = 0xe5c405c5578d84c5231d3a9a29ef4374423fa0c2 THEN 'IDEX: Custodian'
        WHEN holder = 0xb8001c3ec9aa1985f6c747e25c28324e4a361ec1 THEN 'Cobo'
        WHEN holder = 0xeb671e6944c6e90556287c36e91a7c03f6392e91 THEN 'KuCoin: Deposit'
        WHEN holder = 0x0e394d3facf0ce3bd5fcce584e16e0cbac164346 THEN 'ZB.COM'
        WHEN holder = 0xff7e812e5ad47b00e12dacd2fd8883a5258c53f5 THEN 'Binance US: Deposit'
        WHEN holder = 0xff7e812e5ad47b00e12dacd2fd8883a5258c53f5 THEN 'Binance US: Deposit'
        WHEN holder = 0x4f4f73b6f130921b0c5ff8db29d5c1520e214d69 THEN 'Gate.io: Deposit'
        WHEN holder = 0x8aca98f082b88e7516bdfc110a12eb521cb8ddbe THEN 'Binance US: Deposit'
        WHEN cex.distinct_name IS NOT NULL THEN cex.distinct_name
        WHEN total >= 1000000000 THEN '🐋'
        WHEN total >= 500000000 THEN '🐳'
        WHEN total >= 250000000 THEN '🦈'
        WHEN total >= 100000000 THEN '🐬'
        WHEN total >= 50000000 THEN '🦭'
        WHEN total >= 25000000 THEN '🐙'
        WHEN total >= 10000000 THEN '🐡'
        WHEN total >= 5000000 THEN '🐠'
        WHEN total >= 2500000 THEN '🐟'
        ELSE '🐌'
      END AS "Label"
    FROM
      tables_combined
      LEFT JOIN price_query ON price_query.contract_address = tables_combined.contract_address
      LEFT JOIN cex.addresses AS cex ON cex.address = tables_combined.holder
      AND cex.blockchain = 'ethereum'
  )
SELECT
  ROW_NUMBER() OVER (
    ORDER BY
      total DESC
  ) AS "rank",
  *
FROM
  unranked_table
ORDER BY
  "rank"