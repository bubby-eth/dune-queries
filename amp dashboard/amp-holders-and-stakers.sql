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
                TRY_CAST('<a href="https://debank.com/profile/' AS VARCHAR),
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
        WHEN holder = 0x9eda92280965832466c15cd17d66d5e58969fd62 THEN '🔒 Network Development Treasury'
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
        WHEN holder = 0xdfd5293d8e347dfe59e90efd55b2956a1343963d THEN 'Binance 16'
        WHEN holder = 0xa74e8ae2f83d2564af25420ad4d6a7fe224b053f THEN 'Binance US 9'
        WHEN holder = 0x5b71d5fd6bb118665582dd87922bf3b9de6c75f9 THEN 'Crypto.com 21'
        WHEN holder = 0x2677c4c8757da1857cc7cc4071e0e0dd32ccb975 THEN 'KuCoin 49'
        WHEN holder = 0xab782bc7d4a2b306825de5a7730034f8f63ee1bc THEN 'Bitvavo: Hot 3'
        WHEN holder = 0x21a31ee1afc51d94c2efccaa2092ad1028285549 THEN 'Binance 15'
        WHEN holder = 0x58edf78281334335effa23101bbe3371b6a36a51 THEN 'KuCoin 20'
        WHEN holder = 0xedc6bacdc1e29d7c5fa6f6eca6fdd447b9c487c9 THEN 'Bitvavo: Cold 1'
        WHEN holder = 0x0d0707963952f2fba59dd06f2b425ace40b492fe THEN 'Gate.io: Deposit'
        WHEN holder = 0x9642b23ed1e01df1092b92641051881a322f5d4e THEN 'MEXC 16'
        WHEN holder = 0x98adef6f2ac8572ec48965509d69a8dd5e8bba9d THEN 'Binance 93'
        WHEN holder = 0x72a53cdbbcc1b9efa39c834a540550e23463aacb THEN 'Crypto.com 14'
        WHEN holder = 0x593aebee9117eea447279e5973f64c68d8e977a0 THEN 'Bitstamp 20'
        WHEN holder = 0xb8ba36e591facee901ffd3d5d82df491551ad7ef THEN 'Mercado Bitcoin 1'
        WHEN holder = 0xf60c2ea62edbfe808163751dd0d8693dcb30019c THEN 'Binance US 3'
        WHEN holder = 0x5bdf85216ec1e38d6458c870992a69e38e03f7ef THEN 'Bitget 5'
        WHEN holder = 0x8c7efd5b04331efc618e8006f19019a3dc88973e THEN 'CoinDCX 8'
        WHEN holder = 0xf0bc8fddb1f358cef470d63f96ae65b1d7914953 THEN 'Korbit 8'
        WHEN holder = 0x6cc8dcbca746a6e4fdefb98e1d0df903b107fd21 THEN 'Bitrue'
        WHEN holder = 0x46340b20830761efd32832a74d7169b29feb9758 THEN 'Crypto.com 12'
        WHEN holder = 0xa03400e098f4421b34a3a44a1b4e571419517687 THEN 'HTX 48'
        WHEN holder = 0x24eb3a39856723138796c5068a17ba4fb15cd25e THEN 'Crypto.com 38'
        WHEN holder = 0xd4d2960e1e58a597723ae021cc811193f79153b1 THEN 'CoinEx 20'
        WHEN holder = 0x29065a4c1f2f20d1e263930088890d6f49fe715a THEN 'Poloniex 10'
        WHEN holder = 0x0c97305bd07fc582bcc2042b9a2dedd9f451e75b THEN 'Binance: Deposit'
        WHEN holder = 0x167a9333bf582556f35bd4d16a7e80e191aa6476 THEN 'Coinone 1'
        WHEN holder = 0x7f604d597c15b2e2f60dc645844f68b1d781b752 THEN 'Bitstamp 61'
        WHEN holder = 0x18709e89bd403f470088abdacebe86cc60dda12e THEN 'Huobi: Recovery'
        WHEN holder = 0x8782163068c7cd74d2510768a61135c1e4eb07b3 THEN 'Gate.io: Deposit'
        WHEN holder = 0xa294cca691e4c83b1fc0c8d63d9a3eef0a196de1 THEN 'Fund: 0xa29...de1'
        WHEN holder = 0x4fb312915b779b1339388e14b6d079741ca83128 THEN 'HTX 60'
        WHEN holder = 0x39f6a6c85d39d5abad8a398310c52e7c374f2ba3 THEN 'WhiteBIT'
        WHEN holder = 0x4ed6cf63bd9c009d247ee51224fc1c7041f517f1 THEN 'Ceffu 6'
        WHEN holder = 0x379a27a57d6ae4bbc56f0e8176e8a363ffd61ed3 THEN 'Gate.io: Deposit'
        WHEN holder = 0x18e226459ccf0eec276514a4fd3b226d8961e4d1 THEN 'Binance 107'
        WHEN holder = 0x76ec5a0d3632b2133d9f1980903305b62678fbd3 THEN 'BtcTurk 13'
        WHEN holder = 0xf977814e90da44bfa03b6295a0616a897441acec THEN 'Binance: Hot Wallet 20'
        WHEN holder = 0x77fb357f55bef5a70d30663955f8c9f35794df0e THEN 'eToro 4'
        WHEN holder = 0xc837aa0770eb5131c48549da6599fa8d61130e43 THEN 'Bithumb 411'
        WHEN holder = 0x9b0c45d46d386cedd98873168c36efd0dcba8d46 THEN 'Revolut 3'
        WHEN holder = 0x28c6c06298d514db089934071355e5743bf21d60 THEN 'Binance 14'
        WHEN holder = 0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43 THEN 'Coinbase 10'
        WHEN holder = 0x43684d03d81d3a4c70da68febdd61029d426f042 THEN 'Binance 117'
        WHEN holder = 0x5f65f7b609678448494de4c87521cdf6cef1e932 THEN 'Gemini 4'
        WHEN holder = 0xa023f08c70a23abc7edfc5b6b5e171d78dfc947e THEN 'Crypto.com 22'
        WHEN holder = 0x1157a2076b9bb22a85cc2c162f20fab3898f4101 THEN 'FalconX 1'
        WHEN holder = 0x377b8ce04761754e8ac153b47805a9cf6b190873 THEN 'Upbit 59'
        WHEN holder = 0xcffad3200574698b78f32232aa9d63eabd290703 THEN 'Crypto.com 16'
        WHEN holder = 0x6262998ced04146fa42253a5c0af90ca02dfd2a3 THEN 'Crypto.com 1'
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