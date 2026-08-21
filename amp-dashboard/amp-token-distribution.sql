-- Amp Token Distribution
-- Dune query 465058: https://dune.com/queries/465058
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Top 10 AMP holders (by label; addresses sharing a label are combined) plus
-- "All others". Labels come from the shared curated map (same list as the
-- Amp Holders & Stakers query) with cex.addresses as fallback; unlabeled
-- addresses show as shortened hex.
WITH
  -- net AMP balance per address (single transfer scan, +to / -from)
  balances AS (
    SELECT f.holder AS address, SUM(f.amount) / 1e18 AS balance
    FROM erc20_ethereum.evt_Transfer tr
    CROSS JOIN UNNEST(
      ARRAY[
        ROW(tr."from", -CAST(tr.value AS int256)),
        ROW(tr."to",    CAST(tr.value AS int256))
      ]
    ) AS f(holder, amount)
    WHERE tr.contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND f.holder <> 0x0000000000000000000000000000000000000000
    GROUP BY 1
    HAVING SUM(f.amount) > CAST(0 AS int256)
  ),

  -- curated address labels (shared with the Amp Holders & Stakers query)
  label_map (addr, label) AS (
    VALUES
      (0x706d7f8b3445d8dfc790c524e3990ef014e7c578, '🥩 Flexa Capacity V2'),
      (0x5d2725fde4d7aa3388da4519ac0449cc031d675f, '🥩 Flexa Capacity V3'),
      (0xafcd96e580138cfa2332c632e66308eacd45c5da, 'Gemini: Storage Wallet'),
      (0x46f80018211d5cbbc988e853a8683501fca4ee9b, 'BTCTurk: Internal Wallet'),
      (0x8c54ebdd960056d2cff5998df5695daca1fc0190, 'BTCTurk: Hot Wallet'),
      (0x0548f59fee79f8832c299e01dca5c76f034f558e, 'Genesis Trading: OTC'),
      (0xf3b0073e3a7f747c7a38b36b805247b222c302a3, 'Crypto.com: Internal Wallet'),
      (0x75c80ce8fddfc61641bed16cd90c9123f0d9a020, 'Uniswap: V3 USDC-AMP (1.0%) Liquidity Pool'),
      (0x5e8fb38b9a04dbc8e6488136e69ab40df3b3012f, 'GnosisSafeProxy'),
      (0x48ec5560bfd59b95859965cce48cc244cfdf6b0c, 'Bitstamp: Multisig Wallet'),
      (0xf584f8728b874a6a5c7a8d4d387c9aae9172d621, 'Jump Trading'),
      (0x27fd43babfbe83a81d14665b1a6fb8030a60c9b4, 'WazirX'),
      (0x3c02290922a3618a4646e3bbca65853ea45fe7c6, 'Indodax'),
      (0xceb69f6342ece283b2f5c9088ff249b5d0ae66ea, 'Coinbase Custody: Hot Wallet'),
      (0x30741289523c2e4d2a62c7d6722686d14e723851, 'Huobi'),
      (0x15e86e6f65ef7ea1dbb72a5e51a07926fb1c82e3, 'SushiSwap: WETH-AMP Liquidity Pool'),
      (0x500a746c9a44f68fe6aa86a92e7b3af4f322ae66, 'Voyager: Hot Wallet'),
      (0x08650bb9dc722c9c8c62e79c2bafa2d3fc5b3293, 'Uniswap: V2 WETH-AMP Liquidity Pool'),
      (0xe74b28c2eae8679e3ccc3a94d5d0de83ccb84705, 'Wintermute Exploiter: Wallet'),
      (0xdb6fdc30ab61c7cca742d4c13d1b035f3f82019a, 'Coinspot'),
      (0x64c5f5e2a6a719818930f85fa56c2010fd0e5336, 'Binance US: Deposit'),
      (0x2407b9b9662d970ece2224a0403d3b15c7e4d1fe, 'CoinDCX'),
      (0x446b86a33e2a438f569b15855189e3da28d027ba, 'ERC1967Proxy'),
      (0x601a63c50448477310fedb826ed0295499baf623, 'CoinEx'),
      (0x91dca37856240e5e1906222ec79278b16420dc92, 'Indodax'),
      (0x674bdf20a0f284d710bc40872100128e2d66bd3f, 'Loopring: Default Deposit Contract'),
      (0x6a74941c1cf4151b3f15cdd84ee3abde713a999b, 'Bancor: AMP-BNT Liquidity Pool'),
      (0xff20817765cb7f73d4bde2e66e067e58d11095c2, 'Amp: AMP Token Contract'),
      (0xf02e86d9e0efd57ad034faf52201b79917fe0713, 'Alameda Research'),
      (0x649765821d9f64198c905ec0b2b037a4a52bc373, 'Bancor: Master Vault'),
      (0x4f24e16f76ed603a29bca952d454333c431f4e31, 'Gate.io: Deposit'),
      (0x1c727a55ea3c11b0ab7d3a361fe0f3c47ce6de5d, 'Uphold.com'),
      (0xf5bce5077908a1b7370b9ae04adc565ebd643966, 'SushiSwap: BentoBoxV1'),
      (0xe567d569be19dd911937d90c912d538d48f270bc, 'Huobi: Deposit'),
      (0x39bad71bef4bb210b0dad17a3c66222d38c9ed93, 'Binance US: Deposit'),
      (0x395265ca755f7bb7a8c5c89241d1b0178727163e, 'Huobi: Deposit'),
      (0x4e57f830b0b4a82321071ead6ffd1df1575a16e2, 'Uniswap: V3 WETH-AMP (1.0%) Liquidity Pool'),
      (0xa2d20aae9fdacdd153753536e579904b91064bf6, 'Gate.io: Deposit'),
      (0xf22981c5bf0a717c98781af04fdc8213fa789f1c, 'Jump Trading'),
      (0x120051a72966950b8ce12eb5496b5d1eeec1541b, 'LBank'),
      (0x0c6328224eb7f80ed1b5363a377725e2af06e6d9, 'Binance US: Deposit'),
      (0x71127c1044086e8f4bc6056d3e085f97922df75a, 'Gate.io: Deposit'),
      (0x6f892f58e08fe670ef1d61c27c74268810031184, 'Gate.io: Deposit'),
      (0x3ab28ecedea6cdb6feed398e93ae8c7b316b1182, 'BitMart'),
      (0x2bfb4dfdefcc4a72f03b0a3464eb2eafa2633583, 'Huobi: Deposit'),
      (0xe5c405c5578d84c5231d3a9a29ef4374423fa0c2, 'IDEX: Custodian'),
      (0xb8001c3ec9aa1985f6c747e25c28324e4a361ec1, 'Cobo'),
      (0xeb671e6944c6e90556287c36e91a7c03f6392e91, 'KuCoin: Deposit'),
      (0x0e394d3facf0ce3bd5fcce584e16e0cbac164346, 'ZB.COM'),
      (0xff7e812e5ad47b00e12dacd2fd8883a5258c53f5, 'Binance US: Deposit'),
      (0x4f4f73b6f130921b0c5ff8db29d5c1520e214d69, 'Gate.io: Deposit'),
      (0x8aca98f082b88e7516bdfc110a12eb521cb8ddbe, 'Binance US: Deposit'),
      (0xdfd5293d8e347dfe59e90efd55b2956a1343963d, 'Binance 16'),
      (0xa74e8ae2f83d2564af25420ad4d6a7fe224b053f, 'Binance US 9'),
      (0x5b71d5fd6bb118665582dd87922bf3b9de6c75f9, 'Crypto.com 21'),
      (0x2677c4c8757da1857cc7cc4071e0e0dd32ccb975, 'KuCoin 49'),
      (0xab782bc7d4a2b306825de5a7730034f8f63ee1bc, 'Bitvavo: Hot 3'),
      (0x21a31ee1afc51d94c2efccaa2092ad1028285549, 'Binance 15'),
      (0x58edf78281334335effa23101bbe3371b6a36a51, 'KuCoin 20'),
      (0xedc6bacdc1e29d7c5fa6f6eca6fdd447b9c487c9, 'Bitvavo: Cold 1'),
      (0x0d0707963952f2fba59dd06f2b425ace40b492fe, 'Gate.io: Deposit'),
      (0x9642b23ed1e01df1092b92641051881a322f5d4e, 'MEXC 16'),
      (0x98adef6f2ac8572ec48965509d69a8dd5e8bba9d, 'Binance 93'),
      (0x72a53cdbbcc1b9efa39c834a540550e23463aacb, 'Crypto.com 14'),
      (0x593aebee9117eea447279e5973f64c68d8e977a0, 'Bitstamp 20'),
      (0xb8ba36e591facee901ffd3d5d82df491551ad7ef, 'Mercado Bitcoin 1'),
      (0xf60c2ea62edbfe808163751dd0d8693dcb30019c, 'Binance US 3'),
      (0x5bdf85216ec1e38d6458c870992a69e38e03f7ef, 'Bitget 5'),
      (0x8c7efd5b04331efc618e8006f19019a3dc88973e, 'CoinDCX 8'),
      (0xf0bc8fddb1f358cef470d63f96ae65b1d7914953, 'Korbit 8'),
      (0x6cc8dcbca746a6e4fdefb98e1d0df903b107fd21, 'Bitrue'),
      (0x46340b20830761efd32832a74d7169b29feb9758, 'Crypto.com 12'),
      (0xa03400e098f4421b34a3a44a1b4e571419517687, 'HTX 48'),
      (0x24eb3a39856723138796c5068a17ba4fb15cd25e, 'Crypto.com 38'),
      (0xd4d2960e1e58a597723ae021cc811193f79153b1, 'CoinEx 20'),
      (0x29065a4c1f2f20d1e263930088890d6f49fe715a, 'Poloniex 10'),
      (0x0c97305bd07fc582bcc2042b9a2dedd9f451e75b, 'Binance: Deposit'),
      (0x167a9333bf582556f35bd4d16a7e80e191aa6476, 'Coinone 1'),
      (0x7f604d597c15b2e2f60dc645844f68b1d781b752, 'Bitstamp 61'),
      (0x18709e89bd403f470088abdacebe86cc60dda12e, 'Huobi: Recovery'),
      (0x8782163068c7cd74d2510768a61135c1e4eb07b3, 'Gate.io: Deposit'),
      (0xa294cca691e4c83b1fc0c8d63d9a3eef0a196de1, 'Fund: 0xa29...de1'),
      (0x4fb312915b779b1339388e14b6d079741ca83128, 'HTX 60'),
      (0x39f6a6c85d39d5abad8a398310c52e7c374f2ba3, 'WhiteBIT'),
      (0x4ed6cf63bd9c009d247ee51224fc1c7041f517f1, 'Ceffu 6'),
      (0x379a27a57d6ae4bbc56f0e8176e8a363ffd61ed3, 'Gate.io: Deposit'),
      (0x18e226459ccf0eec276514a4fd3b226d8961e4d1, 'Binance 107'),
      (0x76ec5a0d3632b2133d9f1980903305b62678fbd3, 'BtcTurk 13'),
      (0xf977814e90da44bfa03b6295a0616a897441acec, 'Binance: Hot Wallet 20'),
      (0x77fb357f55bef5a70d30663955f8c9f35794df0e, 'eToro 4'),
      (0xc837aa0770eb5131c48549da6599fa8d61130e43, 'Bithumb 411'),
      (0x9b0c45d46d386cedd98873168c36efd0dcba8d46, 'Revolut 3'),
      (0x28c6c06298d514db089934071355e5743bf21d60, 'Binance 14'),
      (0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43, 'Coinbase 10'),
      (0x43684d03d81d3a4c70da68febdd61029d426f042, 'Binance 117'),
      (0x5f65f7b609678448494de4c87521cdf6cef1e932, 'Gemini 4'),
      (0xa023f08c70a23abc7edfc5b6b5e171d78dfc947e, 'Crypto.com 22'),
      (0x1157a2076b9bb22a85cc2c162f20fab3898f4101, 'FalconX 1'),
      (0x377b8ce04761754e8ac153b47805a9cf6b190873, 'Upbit 59'),
      (0xcffad3200574698b78f32232aa9d63eabd290703, 'Crypto.com 16'),
      (0x6262998ced04146fa42253a5c0af90ca02dfd2a3, 'Crypto.com 1')
  ),

  -- one row per label (addresses sharing a label are summed)
  labeled AS (
    SELECT
      COALESCE(
        lm.label,
        cex.distinct_name,
        CONCAT(
          substring(CAST(b.address AS varchar), 1, 5),
          '...',
          substring(CAST(b.address AS varchar), 39, 4)
        )
      ) AS address_alias,
      SUM(b.balance) AS balance
    FROM balances b
    LEFT JOIN label_map lm ON lm.addr = b.address
    LEFT JOIN cex.addresses AS cex
      ON cex.address = b.address AND cex.blockchain = 'ethereum'
    GROUP BY 1
  ),

  top10 AS (
    SELECT address_alias, balance
    FROM labeled
    ORDER BY balance DESC
    LIMIT 10
  )

SELECT address_alias, balance
FROM top10

UNION ALL

SELECT 'All others' AS address_alias, SUM(balance) AS balance
FROM labeled
WHERE address_alias NOT IN (SELECT address_alias FROM top10)

ORDER BY balance DESC
