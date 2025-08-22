WITH
  transfers AS (
    SELECT
      tr.evt_block_time,
      tr."from" AS address,
      -CAST(tr.value AS INT256) AS amount,
      tr.contract_address
    FROM erc20_ethereum.evt_Transfer AS tr
    WHERE tr.contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2

    UNION ALL

    SELECT
      tr.evt_block_time,
      tr."to" AS address,
      CAST(tr.value AS INT256) AS amount,
      tr.contract_address
    FROM erc20_ethereum.evt_Transfer AS tr
    WHERE tr.contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
  ),

  -- Pre-aggregate to shrink downstream joins
  addr_sums AS (
    SELECT address, SUM(amount) AS amount_sum
    FROM transfers
    GROUP BY address
  ),

  latest_price AS (
    SELECT price
    FROM prices.usd
    WHERE blockchain = 'ethereum'
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    ORDER BY minute DESC
    LIMIT 1
  ),

  address_labels AS (
    SELECT * FROM (VALUES
      (0x9eda92280965832466c15cd17d66d5e58969fd62, '🔒 Network Development Treasury'),
      (0x0c3a4a4416562ddccfda34e4fe681569fe60c7bd, '🔒 Network Development Treasury'),
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
      (0x8aca98f082b88e7516bdfc110a12eb521cb8ddbe, 'Binance US: Deposit')
    ) AS t(address, label)
  )

SELECT
  concat(substr(s.address, 1, 6), '...', substr(s.address, length(s.address) - 3, 4)) AS address,
  s.amount_sum / 1e18 AS balance,
  (s.amount_sum * lp.price) / 1e18 AS value,
  COALESCE(al.label, cex.distinct_name, '🐋') AS label
FROM addr_sums s
CROSS JOIN latest_price lp
LEFT JOIN cex.addresses cex
  ON cex.address = s.address AND cex.blockchain = 'ethereum'
LEFT JOIN address_labels al
  ON al.address = s.address
GROUP BY
  concat(substr(s.address, 1, 6), '...', substr(s.address, length(s.address) - 3, 4)),
  COALESCE(al.label, cex.distinct_name, '🐋'),
  s.amount_sum,
  lp.price
ORDER BY
  balance DESC
LIMIT 10;