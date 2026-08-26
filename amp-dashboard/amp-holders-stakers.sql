-- Amp Holders & Stakers
-- Dune query 2262211: https://dune.com/queries/2262211
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- One row per address: AMP wallet balance, staked amount (Flexa Capacity v2 + v3),
-- combined total, USD value, first-received / last-activity dates, and a label.
--   * first_received: date of first incoming AMP transfer (or first v3 stake event
--     if earlier / no transfer exists for the address), linked to its tx on Etherscan
--   * last_activity: date of latest AMP transfer in either direction or v3
--     stake/unstake event, linked to its tx on Etherscan and suffixed with the
--     activity kind: (in) / (out) / (stake) / (unstake). Transfers to/from the
--     v2 Collateral Manager count as stake/unstake; on a same-block tie the v3
--     event kind wins over its own transfer leg.
--   * balance: net of all AMP ERC-20 transfers (single scan, +to / -from)
--   * v2 staked: net AMP transferred to/from the v2 Collateral Manager
--   * v3 staked: decoded pool events (anvil_ethereum.timebasedcollateralpool_evt_*):
--       CollateralStaked(account, token, amount, poolUnitsIssued) -> +units issued
--       UnstakeInitiated(account, token, unitsToUnstake, ...)     -> -units queued
--     Covers every entry path (stake, depositAndStake, stakeReleasableTokensFrom,
--     multisig / ERC-4337 wrappers) and credits the actual staking account rather
--     than the transaction sender. Units convert 1:1 to AMP until a pool claim or
--     reset changes the ratio (none have occurred to date).
WITH
  -- every AMP transfer, emitted once per side (single table scan)
  transfer_flows AS (
    -- kind: transfers to/from the v2 Collateral Manager are v2 stakes/unstakes,
    -- everything else is a plain out/in
    SELECT f.holder, f.amount, f.kind, tr.evt_block_time AS block_time, tr.evt_tx_hash AS tx_hash
    FROM erc20_ethereum.evt_Transfer tr
    CROSS JOIN UNNEST(
      ARRAY[
        ROW(tr."from", -CAST(tr.value AS int256),
            CASE WHEN tr."to" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
                 THEN 'stake' ELSE 'out' END),
        ROW(tr."to",    CAST(tr.value AS int256),
            CASE WHEN tr."from" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
                 THEN 'unstake' ELSE 'in' END)
      ]
    ) AS f(holder, amount, kind)
    WHERE tr.contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
  ),

  holders_grouped AS (
    SELECT holder, SUM(amount) / 1e18 AS balance
    FROM transfer_flows
    GROUP BY 1
    HAVING SUM(amount) > CAST(0 AS int256)
  ),

  -- first incoming AMP transfer and latest AMP transfer (either direction) per
  -- address, each with its transaction hash (MIN_BY/MAX_BY ignore NULL keys, so
  -- the outgoing-only CASE rows don't affect the "first received" pick)
  transfer_dates AS (
    SELECT
      holder,
      MIN(CASE WHEN amount > CAST(0 AS int256) THEN block_time END)         AS first_received_at,
      MIN_BY(tx_hash,
             CASE WHEN amount > CAST(0 AS int256) THEN block_time END)      AS first_received_tx,
      MAX(block_time)                                                       AS last_transfer_at,
      MAX_BY(tx_hash, block_time)                                           AS last_transfer_tx,
      MAX_BY(kind, block_time)                                              AS last_transfer_kind
    FROM transfer_flows
    GROUP BY 1
  ),

  -- Flexa Capacity v2: net AMP moved into the Collateral Manager, per staker.
  -- Both sides are emitted per transfer (single scan) so the manager's frequent
  -- self-transfers (Amp partition moves, from = to = manager) net to zero exactly
  -- as in the two-branch original.
  stakers_v2_grouped AS (
    SELECT f.holder, SUM(f.amount) / 1e18 AS staked
    FROM erc20_ethereum.evt_Transfer tr
    CROSS JOIN UNNEST(
      ARRAY[
        ROW(CASE WHEN tr."to" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
                 THEN tr."from" END,  CAST(tr.value AS int256)),
        ROW(CASE WHEN tr."from" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
                 THEN tr."to" END,   -CAST(tr.value AS int256))
      ]
    ) AS f(holder, amount)
    WHERE tr.contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND (tr."to"   = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
        OR tr."from" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578)
      AND f.holder IS NOT NULL
    GROUP BY 1
    HAVING SUM(f.amount) > CAST(0 AS int256)
  ),

  -- Flexa Capacity v3: staked units per account from the decoded
  -- anvil_ethereum.timebasedcollateralpool_evt_* tables, filtered to AMP so
  -- every current and future Flexa pool is covered without an address list
  stakers_v3_grouped AS (
    SELECT
      account                      AS holder,
      SUM(units) / 1e18            AS staked,
      MIN(block_time)              AS first_event_at,
      MIN_BY(tx_hash, block_time)  AS first_event_tx,
      MAX(block_time)              AS last_event_at,
      MAX_BY(tx_hash, block_time)  AS last_event_tx,
      MAX_BY(kind, block_time)     AS last_event_kind
    FROM (
      SELECT account, evt_block_time AS block_time, evt_tx_hash AS tx_hash,
             'stake' AS kind, CAST(poolUnitsIssued AS int256) AS units
      FROM anvil_ethereum.timebasedcollateralpool_evt_collateralstaked
      WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2 -- AMP

      UNION ALL

      SELECT account, evt_block_time, evt_tx_hash,
             'unstake', -CAST(unitsToUnstake AS int256)
      FROM anvil_ethereum.timebasedcollateralpool_evt_unstakeinitiated
      WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    )
    GROUP BY 1
  ),

  -- one staked figure per holder (v2 + v3); v2 activity is transfer-based so its
  -- event dates come from transfer_dates, hence the NULLs here
  stakers_all_grouped AS (
    SELECT
      holder,
      SUM(staked)                              AS staked,
      MIN(first_event_at)                      AS first_event_at,
      MIN_BY(first_event_tx, first_event_at)   AS first_event_tx,
      MAX(last_event_at)                       AS last_event_at,
      MAX_BY(last_event_tx, last_event_at)     AS last_event_tx,
      MAX_BY(last_event_kind, last_event_at)   AS last_event_kind
    FROM (
      SELECT holder, staked,
             CAST(NULL AS timestamp) AS first_event_at,
             CAST(NULL AS varbinary) AS first_event_tx,
             CAST(NULL AS timestamp) AS last_event_at,
             CAST(NULL AS varbinary) AS last_event_tx,
             CAST(NULL AS varchar)   AS last_event_kind
      FROM stakers_v2_grouped
      UNION ALL
      SELECT holder, staked, first_event_at, first_event_tx, last_event_at, last_event_tx,
             last_event_kind
      FROM stakers_v3_grouped
    )
    GROUP BY 1
  ),

  tables_combined AS (
    SELECT
      COALESCE(h.holder, s.holder)                        AS holder,
      COALESCE(h.balance, 0)                              AS balance,
      COALESCE(s.staked, 0)                               AS staked,
      COALESCE(h.balance, 0) + COALESCE(s.staked, 0)      AS total,
      -- earliest of first incoming transfer / first v3 stake event; the LEAST +
      -- COALESCE pairing keeps a real date when either side is NULL
      LEAST(
        COALESCE(td.first_received_at, s.first_event_at),
        COALESCE(s.first_event_at, td.first_received_at)
      )                                                   AS first_received_at,
      CASE
        WHEN td.first_received_at IS NOT NULL
         AND (s.first_event_at IS NULL OR td.first_received_at <= s.first_event_at)
        THEN td.first_received_tx
        ELSE s.first_event_tx
      END                                                 AS first_received_tx,
      GREATEST(
        COALESCE(td.last_transfer_at, s.last_event_at),
        COALESCE(s.last_event_at, td.last_transfer_at)
      )                                                   AS last_activity_at,
      -- on a timestamp tie the v3 event wins: a v3 stake tx emits both the
      -- outgoing transfer and CollateralStaked in the same block, and the
      -- stake is the meaningful action
      CASE
        WHEN s.last_event_at IS NOT NULL
         AND (td.last_transfer_at IS NULL OR s.last_event_at >= td.last_transfer_at)
        THEN s.last_event_tx
        ELSE td.last_transfer_tx
      END                                                 AS last_activity_tx,
      CASE
        WHEN s.last_event_at IS NOT NULL
         AND (td.last_transfer_at IS NULL OR s.last_event_at >= td.last_transfer_at)
        THEN s.last_event_kind
        ELSE td.last_transfer_kind
      END                                                 AS last_activity_kind
    FROM holders_grouped h
    FULL OUTER JOIN stakers_all_grouped s ON h.holder = s.holder
    LEFT JOIN transfer_dates td ON td.holder = COALESCE(h.holder, s.holder)
  ),

  price_query AS (
    SELECT price
    FROM prices.usd
    WHERE blockchain       = 'ethereum'
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    ORDER BY minute DESC
    LIMIT 1
  ),

  -- curated address labels (deduped from the original CASE list)
  label_map (addr, label) AS (
    VALUES
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
  )

SELECT
  ROW_NUMBER() OVER (ORDER BY tc.total DESC) AS "rank",
  SUBSTR(CAST(tc.holder AS varchar), 1, 6) || '...' ||
    SUBSTR(CAST(tc.holder AS varchar), 39) ||
    ' | <a href="https://etherscan.io/address/' || CAST(tc.holder AS varchar) ||
    '" target="_blank">Etherscan</a>' ||
    ' | <a href="https://debank.com/profile/' || CAST(tc.holder AS varchar) ||
    '" target="_blank">DeBank</a>' AS holder,
  tc.total,
  tc.balance,
  tc.staked,
  tc.total * pq.price AS total_value,
  '<a href="https://etherscan.io/tx/' || CAST(tc.first_received_tx AS varchar) ||
    '" target="_blank">' || CAST(CAST(tc.first_received_at AS date) AS varchar) ||
    '</a>' AS first_received,
  '<a href="https://etherscan.io/tx/' || CAST(tc.last_activity_tx AS varchar) ||
    '" target="_blank">' || CAST(CAST(tc.last_activity_at AS date) AS varchar) ||
    ' (' || tc.last_activity_kind || ')</a>' AS last_activity,
  COALESCE(
    lm.label,
    cex.distinct_name,
    CASE
      WHEN tc.total >= 1000000000 THEN U&'\+01F40B'
      WHEN tc.total >= 500000000  THEN U&'\+01F433'
      WHEN tc.total >= 250000000  THEN U&'\+01F988'
      WHEN tc.total >= 100000000  THEN U&'\+01F42C'
      WHEN tc.total >= 50000000   THEN U&'\+01F9AD'
      WHEN tc.total >= 25000000   THEN U&'\+01F419'
      WHEN tc.total >= 10000000   THEN U&'\+01F421'
      WHEN tc.total >= 5000000    THEN U&'\+01F420'
      WHEN tc.total >= 2500000    THEN U&'\+01F41F'
      ELSE U&'\+01F40C'
    END
  ) AS "Label"
FROM tables_combined tc
CROSS JOIN price_query pq
LEFT JOIN label_map lm ON lm.addr = tc.holder
LEFT JOIN cex.addresses AS cex
  ON cex.address = tc.holder AND cex.blockchain = 'ethereum'
ORDER BY "rank"
