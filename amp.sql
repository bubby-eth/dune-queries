WITH
  ----------------------------------------------------------------
  -- 1) Reusable list of all V3 pool/router addresses
  ----------------------------------------------------------------
  address_list AS (
    SELECT t.addr
    FROM (
      VALUES (ARRAY[
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
        0x1693DeCE45b908Ed25244E8b7FFdE4760cB9Ca24
      ])
    ) AS v(addrs)
    CROSS JOIN UNNEST(v.addrs) AS t(addr)
  ),

  ----------------------------------------------------------------
  -- 2) Build AMP-transfer ledger (V2 deposit/withdraw)
  ----------------------------------------------------------------
  amp AS (
    -- 2a) all inbound transfers (+)
    SELECT
      "to"                AS holder,
      contract_address    AS token_address,
      CAST("value" AS INT256)   AS balanceChange
    FROM erc20_ethereum.evt_Transfer
    WHERE "to" <> 0x0000000000000000000000000000000000000000
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2

    UNION ALL

    -- 2b) all outbound transfers (−)
    SELECT
      "from"              AS holder,
      contract_address    AS token_address,
      -CAST("value" AS INT256)  AS balanceChange
    FROM erc20_ethereum.evt_Transfer
    WHERE "from" <> 0x0000000000000000000000000000000000000000
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
  ),

  ----------------------------------------------------------------
  -- 3) Total supply = sum of all balanceChanges
  ----------------------------------------------------------------
  ts AS (
    SELECT
      token_address,
      SUM(balanceChange)/1e18 AS total_supply
    FROM amp
    GROUP BY 1
  ),

  ----------------------------------------------------------------
  -- 4) Circulating supply = exclude known non-circulating holders
  ----------------------------------------------------------------
  cs AS (
    SELECT
      token_address,
      SUM(balanceChange)/1e18 AS circulating_supply
    FROM amp
    WHERE holder NOT IN (
      0xbcfc557f0b74250a6c4c671b2e6e4daf6af2b9d0,
      0x0c3a4a4416562ddccfda34e4fe681569fe60c7bd,
      0x780f9a570c1bec9f2dc761b9031c992cb3e2ae6e,
      0x9eDA92280965832466c15Cd17d66D5E58969FD62
    )
    GROUP BY 1
  ),

  ----------------------------------------------------------------
  -- 5) V2 stake on AMP contract (single ERC20 holder)
  ----------------------------------------------------------------
  staked AS (
    SELECT
      token_address,
      SUM(balanceChange)/1e18 AS staked_tokens
    FROM amp
    WHERE holder = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
    GROUP BY 1
  ),

  ----------------------------------------------------------------
  -- 6) V3 direct & multisig stake/unstake events
  ----------------------------------------------------------------
  v3_staked AS (
    SELECT
      SUM(token_amount) AS staked_tokens
    FROM (
      -- 6a) direct stake (+)
      SELECT
        varbinary_to_uint256(varbinary_substring(data,37,32))/1e18 AS token_amount
      FROM ethereum.transactions t
      WHERE t."to" IN (SELECT addr FROM address_list)
        AND t.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND varbinary_substring(t.data,1,4) = 0x3e12170f
        AND t.success = TRUE

      UNION ALL

      -- 6b) direct stakeReleasableTokensFrom (+)
      SELECT
        varbinary_to_uint256(varbinary_substring(data,69,32))/1e18 AS token_amount
      FROM ethereum.transactions t
      WHERE t."to" IN (SELECT addr FROM address_list)
        AND t.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND varbinary_substring(t.data,1,4) = 0x34048584
        AND t.success = TRUE

      UNION ALL

      -- 6c) direct unstake (−)
      SELECT
        -varbinary_to_uint256(varbinary_substring(data,37,32))/1e18 AS token_amount
      FROM ethereum.transactions t
      WHERE t."to" IN (SELECT addr FROM address_list)
        AND t.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND varbinary_substring(t.data,1,4) = 0xc2a672e0
        AND t.success = TRUE

      UNION ALL

      -- 6d) multisig stake (+)
      SELECT
        varbinary_to_uint256(
          varbinary_substring(substr(data,357,352),37,32)
        )/1e18 AS token_amount
      FROM ethereum.transactions t
      WHERE varbinary_ltrim(substr(t.data,5,32)) IN (SELECT addr FROM address_list)
        AND t.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND varbinary_substring(substr(t.data,357,352),1,4) = 0x3e12170f
        AND t.success = TRUE

      UNION ALL

      -- 6e) multisig stakeReleasableTokensFrom (+)
      SELECT
        varbinary_to_uint256(
          varbinary_substring(substr(data,357,352),69,32)
        )/1e18 AS token_amount
      FROM ethereum.transactions t
      WHERE varbinary_ltrim(substr(t.data,5,32)) IN (SELECT addr FROM address_list)
        AND t.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND varbinary_substring(substr(t.data,357,352),1,4) = 0x34048584
        AND t.success = TRUE

      UNION ALL

      -- 6f) multisig unstake (−)
      SELECT
        -varbinary_to_uint256(
          varbinary_substring(substr(data,357,352),37,32)
        )/1e18 AS token_amount
      FROM ethereum.transactions t
      WHERE varbinary_ltrim(substr(t.data,5,32)) IN (SELECT addr FROM address_list)
        AND t.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND varbinary_substring(substr(t.data,357,352),1,4) = 0xc2a672e0
        AND t.success = TRUE
    ) AS events
  ),

  ----------------------------------------------------------------
  -- 7) Combine V2 & V3 staked totals
  ----------------------------------------------------------------
  all_staked AS (
    SELECT
      ts.token_address,
      COALESCE(staked.staked_tokens,0)
      + COALESCE(v3_staked.staked_tokens,0) AS staked_tokens,
      COALESCE(staked.staked_tokens,0)    AS v2_staked
    FROM ts
    LEFT JOIN staked     ON staked.token_address    = ts.token_address
    LEFT JOIN v3_staked  ON TRUE
  ),

  ----------------------------------------------------------------
  -- 8) Latest AMP price for USD conversions
  ----------------------------------------------------------------
  price_query AS (
    SELECT price
    FROM prices.usd
    WHERE blockchain        = 'ethereum'
      AND contract_address  = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    ORDER BY minute DESC NULLS FIRST
    LIMIT 1
  )

SELECT
  -- supply metrics (in billions)
  ts.total_supply/1e9        AS "Total Supply (B)",
  cs.circulating_supply/1e9  AS "Circulating Supply (B)",

  -- staked metrics
  all_staked.staked_tokens/1e9               AS "Staked Tokens (B)",
  all_staked.staked_tokens/
    NULLIF(cs.circulating_supply,0)*100      AS "Staked % of Circulating",

  -- contract address + price
  ts.token_address                          AS "AMP Address",
  price_query.price                         AS "AMP Price (USD)",

  -- TVL/Market-cap/FDV (in millions)
  all_staked.v2_staked * price_query.price /1e6   AS "Capacity TVL (M)",
  cs.circulating_supply   * price_query.price /1e6   AS "Market Cap (M)",
  ts.total_supply         * price_query.price /1e6   AS "FDV (M)",

  -- raw full supply values
  cs.circulating_supply AS full_circulating_supply,
  ts.total_supply       AS full_total_supply

FROM ts
JOIN cs           ON cs.token_address    = ts.token_address
LEFT JOIN all_staked ON all_staked.token_address = ts.token_address
LEFT JOIN price_query ON TRUE;