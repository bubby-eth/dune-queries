WITH
  -- 1) pool/router addresses to include
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
        0x1693DeCE45b908Ed25244E8b7FFdE4760cB9Ca24,
        0x603f0200e863784e03cD262bB5266d819DD0eAf0
      ])
    ) AS v(addrs)
    CROSS JOIN UNNEST(v.addrs) AS t(addr)
  ),

  -- 2) daily AMP price (for V3 USD calc)
  price_query AS (
    SELECT
      DATE_TRUNC('day', minute) AS the_date,
      AVG(price)                AS price
    FROM prices.usd
    WHERE blockchain         = 'ethereum'
      AND contract_address   = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND minute >= TIMESTAMP '2024-11-01'
    GROUP BY 1
  ),

  -- 3) V3 staking inflows and outflows by day
  v3_staked AS (
    SELECT
      DATE_TRUNC('day', block_time) AS day,
      SUM(token_amount)             AS daily_staked
    FROM (
      -- 3a) direct stake (+)
      SELECT
        block_time,
        varbinary_to_uint256(varbinary_substring(data, 37, 32)) / 1e18 AS token_amount
      FROM ethereum.transactions t
      JOIN address_list a
        ON t.to = a.addr
      WHERE t.block_time > TIMESTAMP '2024-11-01'
        AND varbinary_substring(t.data, 1, 4) = 0x3e12170f
        AND t.success = TRUE

      UNION ALL

      -- 3b) direct stakeReleasableTokensFrom (+)
      SELECT
        block_time,
        varbinary_to_uint256(varbinary_substring(data, 69, 32)) / 1e18 AS token_amount
      FROM ethereum.transactions t
      JOIN address_list a
        ON t.to = a.addr
      WHERE t.block_time > TIMESTAMP '2024-11-01'
        AND varbinary_substring(t.data, 1, 4) = 0x34048584
        AND t.success = TRUE

      UNION ALL

      -- 3c) direct unstake (−)
      SELECT
        block_time,
        -varbinary_to_uint256(varbinary_substring(data, 37, 32)) / 1e18 AS token_amount
      FROM ethereum.transactions t
      JOIN address_list a
        ON t.to = a.addr
      WHERE t.block_time > TIMESTAMP '2024-11-01'
        AND varbinary_substring(t.data, 1, 4) = 0xc2a672e0
        AND t.success = TRUE

      UNION ALL

      -- 3d) multisig stake (+)
      SELECT
        block_time,
        varbinary_to_uint256(varbinary_substring(substr(data, 357, 352), 37, 32)) / 1e18 AS token_amount
      FROM ethereum.transactions t
      JOIN address_list a
        ON varbinary_ltrim(substr(t.data, 5, 32)) = a.addr
      WHERE t.block_time > TIMESTAMP '2024-11-01'
        AND varbinary_substring(substr(t.data, 357, 352), 1, 4) = 0x3e12170f
        AND t.success = TRUE

      UNION ALL

      -- 3e) multisig stakeReleasableTokensFrom (+)
      SELECT
        block_time,
        varbinary_to_uint256(varbinary_substring(substr(data, 357, 352), 69, 32)) / 1e18 AS token_amount
      FROM ethereum.transactions t
      JOIN address_list a
        ON varbinary_ltrim(substr(t.data, 5, 32)) = a.addr
      WHERE t.block_time > TIMESTAMP '2024-11-01'
        AND varbinary_substring(substr(t.data, 357, 352), 1, 4) = 0x34048584
        AND t.success = TRUE

      UNION ALL

      -- 3f) multisig unstake (−)
      SELECT
        block_time,
        -varbinary_to_uint256(varbinary_substring(substr(data, 357, 352), 37, 32)) / 1e18 AS token_amount
      FROM ethereum.transactions t
      JOIN address_list a
        ON varbinary_ltrim(substr(t.data, 5, 32)) = a.addr
      WHERE t.block_time > TIMESTAMP '2024-11-01'
        AND varbinary_substring(substr(t.data, 357, 352), 1, 4) = 0xc2a672e0
        AND t.success = TRUE
    ) AS stake_events
    GROUP BY 1
  )

SELECT
  v3.day                              AS day,
  v3.daily_staked                     AS v3_daily_net,
  SUM(v3.daily_staked) OVER (
    ORDER BY v3.day
  )                                   AS v3_cumulative,
  SUM(v3.daily_staked) OVER (
    ORDER BY v3.day
  )                                   AS total_amp_balance,
  COALESCE(pq.price, 0)               AS price_at_time,
  SUM(v3.daily_staked) OVER (
    ORDER BY v3.day
  ) * COALESCE(pq.price, 0)           AS total_usd_value
FROM v3_staked v3
LEFT JOIN price_query pq
  ON v3.day = pq.the_date
ORDER BY v3.day