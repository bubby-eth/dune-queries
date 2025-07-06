WITH
  -- 1) list of all pool/router addresses
  address_list AS (
    SELECT addr
    FROM UNNEST(
      ARRAY[
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
      ]
    ) AS t(addr)
  ),

  -- 2) map each address to its pool_name
  pool_map AS (
    SELECT addr, pool_name
    FROM (
      VALUES
        (0xd0415cf4558A0dBEE8242498D25284476bE3c8f2, 'Lightning'),
        (0xA52125ced25602203BCeF6E78E865571306CaB2A, 'Base'),
        (0xD57E335457b6f5d09ac69248230005a02F9B60CF, 'Nighthawk'),
        (0xdB07414039F5e1618E3eCC8019C1C1ecb4b4C06A, 'Bitcoin'),
        (0xE932d1a226E962D820a33363DF32FcC95D2559D2, 'Solana'),
        (0x9477dA44A61ceBCDD0383CD00Bf18A859FEb75b0, 'Ethereum'),
        (0xFF1D02F09A9C55cEFd37f57715FEe7E88278d34e, 'SPEDN'),
        (0x59e772F12938063bCa8A2B978791eBe225f5Bc3c, 'Bitcoin Cash'),
        (0xd80370093a305bbDA27B821bb6c6347989Bf709b, 'Zashi'),
        (0x84706656fabFE15b2b77F292A656dD024607d332, 'Litecoin'),
        (0xa7f2B6aF8c536897f246B1EB62654cb9c886FA47, 'Dogecoin'),
        (0x80E58Fe28F53CCbaD1c295ebAA6A8c13241D034b, 'Celo'),
        (0x1e73f41454D9806f0462Eb6C9FD2A3754cEE7Fc4, 'Polygon'),
        (0xc163c2cC35e32350Aa92DEC2b53b68950942d72F, 'Avalanche'),
        (0x57F6f249DB02083362D43E2D02dD791068Df30C6, 'Cardano'),
        (0xcfBbAE9DCE9a207BaB01E1589e345D3Edc65D842, 'Zcash'),
        (0xCD234A11B26F42B391C2838Beb3DA3Bb3A590B66, 'Tezos'),
        (0xB8706F2dd1Ce8A4328D254cF14271e0fbB5E268A, 'Burner'),
        (0x1693DeCE45b908Ed25244E8b7FFdE4760cB9Ca24, 'Nexus')
    ) AS v(addr, pool_name)
  ),

  -- 3) every week since 2024-11-01
  calendar AS (
    SELECT week_start
    FROM UNNEST(
      sequence(
        CAST(date_trunc('week', DATE '2024-11-01') AS DATE),
        current_date,
        INTERVAL '7' day
      )
    ) AS t(week_start)
  ),

  -- 4) which pools actually had on-chain activity
  pool_list AS (
    SELECT DISTINCT pm.pool_name
    FROM ethereum.transactions tx
    JOIN address_list al  ON tx."to" = al.addr
    JOIN pool_map     pm  ON tx."to" = pm.addr
    WHERE tx.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
      AND tx.success = TRUE
  ),

  -- 5) raw weekly token flows (six methods, then group)
  weekly_raw AS (
    SELECT
      pool_name,
      CAST(date_trunc('week', block_time) AS DATE) AS week_start,
      SUM(token_amount) AS weekly_tokens
    FROM (
      -- 5a) stake
      SELECT
        pm.pool_name,
        tx.block_time,
        varbinary_to_uint256(substr(tx.data,37,32)) / 1e18 AS token_amount
      FROM ethereum.transactions tx
      JOIN pool_map pm
        ON tx."to" = pm.addr
      WHERE tx.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND substr(tx.data,1,4) = 0x3e12170f
        AND tx.success = TRUE

      UNION ALL

      -- 5b) multisig stake
      SELECT
        pm.pool_name,
        tx.block_time,
        varbinary_to_uint256(substr(substr(tx.data,357,352),37,32)) / 1e18
      FROM ethereum.transactions tx
      JOIN pool_map pm
        ON varbinary_ltrim(substr(tx.data,5,32)) = pm.addr
      WHERE tx.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND substr(substr(tx.data,357,352),1,4) = 0x3e12170f
        AND tx.success = TRUE

      UNION ALL

      -- 5c) stakeReleasableTokensFrom
      SELECT
        pm.pool_name,
        tx.block_time,
        varbinary_to_uint256(substr(tx.data,69,32)) / 1e18
      FROM ethereum.transactions tx
      JOIN pool_map pm
        ON tx."to" = pm.addr
      WHERE tx.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND substr(tx.data,1,4) = 0x34048584
        AND tx.success = TRUE

      UNION ALL

      -- 5d) multisig stakeReleasableTokensFrom
      SELECT
        pm.pool_name,
        tx.block_time,
        varbinary_to_uint256(substr(substr(tx.data,357,352),69,32)) / 1e18
      FROM ethereum.transactions tx
      JOIN pool_map pm
        ON varbinary_ltrim(substr(tx.data,5,32)) = pm.addr
      WHERE tx.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND substr(substr(tx.data,357,352),1,4) = 0x34048584
        AND tx.success = TRUE

      UNION ALL

      -- 5e) unstake (negative)
      SELECT
        pm.pool_name,
        tx.block_time,
       -varbinary_to_uint256(substr(tx.data,37,32)) / 1e18
      FROM ethereum.transactions tx
      JOIN pool_map pm
        ON tx."to" = pm.addr
      WHERE tx.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND substr(tx.data,1,4) = 0xc2a672e0
        AND tx.success = TRUE

      UNION ALL

      -- 5f) multisig unstake (negative)
      SELECT
        pm.pool_name,
        tx.block_time,
       -varbinary_to_uint256(substr(substr(tx.data,357,352),37,32)) / 1e18
      FROM ethereum.transactions tx
      JOIN pool_map pm
        ON varbinary_ltrim(substr(tx.data,5,32)) = pm.addr
      WHERE tx.block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND substr(substr(tx.data,357,352),1,4) = 0xc2a672e0
        AND tx.success = TRUE
    ) AS flows
    GROUP BY pool_name, CAST(date_trunc('week', block_time) AS DATE)
  ),

  -- 6) fill in any missing week/pool combinations
  weekly_aggregates AS (
    SELECT
      pl.pool_name,
      c.week_start,
      COALESCE(wr.weekly_tokens, 0) AS weekly_tokens
    FROM pool_list pl
    CROSS JOIN calendar c
    LEFT JOIN weekly_raw wr
      ON wr.pool_name = pl.pool_name
     AND wr.week_start = c.week_start
  ),

  -- 7) current AMP price (note: not Celo)
  price_query AS (
    SELECT *
    FROM prices.usd
    WHERE blockchain = 'ethereum'
      AND "contract_address" = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    ORDER BY minute DESC NULLS FIRST
    LIMIT 1
  )

-- 8) rolling totals & USD valuation
SELECT
  wa.pool_name,
  wa.week_start,
  wa.weekly_tokens,
  SUM(wa.weekly_tokens) OVER (
    PARTITION BY wa.pool_name
    ORDER BY wa.week_start
  ) AS running_total,
  SUM(wa.weekly_tokens) OVER (
    PARTITION BY wa.pool_name
    ORDER BY wa.week_start
  ) * pq.price AS usd_value
FROM weekly_aggregates wa
CROSS JOIN price_query pq
ORDER BY wa.pool_name, wa.week_start DESC;