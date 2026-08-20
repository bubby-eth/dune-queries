-- Flexa Capacity V3 Pool Share
-- Dune query 4675161: https://dune.com/queries/4675161
-- From dashboard: https://dune.com/ampdotxyz/amp-token

-- Flexa Capacity v3 weekly reserved-collateral history per pool.
-- Output: one row per pool x week with the week's net flow, the running reserved
-- balance, and its USD value (at the current AMP price).
--
-- Pool AMP is custodied by the Anvil CollateralVault
-- (0x5d2725fdE4d7Aa3388DA4519ac0449Cc031d675f) as collateral reservations;
-- running_total = reservation.tokenAmount over time.
-- Reconstructed from vault events (exact, per the CollateralVault source):
--   CollateralReserved            -> reservation created with tokenAmount = amount
--   CollateralReservationModified -> tokenAmount: oldAmount -> newAmount
--   CollateralClaimed             -> tokenAmount -= amountWithFee
--                                    (remainderReleased = true deletes the reservation)
--   CollateralReleased            -> reservation deleted (balance -> 0)
-- Deleted reservations are handled with a correction delta at termination time that
-- nets the reservation's lifetime sum to exactly zero (covers the remainder released
-- by a terminal claim, which no event reports directly). Reservation ids are never
-- reused, and a pool reset (new reservation id) is picked up automatically.
WITH
  -- 1) pool address -> name
  --    (from https://api.flexa.co/collateral_pools; see flexa-pools.json)
  pool_map (addr, pool_name) AS (
    VALUES
      (0xd0415cf4558A0dBEE8242498D25284476bE3c8f2, 'Lightning'),
      (0xA52125ced25602203BCeF6E78E865571306CaB2A, 'Base'),
      (0xD57E335457b6f5d09ac69248230005a02F9B60CF, 'Nighthawk Wallet'),
      (0xdB07414039F5e1618E3eCC8019C1C1ecb4b4C06A, 'Bitcoin'),
      (0xE932d1a226E962D820a33363DF32FcC95D2559D2, 'Solana'),
      (0x9477dA44A61ceBCDD0383CD00Bf18A859FEb75b0, 'Ethereum'),
      (0xFF1D02F09A9C55cEFd37f57715FEe7E88278d34e, 'SPEDN'),
      (0x59e772F12938063bCa8A2B978791eBe225f5Bc3c, 'Bitcoin Cash'),
      (0xd80370093a305bbDA27B821bb6c6347989Bf709b, 'Zodl'),
      (0x84706656fabFE15b2b77F292A656dD024607d332, 'Litecoin'),
      (0xa7f2B6aF8c536897f246B1EB62654cb9c886FA47, 'Dogecoin'),
      (0x80E58Fe28F53CCbaD1c295ebAA6A8c13241D034b, 'Celo'),
      (0x1e73f41454D9806f0462Eb6C9FD2A3754cEE7Fc4, 'Polygon'),
      (0xc163c2cC35e32350Aa92DEC2b53b68950942d72F, 'Avalanche'),
      (0x57F6f249DB02083362D43E2D02dD791068Df30C6, 'Cardano'),
      (0xcfBbAE9DCE9a207BaB01E1589e345D3Edc65D842, 'Zcash'),
      (0xCD234A11B26F42B391C2838Beb3DA3Bb3A590B66, 'Tezos'),
      (0xB8706F2dd1Ce8A4328D254cF14271e0fbB5E268A, 'Burner'),
      (0x1693DeCE45b908Ed25244E8b7FFdE4760cB9Ca24, 'Nexus Wallet'),
      (0x603f0200e863784e03cD262bB5266d819DD0eAf0, 'World Chain')
  ),

  -- 2) every week since Capacity v3 launch
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

  -- 3) every vault reservation belonging to a pool
  reservations AS (
    SELECT r."reservationId" AS reservation_id, pm.pool_name
    FROM anvil_ethereum.collateralvault_evt_collateralreserved r
    JOIN pool_map pm ON r.account = pm.addr
  ),

  -- 4) signed balance changes per reservation, with event time
  deltas AS (
    SELECT "reservationId" AS reservation_id, evt_block_time,
           CAST(amount AS int256) AS delta
    FROM anvil_ethereum.collateralvault_evt_collateralreserved
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)

    UNION ALL

    SELECT "reservationId", evt_block_time,
           CAST("newAmount" AS int256) - CAST("oldAmount" AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateralreservationmodified
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)

    UNION ALL

    SELECT "reservationId", evt_block_time,
           -CAST("amountWithFee" AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)

    UNION ALL

    SELECT "reservationId", evt_block_time,
           -CAST(amount AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateralreleased
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)
  ),

  -- 5) when (if ever) each reservation was terminated
  terminations AS (
    SELECT reservation_id, MIN(evt_block_time) AS term_time
    FROM (
      SELECT "reservationId" AS reservation_id, evt_block_time
      FROM anvil_ethereum.collateralvault_evt_collateralreleased
      WHERE "reservationId" IN (SELECT reservation_id FROM reservations)
      UNION ALL
      SELECT "reservationId", evt_block_time
      FROM anvil_ethereum.collateralvault_evt_collateralclaimed
      WHERE "reservationId" IN (SELECT reservation_id FROM reservations)
        AND "remainderReleased" = TRUE
    )
    GROUP BY reservation_id
  ),

  -- 6) correction delta so a terminated reservation nets to exactly zero
  --    (zero for CollateralReleased, the released remainder for terminal claims)
  corrections AS (
    SELECT t.reservation_id, t.term_time AS evt_block_time,
           -SUM(d.delta) AS delta
    FROM terminations t
    JOIN deltas d ON d.reservation_id = t.reservation_id
    GROUP BY t.reservation_id, t.term_time
  ),

  -- 7) weekly net flow per pool
  weekly_raw AS (
    SELECT
      res.pool_name,
      CAST(date_trunc('week', d.evt_block_time) AS DATE) AS week_start,
      SUM(d.delta) / 1e18 AS weekly_tokens
    FROM (
      SELECT reservation_id, evt_block_time, delta FROM deltas
      UNION ALL
      SELECT reservation_id, evt_block_time, delta FROM corrections
    ) d
    JOIN reservations res ON d.reservation_id = res.reservation_id
    GROUP BY res.pool_name, CAST(date_trunc('week', d.evt_block_time) AS DATE)
  ),

  -- 8) fill in any missing week/pool combinations
  weekly_aggregates AS (
    SELECT
      pm.pool_name,
      c.week_start,
      COALESCE(wr.weekly_tokens, 0) AS weekly_tokens
    FROM (SELECT DISTINCT pool_name FROM reservations) pm
    CROSS JOIN calendar c
    LEFT JOIN weekly_raw wr
      ON wr.pool_name = pm.pool_name
     AND wr.week_start = c.week_start
  ),

  -- 9) current AMP price
  price_query AS (
    SELECT price
    FROM prices.usd
    WHERE blockchain       = 'ethereum'
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    ORDER BY minute DESC NULLS FIRST
    LIMIT 1
  )

-- 10) rolling totals & USD valuation
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
