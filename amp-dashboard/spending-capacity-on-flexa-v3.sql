-- Spending Capacity on Flexa V3
-- Dune query 478869: https://dune.com/queries/478869
-- From dashboard: https://dune.com/ampdotxyz/amp-token

-- Daily net flows and cumulative reserved AMP across all Flexa Capacity v3 pools,
-- with USD value at each day's average AMP price.
--
-- Pool AMP is custodied by the Anvil CollateralVault
-- (0x5d2725fdE4d7Aa3388DA4519ac0449Cc031d675f) as collateral reservations;
-- total_amp_balance = sum of reservation.tokenAmount over time.
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
  -- 1) pool addresses to include
  --    (from https://api.flexa.co/collateral_pools; see flexa-pools.json)
  address_list (addr) AS (
    VALUES
      (0xd0415cf4558A0dBEE8242498D25284476bE3c8f2),
      (0xA52125ced25602203BCeF6E78E865571306CaB2A),
      (0xD57E335457b6f5d09ac69248230005a02F9B60CF),
      (0xdB07414039F5e1618E3eCC8019C1C1ecb4b4C06A),
      (0xE932d1a226E962D820a33363DF32FcC95D2559D2),
      (0x9477dA44A61ceBCDD0383CD00Bf18A859FEb75b0),
      (0xFF1D02F09A9C55cEFd37f57715FEe7E88278d34e),
      (0x59e772F12938063bCa8A2B978791eBe225f5Bc3c),
      (0xd80370093a305bbDA27B821bb6c6347989Bf709b),
      (0x84706656fabFE15b2b77F292A656dD024607d332),
      (0xa7f2B6aF8c536897f246B1EB62654cb9c886FA47),
      (0x80E58Fe28F53CCbaD1c295ebAA6A8c13241D034b),
      (0x1e73f41454D9806f0462Eb6C9FD2A3754cEE7Fc4),
      (0xc163c2cC35e32350Aa92DEC2b53b68950942d72F),
      (0x57F6f249DB02083362D43E2D02dD791068Df30C6),
      (0xcfBbAE9DCE9a207BaB01E1589e345D3Edc65D842),
      (0xCD234A11B26F42B391C2838Beb3DA3Bb3A590B66),
      (0xB8706F2dd1Ce8A4328D254cF14271e0fbB5E268A),
      (0x1693DeCE45b908Ed25244E8b7FFdE4760cB9Ca24),
      (0x603f0200e863784e03cD262bB5266d819DD0eAf0)
  ),

  -- 2) daily AMP price
  price_query AS (
    SELECT
      DATE_TRUNC('day', minute) AS the_date,
      AVG(price)                AS price
    FROM prices.usd
    WHERE blockchain       = 'ethereum'
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND minute >= TIMESTAMP '2024-11-01'
    GROUP BY 1
  ),

  -- 3) every vault reservation belonging to a pool
  reservations AS (
    SELECT r."reservationId" AS reservation_id
    FROM anvil_ethereum.collateralvault_evt_collateralreserved r
    JOIN address_list a ON r.account = a.addr
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
  corrections AS (
    SELECT t.reservation_id, t.term_time AS evt_block_time,
           -SUM(d.delta) AS delta
    FROM terminations t
    JOIN deltas d ON d.reservation_id = t.reservation_id
    GROUP BY t.reservation_id, t.term_time
  ),

  -- 7) daily net flow across all pools
  v3_staked AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS day,
      SUM(delta) / 1e18                 AS daily_staked
    FROM (
      SELECT evt_block_time, delta FROM deltas
      UNION ALL
      SELECT evt_block_time, delta FROM corrections
    )
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
