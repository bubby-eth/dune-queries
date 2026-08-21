-- Amp Pool Momentum
-- Dune query 8396301: https://dune.com/queries/8396301
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- 30-day change in reserved collateral per Flexa Capacity v3 pool (signed),
-- alongside each pool's current reserved balance and the change as a fraction
-- of where the pool stood 30 days ago (0-1; Dune's % format multiplies by 100).
-- Derived from the exact CollateralVault event reconstruction.
--
-- NOTE: Flexa boosts one pool's APY each month, so large swings usually
-- reflect stakers rotating toward the boosted pool rather than collateral
-- entering or leaving Flexa (e.g. Aug 2026: Solana -88.6% / boosted Lightning
-- +75%, near 1:1). See the Amp Boost Migration query for the rotation vs
-- new-capital decomposition.
WITH
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

  reservations AS (
    SELECT r."reservationId" AS reservation_id, pm.pool_name
    FROM anvil_ethereum.collateralvault_evt_collateralreserved r
    JOIN pool_map pm ON r.account = pm.addr
  ),

  terminated AS (
    SELECT "reservationId" AS reservation_id
    FROM anvil_ethereum.collateralvault_evt_collateralreleased
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)
    UNION
    SELECT "reservationId"
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)
      AND "remainderReleased" = TRUE
  ),

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
      AND "remainderReleased" = FALSE
  ),

  per_pool AS (
    SELECT
      res.pool_name,
      SUM(CASE WHEN t.reservation_id IS NULL THEN d.delta ELSE CAST(0 AS int256) END) / 1e18 AS reserved_now,
      SUM(CASE WHEN t.reservation_id IS NULL AND d.evt_block_time >= NOW() - INTERVAL '30' day
               THEN d.delta ELSE CAST(0 AS int256) END) / 1e18 AS change_30d
    FROM deltas d
    JOIN reservations res ON d.reservation_id = res.reservation_id
    LEFT JOIN terminated t ON d.reservation_id = t.reservation_id
    GROUP BY 1
  )

SELECT
  pool_name,
  change_30d,
  reserved_now,
  change_30d / NULLIF(reserved_now - change_30d, 0) AS change_pct
FROM per_pool
ORDER BY change_30d DESC
