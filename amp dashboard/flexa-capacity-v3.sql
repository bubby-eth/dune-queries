-- Flexa Capacity v3 pool summary: net tokens (reserved collateral), USD value,
-- deposit count, and share of total.
--
-- Each pool's AMP is custodied by the Anvil CollateralVault
-- (0x5d2725fdE4d7Aa3388DA4519ac0449Cc031d675f) as a collateral reservation;
-- net_tokens = reservation.tokenAmount, the amount actually backing Flexa payments.
-- Reconstructed from vault events (exact, per the CollateralVault source):
--   CollateralReserved            -> reservation created with tokenAmount = amount
--   CollateralReservationModified -> tokenAmount: oldAmount -> newAmount
--   CollateralClaimed             -> tokenAmount -= amountWithFee
--                                    (remainderReleased = true deletes the reservation)
--   CollateralReleased            -> reservation deleted (balance -> 0)
-- Reservation ids are never reused, so a deleted reservation contributes 0 forever,
-- and a pool reset (new reservation id) is picked up automatically.
-- deposits = number of reservation increases (initial reserve + upward modifications),
-- i.e. processed stakes, regardless of how the stake was submitted (direct, multisig,
-- depositAndStake, stakeReleasableTokensFrom).
WITH
  -- 1) current AMP price
  price_query AS (
    SELECT price
    FROM prices.usd
    WHERE blockchain       = 'ethereum'
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    ORDER BY minute DESC NULLS FIRST
    LIMIT 1
  ),

  -- 2) mapping each pool address to its pool_name
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

  -- 3) every vault reservation belonging to a pool
  reservations AS (
    SELECT r."reservationId" AS reservation_id, pm.pool_name
    FROM anvil_ethereum.collateralvault_evt_collateralreserved r
    JOIN pool_map pm ON r.account = pm.addr
  ),

  -- 4) reservations that were terminated (balance is exactly 0 from then on)
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

  -- 5) signed balance changes per reservation
  deltas AS (
    SELECT "reservationId" AS reservation_id,
           CAST(amount AS int256) AS delta
    FROM anvil_ethereum.collateralvault_evt_collateralreserved
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)

    UNION ALL

    SELECT "reservationId",
           CAST("newAmount" AS int256) - CAST("oldAmount" AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateralreservationmodified
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)

    UNION ALL

    SELECT "reservationId",
           -CAST("amountWithFee" AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)
      AND "remainderReleased" = FALSE
  ),

  -- 6) sum up every flow type for each pool
  net_tokens_by_pool AS (
    SELECT
      res.pool_name,
      SUM(CASE WHEN t.reservation_id IS NULL THEN d.delta ELSE CAST(0 AS int256) END) / 1e18 AS net_tokens,
      COUNT_IF(d.delta > 0) AS deposits
    FROM deltas d
    JOIN reservations res ON d.reservation_id = res.reservation_id
    LEFT JOIN terminated t ON d.reservation_id = t.reservation_id
    GROUP BY res.pool_name
  )

-- 7) final USD-valued summary
SELECT
  n.pool_name,
  n.net_tokens,
  n.net_tokens * p.price                            AS usd_value,
  n.deposits,
  100.0 * n.net_tokens / SUM(n.net_tokens) OVER ()  AS pct
FROM net_tokens_by_pool n
CROSS JOIN price_query p
ORDER BY usd_value DESC;
