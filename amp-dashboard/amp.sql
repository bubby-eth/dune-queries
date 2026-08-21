-- Amp
-- Dune query 375548: https://dune.com/queries/375548
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Headline AMP metrics: total supply, market cap (FDV), staked in Flexa
-- Capacity v3, and price. Circulating-supply-based metrics were removed;
-- market cap is fully diluted (total supply x price) by design.
--
-- Staked = reserved collateral across all v3 pools. Pool AMP is custodied by
-- the Anvil CollateralVault (0x5d2725fdE4d7Aa3388DA4519ac0449Cc031d675f) as
-- collateral reservations, reconstructed exactly from vault events:
--   CollateralReserved            -> reservation created with tokenAmount = amount
--   CollateralReservationModified -> tokenAmount: oldAmount -> newAmount
--   CollateralClaimed             -> tokenAmount -= amountWithFee
--                                    (remainderReleased = true deletes the reservation)
--   CollateralReleased            -> reservation deleted (balance -> 0)
WITH
  ----------------------------------------------------------------
  -- 1) Total supply = mints minus burns (zero-address transfers)
  ----------------------------------------------------------------
  ts AS (
    SELECT
      SUM(
        CASE WHEN "from" = 0x0000000000000000000000000000000000000000
             THEN CAST(value AS int256) ELSE -CAST(value AS int256) END
      ) / 1e18 AS total_supply
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND (   "from" = 0x0000000000000000000000000000000000000000
           OR "to"   = 0x0000000000000000000000000000000000000000)
  ),

  ----------------------------------------------------------------
  -- 2) V3 pool addresses (from https://api.flexa.co/collateral_pools)
  ----------------------------------------------------------------
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

  ----------------------------------------------------------------
  -- 3) V3 staked = reserved collateral, from vault events
  ----------------------------------------------------------------
  reservations AS (
    SELECT r."reservationId" AS reservation_id
    FROM anvil_ethereum.collateralvault_evt_collateralreserved r
    JOIN address_list a ON r.account = a.addr
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

  v3_staked AS (
    SELECT
      SUM(CASE WHEN t.reservation_id IS NULL THEN d.delta ELSE CAST(0 AS int256) END) / 1e18 AS staked_tokens
    FROM deltas d
    LEFT JOIN terminated t ON d.reservation_id = t.reservation_id
  ),

  ----------------------------------------------------------------
  -- 4) Latest AMP price
  ----------------------------------------------------------------
  price_query AS (
    SELECT price
    FROM prices.usd
    WHERE blockchain       = 'ethereum'
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    ORDER BY minute DESC NULLS FIRST
    LIMIT 1
  )

SELECT
  -- supply
  ts.total_supply / 1e9                          AS "Total Supply (B)",

  -- staked (v3 only)
  v3.staked_tokens / 1e9                         AS "Staked Tokens (B)",
  v3.staked_tokens / NULLIF(ts.total_supply, 0)  AS "Staked % of Total",

  -- price
  pq.price                                       AS "AMP Price (USD)",

  -- market cap = FDV (total supply x price); both aliases for widget compat
  ts.total_supply * pq.price / 1e6               AS "Market Cap (M)",
  ts.total_supply * pq.price / 1e6               AS "FDV (M)",

  -- raw values
  ts.total_supply                                AS full_total_supply,
  v3.staked_tokens                               AS full_staked_tokens
FROM ts
CROSS JOIN v3_staked v3
CROSS JOIN price_query pq;
