-- Current reserved collateral for the Flexa Capacity v3 "Lightning" pool
-- (0xd0415cf4558A0dBEE8242498D25284476bE3c8f2).
--
-- The pool's AMP is custodied by the Anvil CollateralVault
-- (0x5d2725fdE4d7Aa3388DA4519ac0449Cc031d675f) as a collateral reservation;
-- "reserved" = reservation.tokenAmount, the amount actually backing Flexa payments.
-- Reconstructed from vault events (exact, per the CollateralVault source):
--   CollateralReserved            -> reservation created with tokenAmount = amount
--   CollateralReservationModified -> tokenAmount: oldAmount -> newAmount
--   CollateralClaimed             -> tokenAmount -= amountWithFee
--                                    (remainderReleased = true deletes the reservation)
--   CollateralReleased            -> reservation deleted (balance -> 0)
-- Reservation ids are never reused, so a deleted reservation contributes 0 forever.
-- Covers all of the pool's reservations, so a resetPool (new reservation id) is
-- picked up automatically.
WITH
  pool_reservations AS (
    SELECT "reservationId" AS reservation_id
    FROM anvil_ethereum.collateralvault_evt_collateralreserved
    WHERE account = 0xd0415cf4558A0dBEE8242498D25284476bE3c8f2
  ),

  -- reservations that were terminated (balance is exactly 0 from then on)
  terminated AS (
    SELECT "reservationId" AS reservation_id
    FROM anvil_ethereum.collateralvault_evt_collateralreleased
    WHERE "reservationId" IN (SELECT reservation_id FROM pool_reservations)
    UNION
    SELECT "reservationId"
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed
    WHERE "reservationId" IN (SELECT reservation_id FROM pool_reservations)
      AND "remainderReleased" = TRUE
  ),

  deltas AS (
    SELECT "reservationId" AS reservation_id, evt_block_time,
           CAST(amount AS int256) AS delta
    FROM anvil_ethereum.collateralvault_evt_collateralreserved
    WHERE "reservationId" IN (SELECT reservation_id FROM pool_reservations)

    UNION ALL

    SELECT "reservationId", evt_block_time,
           CAST("newAmount" AS int256) - CAST("oldAmount" AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateralreservationmodified
    WHERE "reservationId" IN (SELECT reservation_id FROM pool_reservations)

    UNION ALL

    SELECT "reservationId", evt_block_time,
           -CAST("amountWithFee" AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed
    WHERE "reservationId" IN (SELECT reservation_id FROM pool_reservations)
      AND "remainderReleased" = FALSE
  ),

  reserved_now AS (
    SELECT
      SUM(CASE WHEN t.reservation_id IS NULL THEN d.delta ELSE CAST(0 AS int256) END) AS reserved_raw,
      MAX(d.evt_block_time) AS last_change
    FROM deltas d
    LEFT JOIN terminated t ON d.reservation_id = t.reservation_id
  ),

  amp_price AS (
    SELECT price
    FROM prices.usd
    WHERE blockchain = 'ethereum'
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    ORDER BY minute DESC NULLS FIRST
    LIMIT 1
  )

SELECT
  r.reserved_raw / 1e18          AS reserved_amp,
  r.reserved_raw / 1e18 * p.price AS reserved_usd,
  r.last_change
FROM reserved_now r
CROSS JOIN amp_price p;
