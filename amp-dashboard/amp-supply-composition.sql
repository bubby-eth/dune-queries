-- Amp Supply Composition
-- Dune query 8395760: https://dune.com/queries/8395760
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Where the AMP supply sits, daily: staked in Flexa Capacity v3 (reserved
-- collateral), staked in Flexa Capacity v2 (Collateral Manager balance),
-- held on tracked centralized exchanges, or anywhere else.
-- Long format (day, category, amp) for a stacked/100% area chart.
--
-- Caveats:
--   * "On exchanges" covers only addresses in Dune's cex.addresses list.
--   * "Other" includes DEX pools and the v3 vault's unreserved exit liquidity.
-- Staked series uses the exact CollateralVault event reconstruction (see
-- flexa-capacity-v3.sql); terminated reservations get a correction delta at
-- termination time so history nets to zero.
WITH
  days AS (
    SELECT day
    FROM UNNEST(SEQUENCE(
      TRY_CAST('2020-08-01' AS TIMESTAMP),
      CAST(DATE_TRUNC('day', CURRENT_TIMESTAMP) AS TIMESTAMP),
      INTERVAL '1' DAY
    )) AS _u(day)
  ),

  -- total supply: mints minus burns, daily
  supply_deltas AS (
    SELECT DATE_TRUNC('day', evt_block_time) AS day,
           SUM(CASE WHEN "from" = 0x0000000000000000000000000000000000000000
                    THEN CAST(value AS int256) ELSE -CAST(value AS int256) END) AS delta
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND (   "from" = 0x0000000000000000000000000000000000000000
           OR "to"   = 0x0000000000000000000000000000000000000000)
    GROUP BY 1
  ),

  -- AMP held by tracked CEX addresses, daily net change
  cex_eth AS (
    SELECT DISTINCT address FROM cex.addresses WHERE blockchain = 'ethereum'
  ),
  cex_deltas AS (
    SELECT DATE_TRUNC('day', tr.evt_block_time) AS day,
           SUM(CASE
                 WHEN ct.address IS NOT NULL AND cf.address IS NULL THEN  CAST(tr.value AS int256)
                 WHEN cf.address IS NOT NULL AND ct.address IS NULL THEN -CAST(tr.value AS int256)
                 ELSE CAST(0 AS int256)
               END) AS delta
    FROM erc20_ethereum.evt_Transfer tr
    LEFT JOIN cex_eth ct ON ct.address = tr."to"
    LEFT JOIN cex_eth cf ON cf.address = tr."from"
    WHERE tr.contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND (ct.address IS NOT NULL OR cf.address IS NOT NULL)
    GROUP BY 1
  ),

  -- Flexa Capacity v2: net AMP held by the Collateral Manager, daily change
  -- (self-transfers contribute +v and -v and cancel)
  v2_deltas AS (
    SELECT DATE_TRUNC('day', evt_block_time) AS day,
           SUM(  (CASE WHEN "to"   = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
                       THEN CAST(value AS int256) ELSE CAST(0 AS int256) END)
               - (CASE WHEN "from" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
                       THEN CAST(value AS int256) ELSE CAST(0 AS int256) END)) AS delta
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND (   "to"   = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
           OR "from" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578)
    GROUP BY 1
  ),

  -- v3 reserved collateral, daily net change (vault events + terminations)
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
  reservations AS (
    SELECT r."reservationId" AS reservation_id
    FROM anvil_ethereum.collateralvault_evt_collateralreserved r
    JOIN address_list a ON r.account = a.addr
  ),
  vault_deltas AS (
    SELECT "reservationId" AS reservation_id, evt_block_time, CAST(amount AS int256) AS delta
    FROM anvil_ethereum.collateralvault_evt_collateralreserved
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)
    UNION ALL
    SELECT "reservationId", evt_block_time, CAST("newAmount" AS int256) - CAST("oldAmount" AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateralreservationmodified
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)
    UNION ALL
    SELECT "reservationId", evt_block_time, -CAST("amountWithFee" AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)
    UNION ALL
    SELECT "reservationId", evt_block_time, -CAST(amount AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateralreleased
    WHERE "reservationId" IN (SELECT reservation_id FROM reservations)
  ),
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
    GROUP BY 1
  ),
  corrections AS (
    SELECT t.term_time AS evt_block_time, -SUM(d.delta) AS delta
    FROM terminations t
    JOIN vault_deltas d ON d.reservation_id = t.reservation_id
    GROUP BY t.reservation_id, t.term_time
  ),
  staked_deltas AS (
    SELECT DATE_TRUNC('day', evt_block_time) AS day, SUM(delta) AS delta
    FROM (
      SELECT evt_block_time, delta FROM vault_deltas
      UNION ALL
      SELECT evt_block_time, delta FROM corrections
    )
    GROUP BY 1
  ),

  -- daily cumulative levels
  levels AS (
    SELECT
      d.day,
      SUM(COALESCE(s.delta, CAST(0 AS int256))) OVER (ORDER BY d.day) / 1e18 AS supply,
      SUM(COALESCE(c.delta, CAST(0 AS int256))) OVER (ORDER BY d.day) / 1e18 AS on_cex,
      SUM(COALESCE(v.delta, CAST(0 AS int256))) OVER (ORDER BY d.day) / 1e18 AS staked,
      SUM(COALESCE(m.delta, CAST(0 AS int256))) OVER (ORDER BY d.day) / 1e18 AS staked_v2
    FROM days d
    LEFT JOIN supply_deltas s ON s.day = d.day
    LEFT JOIN cex_deltas    c ON c.day = d.day
    LEFT JOIN staked_deltas v ON v.day = d.day
    LEFT JOIN v2_deltas     m ON m.day = d.day
  )

SELECT day, category, amp
FROM levels
CROSS JOIN UNNEST(
  ARRAY[
    ROW('Staked (Flexa v3)', staked),
    ROW('Staked (Flexa v2)', staked_v2),
    ROW('On exchanges',      on_cex),
    ROW('Other',             supply - on_cex - staked - staked_v2)
  ]
) AS t(category, amp)
ORDER BY day, category
