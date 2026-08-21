-- Amp Claims Record
-- Dune query 8395759: https://dune.com/queries/8395759
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- The zero-claims record: number of successful collateral claims against any
-- Flexa Capacity v3 pool reservation (a claim is Flexa drawing staked AMP to
-- cover a payment default), and how many days that record has been running
-- since the first pool reservation was created (2024-11-08).
-- Source: CollateralClaimed events on the pools' vault reservations. The one
-- claim ever attempted (Lightning, tx-level) reverted and emitted nothing.
WITH
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
    SELECT r."reservationId" AS reservation_id, r.evt_block_time
    FROM anvil_ethereum.collateralvault_evt_collateralreserved r
    JOIN address_list a ON r.account = a.addr
  )

SELECT
  (SELECT COUNT(*)
   FROM anvil_ethereum.collateralvault_evt_collateralclaimed
   WHERE "reservationId" IN (SELECT reservation_id FROM reservations)) AS collateral_claims,
  DATE_DIFF('day',
            (SELECT MIN(evt_block_time) FROM reservations),
            NOW())                                                     AS days_running
