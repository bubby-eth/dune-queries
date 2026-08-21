-- Amp Staker Distribution
-- Dune query 8395756: https://dune.com/queries/8395756
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Flexa Capacity v3 stakers bucketed by staked size: how many stakers sit in
-- each bucket and what share of the collateral each bucket controls.
-- share_of_staked is a 0-1 fraction (Dune's % format multiplies by 100).
-- Source: pool CollateralStaked / UnstakeInitiated events from raw logs;
-- units convert 1:1 to AMP until a pool claim or reset (none to date).
WITH
  events AS (
    SELECT
      varbinary_substring(topic1, 13, 20) AS account,
      CASE
        WHEN topic0 = 0xa7b456599fe289da1e1af41ace1eaafeb22eb6daaf83cb8c545bb631963aa373
        THEN varbinary_to_int256(varbinary_substring(data, 33, 32))  -- poolUnitsIssued
        ELSE -varbinary_to_int256(varbinary_substring(data, 1, 32))  -- unitsToUnstake
      END AS units
    FROM ethereum.logs
    WHERE contract_address IN (
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
      )
      AND topic0 IN (
        0xa7b456599fe289da1e1af41ace1eaafeb22eb6daaf83cb8c545bb631963aa373, -- CollateralStaked
        0x282129d404496635cd18d83022451839006a0623bada56a71d3b1e204231dbe0  -- UnstakeInitiated
      )
      AND block_time > TRY_CAST('2024-10-01' AS TIMESTAMP)
  ),

  staked_per_account AS (
    SELECT account, SUM(units) / 1e18 AS staked
    FROM events
    GROUP BY 1
    HAVING SUM(units) > CAST(0 AS int256)
  ),

  bucketed AS (
    SELECT
      CASE
        WHEN staked < 100000    THEN '1. < 100k'
        WHEN staked < 1000000   THEN '2. 100k - 1M'
        WHEN staked < 10000000  THEN '3. 1M - 10M'
        WHEN staked < 100000000 THEN '4. 10M - 100M'
        ELSE                         '5. > 100M'
      END AS bucket,
      staked
    FROM staked_per_account
  )

SELECT
  bucket,
  COUNT(*)                                   AS stakers,
  SUM(staked)                                AS amp_staked,
  SUM(staked) / SUM(SUM(staked)) OVER ()     AS share_of_staked
FROM bucketed
GROUP BY 1
ORDER BY 1
