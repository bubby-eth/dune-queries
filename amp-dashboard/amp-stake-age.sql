-- Amp Stake Age
-- Dune query 8396304: https://dune.com/queries/8396304
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- How long the currently-staked AMP has been staked, in age buckets.
-- FIFO attribution: unstakes consume an account's OLDEST stakes first, so the
-- units still staked today are each account's most recent stakes up to its
-- current net balance. Each surviving stake keeps its original stake date.
-- share_of_staked is a 0-1 fraction (Dune's % format multiplies by 100).
-- Source: pool CollateralStaked / UnstakeInitiated events from raw logs;
-- units convert 1:1 to AMP until a pool claim or reset (none to date).
WITH
  events AS (
    SELECT
      varbinary_substring(topic1, 13, 20) AS account,
      block_time,
      topic0 = 0xa7b456599fe289da1e1af41ace1eaafeb22eb6daaf83cb8c545bb631963aa373 AS is_stake,
      CASE
        WHEN topic0 = 0xa7b456599fe289da1e1af41ace1eaafeb22eb6daaf83cb8c545bb631963aa373
        THEN varbinary_to_int256(varbinary_substring(data, 33, 32))  -- poolUnitsIssued
        ELSE varbinary_to_int256(varbinary_substring(data, 1, 32))   -- unitsToUnstake
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

  net_per_account AS (
    SELECT account,
           SUM(CASE WHEN is_stake THEN units ELSE -units END) AS net_units
    FROM events
    GROUP BY 1
    HAVING SUM(CASE WHEN is_stake THEN units ELSE -units END) > CAST(0 AS int256)
  ),

  -- newest-first running total of each account's stakes; the portion of each
  -- stake still held is what fits inside the account's net balance
  surviving AS (
    SELECT
      s.account,
      s.block_time,
      GREATEST(
        CAST(0 AS int256),
        LEAST(s.units, n.net_units - (s.running - s.units))
      ) AS surviving_units
    FROM (
      SELECT account, block_time, units,
             SUM(units) OVER (PARTITION BY account ORDER BY block_time DESC
                              ROWS UNBOUNDED PRECEDING) AS running
      FROM events
      WHERE is_stake
    ) s
    JOIN net_per_account n ON n.account = s.account
  )

SELECT
  CASE
    WHEN block_time >= NOW() - INTERVAL '30'  day THEN '1. < 1 month'
    WHEN block_time >= NOW() - INTERVAL '90'  day THEN '2. 1-3 months'
    WHEN block_time >= NOW() - INTERVAL '180' day THEN '3. 3-6 months'
    WHEN block_time >= NOW() - INTERVAL '365' day THEN '4. 6-12 months'
    ELSE                                               '5. > 12 months'
  END                                           AS age_bucket,
  SUM(surviving_units) / 1e18                   AS amp_staked,
  (SUM(surviving_units) / 1e18)
    / SUM(SUM(surviving_units) / 1e18) OVER ()  AS share_of_staked,
  COUNT(DISTINCT CASE WHEN surviving_units > CAST(0 AS int256) THEN account END) AS stakers
FROM surviving
WHERE surviving_units > CAST(0 AS int256)
GROUP BY 1
ORDER BY 1
