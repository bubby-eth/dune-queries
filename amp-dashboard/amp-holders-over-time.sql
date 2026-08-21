-- Amp Holders Over Time
-- Dune query 381317: https://dune.com/queries/381317
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Daily count of addresses holding any AMP, where effective holdings =
-- wallet balance + staked in Flexa Capacity v2 + staked in Flexa Capacity v3. An address that stakes its entire balance still counts as
-- a holder (staking moves tokens to the custodian, but they remain the
-- staker's AMP).
--
-- Flow ledger per address:
--   * wallet:   every AMP transfer, +to / -from
--   * v2 stake: transfers into the v2 Collateral Manager credit the staker,
--               transfers out debit the recipient (net zero vs the wallet leg,
--               so staking does not change effective holdings)
--   * v3 stake: pool events from raw logs (pools are not decoded on Dune):
--               CollateralStaked(account, token, amount, poolUnitsIssued) -> +units
--               UnstakeInitiated(account, token, unitsToUnstake, ...)     -> -units
--               Units are 1:1 with AMP until a pool claim or reset (none to date).
--               Between unstake initiation and token release the tokens are in
--               transit and count for neither leg.
--
-- Counting uses threshold-crossing events (+1 when an address rises above the
-- threshold, -1 when it falls below) with a running sum over a daily calendar,
-- instead of materializing one row per holder per day.
WITH
  flows AS (
    -- wallet leg: single transfer scan, both sides
    SELECT
      DATE_TRUNC('day', tr.evt_block_time) AS day,
      f.address,
      f.amount
    FROM erc20_ethereum.evt_Transfer tr
    CROSS JOIN UNNEST(
      ARRAY[
        ROW(tr."from", -CAST(tr.value AS int256)),
        ROW(tr."to",    CAST(tr.value AS int256))
      ]
    ) AS f(address, amount)
    WHERE tr.contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2

    UNION ALL

    -- v2 staking leg: net AMP moved into/out of the Collateral Manager,
    -- credited to the staker / debited from the withdrawal recipient
    SELECT
      DATE_TRUNC('day', tr.evt_block_time) AS day,
      f.address,
      f.amount
    FROM erc20_ethereum.evt_Transfer tr
    CROSS JOIN UNNEST(
      ARRAY[
        ROW(CASE WHEN tr."to" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
                 THEN tr."from" END,  CAST(tr.value AS int256)),
        ROW(CASE WHEN tr."from" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
                 THEN tr."to" END,   -CAST(tr.value AS int256))
      ]
    ) AS f(address, amount)
    WHERE tr.contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND (tr."to"   = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
        OR tr."from" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578)
      AND f.address IS NOT NULL

    UNION ALL

    -- v3 staking leg: pool events, credited to the staking account
    SELECT
      DATE_TRUNC('day', block_time) AS day,
      varbinary_substring(topic1, 13, 20) AS address,
      CASE
        WHEN topic0 = 0xa7b456599fe289da1e1af41ace1eaafeb22eb6daaf83cb8c545bb631963aa373
        THEN varbinary_to_int256(varbinary_substring(data, 33, 32))  -- poolUnitsIssued
        ELSE -varbinary_to_int256(varbinary_substring(data, 1, 32))  -- unitsToUnstake
      END AS amount
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

  -- net change per address per active day
  daily AS (
    SELECT day, address, SUM(amount) AS amount
    FROM flows
    WHERE address <> 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
  ),

  -- running effective balance per address, flagged holder / not holder
  running AS (
    SELECT
      day,
      address,
      CASE WHEN SUM(amount) OVER (PARTITION BY address ORDER BY day) > CAST(0 AS int256)
           THEN 1 ELSE 0 END AS above
    FROM daily
  ),

  -- +1 / -1 whenever an address moves between zero and a positive balance
  crossings AS (
    SELECT day, SUM(above - prev_above) AS delta
    FROM (
      SELECT
        day,
        above,
        LAG(above, 1, 0) OVER (PARTITION BY address ORDER BY day) AS prev_above
      FROM running
    )
    WHERE above <> prev_above
    GROUP BY 1
  ),

  days AS (
    SELECT day
    FROM UNNEST(
      SEQUENCE(
        TRY_CAST('2020-08-01' AS TIMESTAMP),
        CAST(DATE_TRUNC('day', CURRENT_TIMESTAMP) AS TIMESTAMP),
        INTERVAL '1' DAY
      )
    ) AS _u(day)
  )

SELECT
  d.day                                        AS "Date",
  SUM(COALESCE(c.delta, 0)) OVER (ORDER BY d.day) AS "AMP Holders",
  COALESCE(c.delta, 0)                         AS "Change in 1 Day"
FROM days d
LEFT JOIN crossings c ON c.day = d.day
ORDER BY 1
