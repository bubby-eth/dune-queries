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
--   * v3 stake: decoded pool events (anvil_ethereum.timebasedcollateralpool_evt_*):
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

    -- v3 staking leg: decoded pool events (all AMP pools, no address list
    -- needed), credited to the staking account
    SELECT
      DATE_TRUNC('day', evt_block_time)  AS day,
      account                            AS address,
      CAST(poolUnitsIssued AS int256)    AS amount
    FROM anvil_ethereum.timebasedcollateralpool_evt_collateralstaked
    WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2 -- AMP

    UNION ALL

    SELECT
      DATE_TRUNC('day', evt_block_time),
      account,
      -CAST(unitsToUnstake AS int256)
    FROM anvil_ethereum.timebasedcollateralpool_evt_unstakeinitiated
    WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
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
