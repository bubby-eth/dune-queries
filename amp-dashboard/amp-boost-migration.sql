-- Amp Boost Migration
-- Dune query 8396972: https://dune.com/queries/8396972
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Monthly decomposition of Flexa Capacity v3 staking activity into:
--   rotated     - AMP a staker unstaked and restaked within the same month
--                 (APY-boost chasing between pools; nets zero new collateral)
--   new_capital - staked amount exceeding the same account's unstakes that
--                 month (genuinely new collateral)
--   exited      - unstaked amount never restaked that month (emitted negative)
-- Per account and month: rotated = LEAST(staked, unstaked); the excess on
-- either side is new capital or exit.
--
-- Context: Flexa boosts one pool's APY each month, so stakers migrate
-- (e.g. Aug 2026: Solana -88.6%, boosted Lightning +75%, near 1:1).
-- Caveat: a migration straddling a month boundary (unstake late in month M,
-- restake in M+1 after epoch release) counts as an exit in M and new capital
-- in M+1. Units convert 1:1 to AMP until a pool claim or reset (none to date).
-- Source: decoded anvil_ethereum.timebasedcollateralpool_evt_* tables,
-- filtered to AMP so every current and future Flexa pool is covered without
-- an address list.
WITH
  events AS (
    SELECT
      account,
      DATE_TRUNC('month', evt_block_time) AS month,
      true                                AS is_stake,
      CAST(poolUnitsIssued AS int256)     AS units
    FROM anvil_ethereum.timebasedcollateralpool_evt_collateralstaked
    WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2 -- AMP

    UNION ALL

    SELECT
      account,
      DATE_TRUNC('month', evt_block_time),
      false,
      CAST(unitsToUnstake AS int256)
    FROM anvil_ethereum.timebasedcollateralpool_evt_unstakeinitiated
    WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
  ),

  per_account_month AS (
    SELECT
      account,
      month,
      SUM(CASE WHEN is_stake THEN units ELSE CAST(0 AS int256) END) / 1e18     AS staked,
      SUM(CASE WHEN NOT is_stake THEN units ELSE CAST(0 AS int256) END) / 1e18 AS unstaked
    FROM events
    GROUP BY 1, 2
  )

SELECT
  CAST(month AS DATE)                          AS month,
  SUM(GREATEST(staked - unstaked, 0))          AS new_capital,
  SUM(LEAST(staked, unstaked))                 AS rotated,
  -SUM(GREATEST(unstaked - staked, 0))         AS exited,
  SUM(staked - unstaked)                       AS net
FROM per_account_month
GROUP BY 1
ORDER BY 1
