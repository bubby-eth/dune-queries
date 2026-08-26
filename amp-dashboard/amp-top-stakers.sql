-- Amp Top Stakers
-- Dune query 8396305: https://dune.com/queries/8396305
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Top 25 Flexa Capacity v3 staking accounts: staked AMP, share of all staked
-- collateral (0-1 fraction), and first/latest staking activity.
--   * staker: truncated address with Etherscan / DeBank profile links
--   * first_stake / last_activity: dates linked to their tx on Etherscan;
--     last_activity is suffixed with the event kind: (stake) / (unstake)
-- Source: decoded anvil_ethereum.timebasedcollateralpool_evt_* tables,
-- filtered to AMP so every current and future Flexa pool is covered without
-- an address list; units convert 1:1 to AMP until a pool claim or reset
-- (none to date).
WITH
  events AS (
    SELECT
      account,
      evt_block_time                    AS block_time,
      evt_tx_hash                       AS tx_hash,
      'stake'                           AS kind,
      CAST(poolUnitsIssued AS int256)   AS units
    FROM anvil_ethereum.timebasedcollateralpool_evt_collateralstaked
    WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2 -- AMP

    UNION ALL

    SELECT
      account,
      evt_block_time,
      evt_tx_hash,
      'unstake',
      -CAST(unitsToUnstake AS int256)
    FROM anvil_ethereum.timebasedcollateralpool_evt_unstakeinitiated
    WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
  ),

  per_account AS (
    SELECT
      account,
      SUM(units) / 1e18            AS staked,
      MIN(block_time)              AS first_activity,
      MIN_BY(tx_hash, block_time)  AS first_tx,
      MAX(block_time)              AS last_activity,
      MAX_BY(tx_hash, block_time)  AS last_tx,
      MAX_BY(kind, block_time)     AS last_kind
    FROM events
    GROUP BY 1
    HAVING SUM(units) > CAST(0 AS int256)
  )

SELECT
  ROW_NUMBER() OVER (ORDER BY p.staked DESC) AS rank,
  SUBSTR(CAST(p.account AS varchar), 1, 6) || '...' ||
    SUBSTR(CAST(p.account AS varchar), 39) ||
    ' | <a href="https://etherscan.io/address/' || CAST(p.account AS varchar) ||
    '" target="_blank">Etherscan</a>' ||
    ' | <a href="https://debank.com/profile/' || CAST(p.account AS varchar) ||
    '" target="_blank">DeBank</a>' AS staker,
  p.staked                                  AS staked_amp,
  p.staked / SUM(p.staked) OVER ()          AS share_of_staked,
  '<a href="https://etherscan.io/tx/' || CAST(p.first_tx AS varchar) ||
    '" target="_blank">' || CAST(CAST(p.first_activity AS date) AS varchar) ||
    '</a>' AS first_stake,
  '<a href="https://etherscan.io/tx/' || CAST(p.last_tx AS varchar) ||
    '" target="_blank">' || CAST(CAST(p.last_activity AS date) AS varchar) ||
    ' (' || p.last_kind || ')</a>' AS last_activity
FROM per_account p
ORDER BY p.staked DESC
LIMIT 25
