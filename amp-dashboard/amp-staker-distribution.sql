-- Amp Staker Distribution
-- Dune query 8395756: https://dune.com/queries/8395756
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Flexa Capacity v3 stakers bucketed by staked size: how many stakers sit in
-- each bucket and what share of the collateral each bucket controls.
-- share_of_staked is a 0-1 fraction (Dune's % format multiplies by 100).
-- Source: decoded anvil_ethereum.timebasedcollateralpool_evt_* tables,
-- filtered to AMP so every current and future Flexa pool is covered without
-- an address list; units convert 1:1 to AMP until a pool claim or reset
-- (none to date).
WITH
  events AS (
    SELECT account, CAST(poolUnitsIssued AS int256) AS units
    FROM anvil_ethereum.timebasedcollateralpool_evt_collateralstaked
    WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2 -- AMP

    UNION ALL

    SELECT account, -CAST(unitsToUnstake AS int256)
    FROM anvil_ethereum.timebasedcollateralpool_evt_unstakeinitiated
    WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
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
