-- Amp Stake Age
-- Dune query 8396304: https://dune.com/queries/8396304
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- How long the currently-staked AMP has been staked, in age buckets.
-- FIFO attribution: unstakes consume an account's OLDEST stakes first, so the
-- units still staked today are each account's most recent stakes up to its
-- current net balance. Each surviving stake keeps its original stake date.
-- share_of_staked is a 0-1 fraction (Dune's % format multiplies by 100).
-- Source: decoded anvil_ethereum.timebasedcollateralpool_evt_* tables,
-- filtered to AMP so every current and future Flexa pool is covered without
-- an address list; units convert 1:1 to AMP until a pool claim or reset
-- (none to date).
WITH
  events AS (
    SELECT
      account,
      evt_block_time                  AS block_time,
      true                            AS is_stake,
      CAST(poolUnitsIssued AS int256) AS units
    FROM anvil_ethereum.timebasedcollateralpool_evt_collateralstaked
    WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2 -- AMP

    UNION ALL

    SELECT
      account,
      evt_block_time,
      false,
      CAST(unitsToUnstake AS int256)
    FROM anvil_ethereum.timebasedcollateralpool_evt_unstakeinitiated
    WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
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
