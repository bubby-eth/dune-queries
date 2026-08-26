-- Amp Stakers Growth
-- Dune query 8395755: https://dune.com/queries/8395755
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Monthly count of accounts that staked into Flexa Capacity v3, split into
-- first-time stakers (their first stake ever was that month) and returning
-- stakers. total_stakers repeats the all-time unique-staker count on every row
-- for the companion counter widget.
-- Source: decoded anvil_ethereum.timebasedcollateralpool_evt_collateralstaked,
-- filtered to AMP so every current and future Flexa pool is covered without
-- an address list.
WITH
  stakes AS (
    SELECT
      account,
      DATE_TRUNC('month', evt_block_time) AS month
    FROM anvil_ethereum.timebasedcollateralpool_evt_collateralstaked
    WHERE token = 0xff20817765cb7f73d4bde2e66e067e58d11095c2 -- AMP
  ),

  first_stake AS (
    SELECT account, MIN(month) AS first_month
    FROM stakes
    GROUP BY 1
  ),

  monthly AS (
    SELECT
      s.month,
      COUNT(DISTINCT CASE WHEN f.first_month = s.month THEN s.account END) AS new_stakers,
      COUNT(DISTINCT CASE WHEN f.first_month < s.month THEN s.account END) AS returning_stakers
    FROM (SELECT DISTINCT account, month FROM stakes) s
    JOIN first_stake f ON f.account = s.account
    GROUP BY 1
  )

SELECT
  CAST(month AS DATE)                        AS month,
  new_stakers,
  returning_stakers,
  (SELECT COUNT(*) FROM first_stake)         AS total_stakers
FROM monthly
ORDER BY 1
