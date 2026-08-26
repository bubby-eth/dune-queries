-- Amp Staking Rewards
-- Dune query 8397618: https://dune.com/queries/8397618
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- Flexa Capacity staking rewards paid from the Reward contract
-- (0x87A07ABF94e1aB709c2e5c3ed4A1FE76901f4593), a Merkle distributor funded
-- with AMP by Flexa. Weekly claimed amounts for the chart, plus repeated
-- scalar columns for the counters: total rewards paid, unique claimants, and
-- the contract's remaining AMP budget (funded minus paid, verified equal to
-- its live balance).
-- Source: decoded amp_ethereum.reward_evt_rewardsclaimed
-- (RewardsClaimed(address indexed byAccount, uint256 amount)).
WITH
  claims AS (
    SELECT
      evt_block_time                AS block_time,
      byAccount                     AS account,
      CAST(amount AS double) / 1e18 AS amp
    FROM amp_ethereum.reward_evt_rewardsclaimed
  ),

  funding AS (
    SELECT
      SUM(CASE WHEN "to" = 0x87A07ABF94e1aB709c2e5c3ed4A1FE76901f4593
               THEN CAST(value AS int256) ELSE -CAST(value AS int256) END) / 1e18 AS net_balance
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND (   "to"   = 0x87A07ABF94e1aB709c2e5c3ed4A1FE76901f4593
           OR "from" = 0x87A07ABF94e1aB709c2e5c3ed4A1FE76901f4593)
  )

SELECT
  CAST(DATE_TRUNC('week', block_time) AS DATE)   AS week,
  SUM(amp)                                       AS amp_claimed,
  COUNT(*)                                       AS claims,
  (SELECT SUM(amp) FROM claims)                  AS total_claimed,
  (SELECT COUNT(DISTINCT account) FROM claims)   AS claimants,
  (SELECT net_balance FROM funding)              AS remaining_budget
FROM claims
GROUP BY 1
ORDER BY 1
