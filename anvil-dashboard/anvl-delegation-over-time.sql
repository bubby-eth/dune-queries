-- ANVL Delegation Over Time
-- Dune query 8425043: https://dune.com/queries/8425043
-- From dashboard: https://dune.com/anvil/anvil
--
-- Weekly total delegated ANVL voting power and its share of total supply,
-- for the ANVL token (0xAEEA...5597, the post-migration token that live
-- governance verifiably uses). Parsed from raw logs because this token is
-- not yet decoded on Dune.
--
-- Total delegated power is the running sum of DelegateVotesChanged deltas
-- (newVotes - previousVotes), carried forward through weeks with no events.
WITH
  -- DelegateVotesChanged(address indexed delegate, uint256 previousVotes, uint256 newVotes)
  deltas AS (
    SELECT
      CAST(DATE_TRUNC('week', block_time) AS DATE) AS week,
      SUM(  CAST(varbinary_to_uint256(varbinary_substring(data, 33, 32)) AS int256)
          - CAST(varbinary_to_uint256(varbinary_substring(data,  1, 32)) AS int256)) AS delta
    FROM ethereum.logs
    WHERE contract_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
      AND topic0 = 0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724
      -- scan bound: token deployed Oct 2025
      AND block_time >= TIMESTAMP '2025-10-01'
    GROUP BY 1
  ),

  weeks AS (
    SELECT CAST(w AS DATE) AS week
    FROM UNNEST(SEQUENCE(
      (SELECT MIN(week) FROM deltas),
      CAST(DATE_TRUNC('week', NOW()) AS DATE),
      INTERVAL '7' day
    )) AS t(w)
  ),

  supply AS (
    SELECT SUM(CASE WHEN "from" = 0x0000000000000000000000000000000000000000
                    THEN CAST(value AS int256) ELSE -CAST(value AS int256) END) / 1e18 AS total
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
      AND evt_block_time >= TIMESTAMP '2025-10-01'
      AND (   "from" = 0x0000000000000000000000000000000000000000
           OR "to"   = 0x0000000000000000000000000000000000000000)
  )

SELECT
  w.week,
  SUM(COALESCE(d.delta, CAST(0 AS int256))) OVER (ORDER BY w.week) / 1e18          AS delegated_anvl,
  100.0 * (SUM(COALESCE(d.delta, CAST(0 AS int256))) OVER (ORDER BY w.week) / 1e18)
        / s.total                                                                   AS delegated_pct
FROM weeks w
LEFT JOIN deltas d ON d.week = w.week
CROSS JOIN supply s
ORDER BY 1
