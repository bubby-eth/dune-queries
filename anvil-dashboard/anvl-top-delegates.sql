-- ANVL Top Delegates
-- Dune query 8425041: https://dune.com/queries/8425041
-- From dashboard: https://dune.com/anvil/anvil
--
-- Current governance voting power per delegate for the ANVL token
-- (0xAEEA...5597, the post-migration token that live governance verifiably
-- uses -- recent VoteCast weights match its delegation records).
-- Parsed from raw logs because this token is not yet decoded on Dune
-- (anvil_ethereum.anvil_evt_* covers only the pre-migration token
-- 0x2ca9...1bfc).
--
-- voting power = latest DelegateVotesChanged.newVotes per delegate
-- delegators   = accounts whose latest DelegateChanged.toDelegate is this
--                delegate
-- pct_supply   = share of total supply (mints minus burns; fixed 100B)
WITH
  -- DelegateVotesChanged(address indexed delegate, uint256 previousVotes, uint256 newVotes)
  votes_changed AS (
    SELECT
      varbinary_substring(topic1, 13, 20)                          AS delegate,
      varbinary_to_uint256(varbinary_substring(data, 33, 32))      AS new_votes,
      block_number * CAST(100000 AS bigint) + index                AS ord
    FROM ethereum.logs
    WHERE contract_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
      AND topic0 = 0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724
      -- scan bound: token deployed Oct 2025
      AND block_time >= TIMESTAMP '2025-10-01'
  ),

  current_power AS (
    SELECT delegate, MAX_BY(new_votes, ord) AS votes
    FROM votes_changed
    GROUP BY 1
  ),

  -- DelegateChanged(address indexed delegator, address indexed fromDelegate, address indexed toDelegate)
  delegations AS (
    SELECT
      varbinary_substring(topic1, 13, 20)            AS delegator,
      varbinary_substring(topic3, 13, 20)            AS to_delegate,
      block_number * CAST(100000 AS bigint) + index  AS ord
    FROM ethereum.logs
    WHERE contract_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
      AND topic0 = 0x3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f
      AND block_time >= TIMESTAMP '2025-10-01'
  ),

  current_delegations AS (
    SELECT delegator, MAX_BY(to_delegate, ord) AS delegate
    FROM delegations
    GROUP BY 1
  ),

  delegator_counts AS (
    SELECT delegate, COUNT(*) AS delegators
    FROM current_delegations
    WHERE delegate <> 0x0000000000000000000000000000000000000000
    GROUP BY 1
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
  ROW_NUMBER() OVER (ORDER BY p.votes DESC)                       AS rank,
  '<a href="https://etherscan.io/address/' || CAST(p.delegate AS varchar) ||
    '" target="_blank">' || CAST(p.delegate AS varchar) || '</a>' AS delegate,
  CAST(p.votes AS double) / 1e18                                  AS voting_power,
  100.0 * CAST(p.votes AS double) / 1e18 / s.total                AS pct_supply,
  COALESCE(d.delegators, 0)                                       AS delegators
FROM current_power p
LEFT JOIN delegator_counts d ON d.delegate = p.delegate
CROSS JOIN supply s
WHERE p.votes > UINT256 '0'
ORDER BY p.votes DESC
LIMIT 25
