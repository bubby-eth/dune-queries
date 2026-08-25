-- Anvil Governance Proposals
-- Dune query 8453864: https://dune.com/queries/8453864
-- From dashboard: https://dune.com/anvil/anvil
--
-- One row per governance proposal on the AnvilGovernor proxy
-- (0x00e83d0698FAf01BD080A4Dd2927e6aB7C4874c9), newest first, from the
-- decoded anvil_ethereum.anvilgovernordelegate_evt_* tables: title (first
-- line of the proposal description), proposer, vote totals in ANVL
-- (support: 0 = against, 1 = for, 2 = abstain), unique voters, and lifecycle
-- status. voteEnd is a block number (the governor clock runs on blocks):
-- a proposal with no terminal event and voteEnd in the future is Active.
WITH
  proposals AS (
    SELECT
      proposalId,
      proposer,
      description,
      evt_block_time,
      evt_tx_hash,
      voteEnd
    FROM anvil_ethereum.anvilgovernordelegate_evt_proposalcreated
  ),

  votes AS (
    SELECT
      proposalId,
      SUM(CASE WHEN support = 1 THEN CAST(weight AS double) ELSE 0 END) / 1e18 AS for_anvl,
      SUM(CASE WHEN support = 0 THEN CAST(weight AS double) ELSE 0 END) / 1e18 AS against_anvl,
      SUM(CASE WHEN support = 2 THEN CAST(weight AS double) ELSE 0 END) / 1e18 AS abstain_anvl,
      COUNT(DISTINCT voter) AS voters
    FROM anvil_ethereum.anvilgovernordelegate_evt_votecast
    GROUP BY 1
  ),

  queued AS (
    SELECT DISTINCT proposalId
    FROM anvil_ethereum.anvilgovernordelegate_evt_proposalqueued
  ),
  executed AS (
    SELECT DISTINCT proposalId
    FROM anvil_ethereum.anvilgovernordelegate_evt_proposalexecuted
  ),
  canceled AS (
    SELECT DISTINCT proposalId
    FROM anvil_ethereum.anvilgovernordelegate_evt_proposalcanceled
  ),

  tip AS (SELECT MAX(number) AS bn FROM ethereum.blocks)

SELECT
  ROW_NUMBER() OVER (ORDER BY p.evt_block_time DESC)     AS "#",
  '<a href="https://etherscan.io/tx/' || CAST(p.evt_tx_hash AS varchar) ||
    '" target="_blank">' || CAST(CAST(p.evt_block_time AS date) AS varchar) ||
    '</a>'                                               AS created,
  SUBSTR(element_at(split(p.description, chr(10)), 1), 1, 100) AS title,
  SUBSTR(CAST(p.proposer AS varchar), 1, 6) || '...' ||
    SUBSTR(CAST(p.proposer AS varchar), 39) ||
    ' | <a href="https://etherscan.io/address/' || CAST(p.proposer AS varchar) ||
    '" target="_blank">Etherscan</a>'                    AS proposer,
  CASE
    WHEN c.proposalId IS NOT NULL THEN '🚫 Canceled'
    WHEN e.proposalId IS NOT NULL THEN '✅ Executed'
    WHEN q.proposalId IS NOT NULL THEN '⏳ Queued'
    WHEN p.voteEnd >= CAST((SELECT bn FROM tip) AS uint256) THEN '🗳️ Active'
    ELSE '❌ Not passed'
  END                                                    AS status,
  COALESCE(v.for_anvl, 0)                                AS for_anvl,
  COALESCE(v.against_anvl, 0)                            AS against_anvl,
  COALESCE(v.abstain_anvl, 0)                            AS abstain_anvl,
  COALESCE(v.voters, 0)                                  AS voters
FROM proposals p
LEFT JOIN votes v    ON v.proposalId = p.proposalId
LEFT JOIN queued q   ON q.proposalId = p.proposalId
LEFT JOIN executed e ON e.proposalId = p.proposalId
LEFT JOIN canceled c ON c.proposalId = p.proposalId
ORDER BY p.evt_block_time DESC
