-- Anvil Governance Participation
-- Dune query 8453865: https://dune.com/queries/8453865
-- From dashboard: https://dune.com/anvil/anvil
--
-- Voting activity per governance proposal over time, from the decoded
-- AnvilGovernor proxy tables: ANVL weight cast for / against / abstain
-- (stacked columns) and the number of unique voters (line). One row per
-- proposal, keyed by its creation date, so the chart shows whether
-- participation is growing or decaying across proposals.
SELECT
  CAST(p.evt_block_date AS date)                         AS proposal_date,
  SUBSTR(element_at(split(p.description, chr(10)), 1), 1, 60) AS title,
  SUM(CASE WHEN v.support = 1 THEN CAST(v.weight AS double) ELSE 0 END) / 1e18 AS for_anvl,
  SUM(CASE WHEN v.support = 0 THEN CAST(v.weight AS double) ELSE 0 END) / 1e18 AS against_anvl,
  SUM(CASE WHEN v.support = 2 THEN CAST(v.weight AS double) ELSE 0 END) / 1e18 AS abstain_anvl,
  COUNT(DISTINCT v.voter)                                AS voters
FROM anvil_ethereum.anvilgovernordelegate_evt_proposalcreated p
LEFT JOIN anvil_ethereum.anvilgovernordelegate_evt_votecast v
  ON v.proposalId = p.proposalId
GROUP BY 1, 2
ORDER BY 1
