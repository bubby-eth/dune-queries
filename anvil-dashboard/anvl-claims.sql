-- ANVL Claims
-- Dune query 8425045: https://dune.com/queries/8425045
-- From dashboard: https://dune.com/anvil/anvil
--
-- Weekly ANVL claimed from the Anvil claim contract, with cumulative total
-- and the number of unique claiming accounts. Built on the decoded
-- anvil_ethereum.claim_evt_tokensclaimed table.
--
-- Note: the claim contract distributes the pre-migration ANVL token
-- (0x2ca9...1bfc) -- also after the Oct 2025 token migration; claimers swap
-- into the current token themselves. Claims run from May 2025 and have
-- largely tapered off.
WITH
  claims AS (
    SELECT
      CAST(DATE_TRUNC('week', evt_block_time) AS DATE) AS week,
      byAccount                                        AS account,
      CAST(amount AS double) / 1e18                    AS claimed
    FROM anvil_ethereum.claim_evt_tokensclaimed
  ),

  first_claims AS (
    SELECT account, MIN(week) AS week
    FROM claims
    GROUP BY 1
  ),

  weekly AS (
    SELECT
      c.week,
      SUM(c.claimed)  AS claimed_anvl,
      COUNT(*)        AS claims
    FROM claims c
    GROUP BY 1
  ),

  weekly_new_claimers AS (
    SELECT week, COUNT(*) AS new_claimers
    FROM first_claims
    GROUP BY 1
  )

SELECT
  w.week,
  w.claimed_anvl,
  SUM(w.claimed_anvl) OVER (ORDER BY w.week)             AS cumulative_claimed_anvl,
  w.claims,
  SUM(COALESCE(n.new_claimers, 0)) OVER (ORDER BY w.week) AS cumulative_claimers
FROM weekly w
LEFT JOIN weekly_new_claimers n ON n.week = w.week
ORDER BY 1
