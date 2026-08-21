-- Amp CEX Withdrawals/Deposits Last 30 Days
-- Dune query 1017568: https://dune.com/queries/1017568
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- AMP deposited to / withdrawn from centralized exchanges over the last 30
-- days, per exchange. Single transfer scan; each transfer contributes a
-- deposit leg (receiver is a CEX) and/or a withdrawal leg (sender is a CEX).
-- Transfers between addresses of the SAME exchange (internal shuffles) are
-- excluded from both legs; transfers between two different exchanges count
-- as a withdrawal from one and a deposit to the other.
WITH
  -- one label per address (cex.addresses can list an address on several
  -- chains; unfiltered joins would double-count)
  cex_eth AS (
    SELECT DISTINCT address, cex_name
    FROM cex.addresses
    WHERE blockchain = 'ethereum'
  ),

  legs AS (
    SELECT f.role, f.cex_name, et.value
    FROM erc20_ethereum.evt_Transfer et
    LEFT JOIN cex_eth ct ON ct.address = et."to"
    LEFT JOIN cex_eth cf ON cf.address = et."from"
    CROSS JOIN UNNEST(
      ARRAY[
        ROW('deposit',    ct.cex_name),
        ROW('withdrawal', cf.cex_name)
      ]
    ) AS f(role, cex_name)
    WHERE et.contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND et.evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '30' day
      AND (ct.address IS NOT NULL OR cf.address IS NOT NULL)
      AND f.cex_name IS NOT NULL
      AND (ct.cex_name IS NULL OR cf.cex_name IS NULL OR ct.cex_name <> cf.cex_name)
  )

SELECT
  cex_name                                                          AS exchange,
  COALESCE(SUM(CASE WHEN role = 'withdrawal' THEN CAST(value AS int256) END), CAST(0 AS int256)) / 1e18 AS withdrawals,
  -COALESCE(SUM(CASE WHEN role = 'deposit'   THEN CAST(value AS int256) END), CAST(0 AS int256)) / 1e18 AS deposits,
  (COALESCE(SUM(CASE WHEN role = 'withdrawal' THEN CAST(value AS int256) END), CAST(0 AS int256))
   - COALESCE(SUM(CASE WHEN role = 'deposit'  THEN CAST(value AS int256) END), CAST(0 AS int256))) / 1e18 AS netflow
FROM legs
GROUP BY 1
ORDER BY 1
