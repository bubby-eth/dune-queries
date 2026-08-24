-- Anvil Vault Collateral Utilization
-- Dune query 8423741: https://dune.com/queries/8423741
-- From dashboard: https://dune.com/anvil/anvil
--
-- Daily share of vault collateral reserved to back letters of credit:
--   utilization_pct = reserved_usd / tvl_usd
-- Reserved outstanding follows each reservation's lifecycle:
--   CollateralReserved            -> +amount
--   CollateralReservationModified -> +(newAmount − oldAmount)
--   CollateralReleased            -> −amount
--   CollateralClaimed             -> −amountWithFee, and when remainderReleased
--     the rest of that reservation unreserves too (the contract deletes the
--     reservation without emitting CollateralReleased)
-- TVL is the vault's physical balance (see anvil-vault-tvl-over-time.sql).
-- Sorted newest-first so a counter widget on row 1 shows current utilization.
WITH
  res_events AS (
    SELECT reservationId, evt_block_number AS bn, evt_index AS ei,
           evt_block_date AS day, CAST(amount AS int256) AS delta, false AS terminal
    FROM anvil_ethereum.collateralvault_evt_collateralreserved

    UNION ALL

    SELECT reservationId, evt_block_number, evt_index, evt_block_date,
           CAST(newAmount AS int256) - CAST(oldAmount AS int256), false
    FROM anvil_ethereum.collateralvault_evt_collateralreservationmodified

    UNION ALL

    SELECT reservationId, evt_block_number, evt_index, evt_block_date,
           -CAST(amount AS int256), false
    FROM anvil_ethereum.collateralvault_evt_collateralreleased

    UNION ALL

    SELECT reservationId, evt_block_number, evt_index, evt_block_date,
           -CAST(amountWithFee AS int256), remainderReleased
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed
  ),

  -- running reservation balance; a terminal claim releases whatever remains
  running AS (
    SELECT reservationId, day, delta, terminal,
           SUM(delta) OVER (
             PARTITION BY reservationId
             ORDER BY bn, ei
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS bal_after
    FROM res_events
  ),

  reserved_flows AS (
    SELECT
      r.day,
      res.tokenAddress AS token,
      SUM(r.delta + CASE WHEN r.terminal THEN -r.bal_after ELSE CAST(0 AS int256) END) AS amt
    FROM running r
    JOIN anvil_ethereum.collateralvault_evt_collateralreserved res
      ON res.reservationId = r.reservationId
    GROUP BY 1, 2
  ),

  -- physical vault balance flows (deposits − withdrawal/claim payouts)
  tvl_flows AS (
    SELECT evt_block_date AS day, tokenAddress AS token,
           CAST(amount AS int256) AS amt
    FROM anvil_ethereum.collateralvault_evt_fundsdeposited

    UNION ALL

    SELECT evt_block_date, tokenAddress,
           -(CAST(amountWithFee AS int256) - CAST(feeAmount AS int256))
    FROM anvil_ethereum.collateralvault_evt_fundswithdrawn

    UNION ALL

    SELECT c.evt_block_date, r.tokenAddress,
           -(CAST(c.amountWithFee AS int256) - CAST(c.feeAmount AS int256))
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed c
    JOIN anvil_ethereum.collateralvault_evt_collateralreserved r
      ON r.reservationId = c.reservationId
  ),

  daily AS (
    SELECT day, token,
           SUM(CASE WHEN src = 'tvl' THEN amt ELSE CAST(0 AS int256) END) AS tvl_amt,
           SUM(CASE WHEN src = 'res' THEN amt ELSE CAST(0 AS int256) END) AS res_amt
    FROM (
      SELECT day, token, amt, 'tvl' AS src FROM tvl_flows
      UNION ALL
      SELECT day, token, amt, 'res' FROM reserved_flows
    )
    GROUP BY 1, 2
  ),

  day_spine AS (
    SELECT CAST(d AS date) AS day
    FROM (SELECT MIN(day) AS start_day FROM daily)
    CROSS JOIN UNNEST(SEQUENCE(start_day, CURRENT_DATE, INTERVAL '1' DAY)) AS t(d)
  ),

  balances AS (
    SELECT
      s.day,
      tok.token,
      SUM(COALESCE(d.tvl_amt, CAST(0 AS int256)))
        OVER (PARTITION BY tok.token ORDER BY s.day) AS tvl_raw,
      SUM(COALESCE(d.res_amt, CAST(0 AS int256)))
        OVER (PARTITION BY tok.token ORDER BY s.day) AS res_raw
    FROM day_spine s
    CROSS JOIN (SELECT DISTINCT token FROM daily) tok
    LEFT JOIN daily d ON d.token = tok.token AND d.day = s.day
  ),

  usd AS (
    SELECT
      b.day,
      SUM(b.tvl_raw / POWER(10, COALESCE(tk.decimals, 18)) * p.price) AS tvl_usd,
      SUM(b.res_raw / POWER(10, COALESCE(tk.decimals, 18)) * p.price) AS reserved_usd
    FROM balances b
    LEFT JOIN tokens.erc20 tk
      ON tk.blockchain = 'ethereum' AND tk.contract_address = b.token
    LEFT JOIN prices.usd_daily p
      ON p.blockchain = 'ethereum' AND p.contract_address = b.token AND p.day = b.day
    GROUP BY 1
  )

SELECT
  day,
  tvl_usd,
  reserved_usd,
  tvl_usd - reserved_usd                   AS available_usd,
  100.0 * reserved_usd / NULLIF(tvl_usd, 0) AS utilization_pct
FROM usd
WHERE tvl_usd IS NOT NULL
ORDER BY day DESC
