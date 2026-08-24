-- Anvil Vault TVL Over Time
-- Dune query 8423739: https://dune.com/queries/8423739
-- From dashboard: https://dune.com/anvil/anvil
--
-- Daily USD value of each asset physically held in the CollateralVault
-- (0x5d2725fdE4d7Aa3388DA4519ac0449Cc031d675f), for a stacked area chart.
-- Physical balance per token = deposits − withdrawal payouts − claim payouts.
-- Withdrawal and claim fees stay in the vault as protocol balance until swept
-- by governance, so each outflow event removes only amountWithFee − feeAmount.
-- Internal account-to-account transfers (CollateralTransferred) never move
-- tokens and are excluded.
WITH
  flows AS (
    SELECT evt_block_date AS day, tokenAddress AS token,
           CAST(amount AS int256) AS amt
    FROM anvil_ethereum.collateralvault_evt_fundsdeposited

    UNION ALL

    SELECT evt_block_date, tokenAddress,
           -(CAST(amountWithFee AS int256) - CAST(feeAmount AS int256))
    FROM anvil_ethereum.collateralvault_evt_fundswithdrawn

    UNION ALL

    -- claimed collateral leaves the vault; the claim event only carries the
    -- reservation id, so the token comes from its CollateralReserved event
    SELECT c.evt_block_date, r.tokenAddress,
           -(CAST(c.amountWithFee AS int256) - CAST(c.feeAmount AS int256))
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed c
    JOIN anvil_ethereum.collateralvault_evt_collateralreserved r
      ON r.reservationId = c.reservationId
  ),

  daily_flows AS (
    SELECT day, token, SUM(amt) AS amt
    FROM flows
    GROUP BY 1, 2
  ),

  -- one row per (day, token) from first activity through today
  day_spine AS (
    SELECT CAST(d AS date) AS day
    FROM (SELECT MIN(day) AS start_day FROM daily_flows)
    CROSS JOIN UNNEST(SEQUENCE(start_day, CURRENT_DATE, INTERVAL '1' DAY)) AS t(d)
  ),

  balances AS (
    SELECT
      s.day,
      tok.token,
      SUM(COALESCE(df.amt, CAST(0 AS int256)))
        OVER (PARTITION BY tok.token ORDER BY s.day) AS raw_balance
    FROM day_spine s
    CROSS JOIN (SELECT DISTINCT token FROM daily_flows) tok
    LEFT JOIN daily_flows df ON df.token = tok.token AND df.day = s.day
  )

SELECT
  b.day,
  COALESCE(tk.symbol, CAST(b.token AS varchar))              AS asset,
  b.raw_balance / POWER(10, COALESCE(tk.decimals, 18))       AS balance,
  b.raw_balance / POWER(10, COALESCE(tk.decimals, 18))
    * p.price                                                AS tvl_usd
FROM balances b
LEFT JOIN tokens.erc20 tk
  ON tk.blockchain = 'ethereum' AND tk.contract_address = b.token
LEFT JOIN prices.usd_daily p
  ON p.blockchain = 'ethereum' AND p.contract_address = b.token AND p.day = b.day
WHERE b.raw_balance > CAST(0 AS int256)
ORDER BY b.day, asset
