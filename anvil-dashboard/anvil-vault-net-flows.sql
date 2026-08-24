-- Anvil Vault Net Flows
-- Dune query 8423740: https://dune.com/queries/8423740
-- From dashboard: https://dune.com/anvil/anvil
--
-- Weekly USD flows in and out of the CollateralVault: deposits (+), withdrawal
-- payouts (−), and claim payouts (−, collateral seized to satisfy letters of
-- credit), plus the net. Outflows use amountWithFee − feeAmount since fees stay
-- in the vault as protocol balance. Bar chart: deposited/withdrawn/claimed
-- columns with net_usd as a line.
WITH
  flows AS (
    SELECT evt_block_date AS day, tokenAddress AS token,
           CAST(amount AS int256) AS amt, 'deposit' AS kind
    FROM anvil_ethereum.collateralvault_evt_fundsdeposited

    UNION ALL

    SELECT evt_block_date, tokenAddress,
           CAST(amountWithFee AS int256) - CAST(feeAmount AS int256), 'withdraw'
    FROM anvil_ethereum.collateralvault_evt_fundswithdrawn

    UNION ALL

    SELECT c.evt_block_date, r.tokenAddress,
           CAST(c.amountWithFee AS int256) - CAST(c.feeAmount AS int256), 'claim'
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed c
    JOIN anvil_ethereum.collateralvault_evt_collateralreserved r
      ON r.reservationId = c.reservationId
  ),

  priced AS (
    SELECT
      DATE_TRUNC('week', f.day) AS week,
      f.kind,
      f.amt / POWER(10, COALESCE(tk.decimals, 18)) * p.price AS usd
    FROM flows f
    LEFT JOIN tokens.erc20 tk
      ON tk.blockchain = 'ethereum' AND tk.contract_address = f.token
    LEFT JOIN prices.usd_daily p
      ON p.blockchain = 'ethereum' AND p.contract_address = f.token AND p.day = f.day
  )

SELECT
  week,
  SUM(CASE WHEN kind = 'deposit'  THEN usd END)  AS deposited_usd,
  -SUM(CASE WHEN kind = 'withdraw' THEN usd END) AS withdrawn_usd,
  -SUM(CASE WHEN kind = 'claim'    THEN usd END) AS claimed_usd,
  SUM(CASE WHEN kind = 'deposit' THEN usd ELSE -usd END) AS net_usd
FROM priced
GROUP BY 1
ORDER BY 1
