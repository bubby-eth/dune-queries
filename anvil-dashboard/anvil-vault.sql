-- Anvil Vault
-- Dune query 4668282: https://dune.com/queries/4668282
-- From dashboard: https://dune.com/anvil/anvil
--
-- Current physical balance of each asset held in the CollateralVault
-- (0x5d2725fdE4d7Aa3388DA4519ac0449Cc031d675f), priced at the latest USD
-- price. Balance per token = deposits − withdrawal payouts − claim payouts;
-- withdrawal/claim fees stay in the vault until swept by governance, so each
-- outflow removes only amountWithFee − feeAmount (same accounting as
-- anvil-vault-tvl-over-time.sql). Built on the decoded CollateralVault tables
-- instead of a full erc20 Transfer scan, so any newly added collateral token
-- shows up without a hardcoded list.
WITH
  flows AS (
    SELECT tokenAddress AS token, CAST(amount AS int256) AS amt
    FROM anvil_ethereum.collateralvault_evt_fundsdeposited

    UNION ALL

    SELECT tokenAddress,
           -(CAST(amountWithFee AS int256) - CAST(feeAmount AS int256))
    FROM anvil_ethereum.collateralvault_evt_fundswithdrawn

    UNION ALL

    -- claimed collateral leaves the vault; the claim event only carries the
    -- reservation id, so the token comes from its CollateralReserved event
    SELECT r.tokenAddress,
           -(CAST(c.amountWithFee AS int256) - CAST(c.feeAmount AS int256))
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed c
    JOIN anvil_ethereum.collateralvault_evt_collateralreserved r
      ON r.reservationId = c.reservationId
  ),

  balances AS (
    SELECT token, SUM(amt) AS raw_balance
    FROM flows
    GROUP BY 1
    HAVING SUM(amt) > CAST(0 AS int256)
  )

SELECT
  COALESCE(tk.symbol, CAST(b.token AS varchar))              AS asset,
  b.raw_balance / POWER(10, COALESCE(tk.decimals, 18))       AS quantity,
  b.raw_balance / POWER(10, COALESCE(tk.decimals, 18))
    * COALESCE(p.price, 0)                                   AS amount
FROM balances b
LEFT JOIN tokens.erc20 tk
  ON tk.blockchain = 'ethereum' AND tk.contract_address = b.token
LEFT JOIN prices.latest p
  ON p.blockchain = 'ethereum' AND p.contract_address = b.token
ORDER BY amount DESC
