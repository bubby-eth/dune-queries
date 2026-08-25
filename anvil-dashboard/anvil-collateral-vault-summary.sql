-- Anvil Collateral Vault Summary
-- Dune query 4668638: https://dune.com/queries/4668638
-- From dashboard: https://dune.com/anvil/anvil
--
-- Total USD value locked in the CollateralVault
-- (0x5d2725fdE4d7Aa3388DA4519ac0449Cc031d675f) right now: the sum over all
-- tokens of the vault's physical balance (deposits − withdrawal payouts −
-- claim payouts; fees stay in the vault until swept) at the latest USD price.
-- Built on the decoded CollateralVault tables instead of a full erc20
-- Transfer scan (same accounting as anvil-vault.sql).
WITH
  flows AS (
    SELECT tokenAddress AS token, CAST(amount AS int256) AS amt
    FROM anvil_ethereum.collateralvault_evt_fundsdeposited

    UNION ALL

    SELECT tokenAddress,
           -(CAST(amountWithFee AS int256) - CAST(feeAmount AS int256))
    FROM anvil_ethereum.collateralvault_evt_fundswithdrawn

    UNION ALL

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
  SUM(b.raw_balance / POWER(10, COALESCE(tk.decimals, 18))
      * COALESCE(p.price, 0)) AS total_value_locked
FROM balances b
LEFT JOIN tokens.erc20 tk
  ON tk.blockchain = 'ethereum' AND tk.contract_address = b.token
LEFT JOIN prices.latest p
  ON p.blockchain = 'ethereum' AND p.contract_address = b.token
