-- Anvil Protocol Fee Revenue
-- Dune query 8453869: https://dune.com/queries/8453869
-- From dashboard: https://dune.com/anvil/anvil
--
-- Weekly protocol fee revenue in USD with the cumulative total. The
-- CollateralVault takes a fee on withdrawals (FundsWithdrawn.feeAmount) and
-- on collateral claims (CollateralClaimed.feeAmount); fees accumulate in the
-- vault as protocol balance until swept by governance. Claim fees are
-- denominated in the reservation's token, resolved via CollateralReserved.
-- Each fee is priced in USD on its event day. Bar chart: fees_usd columns
-- with cumulative_fees_usd as a line.
WITH
  fees AS (
    SELECT evt_block_date AS day, tokenAddress AS token,
           CAST(feeAmount AS int256) AS amt
    FROM anvil_ethereum.collateralvault_evt_fundswithdrawn
    WHERE feeAmount > UINT256 '0'

    UNION ALL

    SELECT c.evt_block_date, r.tokenAddress, CAST(c.feeAmount AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed c
    JOIN anvil_ethereum.collateralvault_evt_collateralreserved r
      ON r.reservationId = c.reservationId
    WHERE c.feeAmount > UINT256 '0'
  ),

  weekly AS (
    SELECT
      CAST(DATE_TRUNC('week', f.day) AS date) AS week,
      SUM(CAST(f.amt AS double) / POWER(10, COALESCE(tk.decimals, 18)) * p.price) AS fees_usd
    FROM fees f
    LEFT JOIN tokens.erc20 tk
      ON tk.blockchain = 'ethereum' AND tk.contract_address = f.token
    LEFT JOIN prices.usd_daily p
      ON p.blockchain = 'ethereum' AND p.contract_address = f.token AND p.day = f.day
    GROUP BY 1
  )

SELECT
  week,
  fees_usd,
  SUM(fees_usd) OVER (ORDER BY week) AS cumulative_fees_usd
FROM weekly
ORDER BY week
