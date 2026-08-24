-- Anvil Vault Top Depositors
-- Dune query 8423742: https://dune.com/queries/8423742
-- From dashboard: https://dune.com/anvil/anvil
--
-- Top 25 CollateralVault accounts by current USD value of collateral held:
--   balance = deposits − withdrawals (amountWithFee; fees debit the account)
--             + internal transfers in − out − collateral claimed away
-- Protocol accounts are labeled from CollateralizableContractApprovalUpdated
-- (collateral pools vs. other approved contracts, e.g. LetterOfCredit).
-- first_deposit / last_activity dates link to their tx on Etherscan; the
-- last_activity suffix shows the kind: (deposit)/(withdraw)/(in)/(out)/(claim).
WITH
  flows AS (
    SELECT toAccount AS account, tokenAddress AS token,
           CAST(amount AS int256) AS amt,
           evt_block_time AS block_time, evt_tx_hash AS tx_hash, 'deposit' AS kind
    FROM anvil_ethereum.collateralvault_evt_fundsdeposited

    UNION ALL

    SELECT fromAccount, tokenAddress, -CAST(amountWithFee AS int256),
           evt_block_time, evt_tx_hash, 'withdraw'
    FROM anvil_ethereum.collateralvault_evt_fundswithdrawn

    UNION ALL

    SELECT toAccount, tokenAddress, CAST(tokenAmount AS int256),
           evt_block_time, evt_tx_hash, 'in'
    FROM anvil_ethereum.collateralvault_evt_collateraltransferred

    UNION ALL

    SELECT fromAccount, tokenAddress, -CAST(tokenAmount AS int256),
           evt_block_time, evt_tx_hash, 'out'
    FROM anvil_ethereum.collateralvault_evt_collateraltransferred

    UNION ALL

    -- claim debits the reservation owner's collateral
    SELECT r.account, r.tokenAddress, -CAST(c.amountWithFee AS int256),
           c.evt_block_time, c.evt_tx_hash, 'claim'
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed c
    JOIN anvil_ethereum.collateralvault_evt_collateralreserved r
      ON r.reservationId = c.reservationId
  ),

  -- latest available daily price per token
  latest_price AS (
    SELECT contract_address, MAX_BY(price, day) AS price
    FROM prices.usd_daily
    WHERE blockchain = 'ethereum'
      AND contract_address IN (SELECT DISTINCT token FROM flows)
    GROUP BY 1
  ),

  account_tokens AS (
    SELECT account, token, SUM(amt) AS raw_balance
    FROM flows
    GROUP BY 1, 2
    HAVING SUM(amt) > CAST(0 AS int256)
  ),

  account_values AS (
    SELECT
      at.account,
      SUM(at.raw_balance / POWER(10, COALESCE(tk.decimals, 18)) * lp.price) AS value_usd,
      MAX_BY(COALESCE(tk.symbol, CAST(at.token AS varchar)),
             at.raw_balance / POWER(10, COALESCE(tk.decimals, 18)) * lp.price) AS top_asset,
      COUNT(*) AS assets
    FROM account_tokens at
    LEFT JOIN tokens.erc20 tk
      ON tk.blockchain = 'ethereum' AND tk.contract_address = at.token
    LEFT JOIN latest_price lp ON lp.contract_address = at.token
    GROUP BY 1
  ),

  activity AS (
    SELECT
      account,
      -- falls back to first activity of any kind: pool accounts are funded via
      -- internal transfers and never emit a FundsDeposited of their own
      COALESCE(MIN(CASE WHEN kind = 'deposit' THEN block_time END),
               MIN(block_time))                                   AS first_deposit_at,
      COALESCE(MIN_BY(tx_hash, CASE WHEN kind = 'deposit' THEN block_time END),
               MIN_BY(tx_hash, block_time))                       AS first_deposit_tx,
      MAX(block_time)                                             AS last_activity_at,
      MAX_BY(tx_hash, block_time)                                 AS last_activity_tx,
      MAX_BY(kind, block_time)                                    AS last_activity_kind
    FROM flows
    GROUP BY 1
  ),

  -- protocol contracts approved to reserve collateral (pools vs. LoC etc.)
  protocol_labels AS (
    SELECT contractAddress AS account,
           MAX_BY(CASE WHEN isCollateralPool THEN '🏛️ Collateral Pool'
                       ELSE '🏛️ Protocol Contract' END, evt_block_number) AS label
    FROM anvil_ethereum.collateralvault_evt_collateralizablecontractapprovalupdated
    GROUP BY 1
  )

SELECT
  ROW_NUMBER() OVER (ORDER BY av.value_usd DESC) AS rank,
  COALESCE(pl.label || ' ', '') ||
    SUBSTR(CAST(av.account AS varchar), 1, 6) || '...' ||
    SUBSTR(CAST(av.account AS varchar), 39) ||
    ' | <a href="https://etherscan.io/address/' || CAST(av.account AS varchar) ||
    '" target="_blank">Etherscan</a>' ||
    ' | <a href="https://debank.com/profile/' || CAST(av.account AS varchar) ||
    '" target="_blank">DeBank</a>' AS depositor,
  av.value_usd,
  av.top_asset,
  av.assets,
  '<a href="https://etherscan.io/tx/' || CAST(ac.first_deposit_tx AS varchar) ||
    '" target="_blank">' || CAST(CAST(ac.first_deposit_at AS date) AS varchar) ||
    '</a>' AS first_deposit,
  '<a href="https://etherscan.io/tx/' || CAST(ac.last_activity_tx AS varchar) ||
    '" target="_blank">' || CAST(CAST(ac.last_activity_at AS date) AS varchar) ||
    ' (' || ac.last_activity_kind || ')</a>' AS last_activity
FROM account_values av
JOIN activity ac ON ac.account = av.account
LEFT JOIN protocol_labels pl ON pl.account = av.account
ORDER BY av.value_usd DESC
LIMIT 25
