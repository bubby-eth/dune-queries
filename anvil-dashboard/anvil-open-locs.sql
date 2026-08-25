-- Anvil Open Letters of Credit
-- Dune query 8453866: https://dune.com/queries/8453866
-- From dashboard: https://dune.com/anvil/anvil
--
-- Every currently open letter of credit, largest first: an LOC is open when
-- it has no terminal event (LOCRedeemed / LOCCanceled / LOCConverted) and its
-- latest expiration (LOCExtended-aware) is in the future. Collateral amounts
-- reflect the latest LOCCollateralModified event where one exists. USD values
-- use the latest prices; collateralization = collateral value / credit value.
WITH
  created AS (
    SELECT
      id, evt_block_time, evt_tx_hash, creator, beneficiary,
      collateralTokenAddress, collateralTokenAmount,
      creditedTokenAddress, creditedTokenAmount,
      collateralFactorBasisPoints, expirationTimestamp
    FROM anvil_ethereum.letterofcredit_evt_loccreated

    UNION ALL

    SELECT
      id, evt_block_time, evt_tx_hash, creator, beneficiary,
      collateralTokenAddress, collateralTokenAmount,
      creditedTokenAddress, creditedTokenAmount,
      collateralFactorBasisPoints, expirationTimestamp
    FROM anvil_ethereum.letterofcredit_evt_loccreatedv2
  ),

  terminal AS (
    SELECT id FROM anvil_ethereum.letterofcredit_evt_locredeemed
    UNION
    SELECT id FROM anvil_ethereum.letterofcredit_evt_loccanceled
    UNION
    SELECT id FROM anvil_ethereum.letterofcredit_evt_locconverted
  ),

  latest_collateral AS (
    SELECT id, MAX_BY(newCollateralAmount, evt_block_number) AS amount
    FROM anvil_ethereum.letterofcredit_evt_loccollateralmodified
    GROUP BY 1
  ),

  latest_expiry AS (
    SELECT id, MAX(newExpirationTimestamp) AS expiration
    FROM anvil_ethereum.letterofcredit_evt_locextended
    GROUP BY 1
  ),

  open_locs AS (
    SELECT
      c.*,
      COALESCE(lc.amount, c.collateralTokenAmount)        AS collateral_amt,
      FROM_UNIXTIME(COALESCE(le.expiration, c.expirationTimestamp)) AS expires_at
    FROM created c
    LEFT JOIN terminal t          ON t.id = c.id
    LEFT JOIN latest_collateral lc ON lc.id = c.id
    LEFT JOIN latest_expiry le     ON le.id = c.id
    WHERE t.id IS NULL
      AND FROM_UNIXTIME(COALESCE(le.expiration, c.expirationTimestamp)) > NOW()
  )

SELECT
  ROW_NUMBER() OVER (ORDER BY o.collateral_usd DESC) AS rank,
  CAST(o.id AS varchar)                              AS loc_id,
  '<a href="https://etherscan.io/tx/' || CAST(o.evt_tx_hash AS varchar) ||
    '" target="_blank">' || CAST(CAST(o.evt_block_time AS date) AS varchar) ||
    '</a>'                                           AS created,
  SUBSTR(CAST(o.creator AS varchar), 1, 6) || '...' ||
    SUBSTR(CAST(o.creator AS varchar), 39) ||
    ' | <a href="https://etherscan.io/address/' || CAST(o.creator AS varchar) ||
    '" target="_blank">Etherscan</a>'                AS creator,
  SUBSTR(CAST(o.beneficiary AS varchar), 1, 6) || '...' ||
    SUBSTR(CAST(o.beneficiary AS varchar), 39) ||
    ' | <a href="https://etherscan.io/address/' || CAST(o.beneficiary AS varchar) ||
    '" target="_blank">Etherscan</a>'                AS beneficiary,
  o.collateral_tokens                                AS collateral,
  o.collateral_symbol                                AS collateral_asset,
  o.collateral_usd,
  o.credit_tokens                                    AS credit,
  o.credit_symbol                                    AS credit_asset,
  o.credit_usd,
  100.0 * o.collateral_usd / NULLIF(o.credit_usd, 0) AS collateralization_pct,
  CAST(CAST(o.expires_at AS date) AS varchar)        AS expires,
  DATE_DIFF('day', NOW(), o.expires_at)              AS days_left
FROM (
  SELECT
    ol.*,
    CAST(ol.collateral_amt AS double)
      / POWER(10, COALESCE(ctk.decimals, 18))                    AS collateral_tokens,
    COALESCE(ctk.symbol, CAST(ol.collateralTokenAddress AS varchar)) AS collateral_symbol,
    CAST(ol.collateral_amt AS double)
      / POWER(10, COALESCE(ctk.decimals, 18)) * cp.price         AS collateral_usd,
    CAST(ol.creditedTokenAmount AS double)
      / POWER(10, COALESCE(dtk.decimals, 18))                    AS credit_tokens,
    COALESCE(dtk.symbol, CAST(ol.creditedTokenAddress AS varchar)) AS credit_symbol,
    CAST(ol.creditedTokenAmount AS double)
      / POWER(10, COALESCE(dtk.decimals, 18)) * dp.price         AS credit_usd
  FROM open_locs ol
  LEFT JOIN tokens.erc20 ctk
    ON ctk.blockchain = 'ethereum' AND ctk.contract_address = ol.collateralTokenAddress
  LEFT JOIN tokens.erc20 dtk
    ON dtk.blockchain = 'ethereum' AND dtk.contract_address = ol.creditedTokenAddress
  LEFT JOIN prices.latest cp
    ON cp.blockchain = 'ethereum' AND cp.contract_address = ol.collateralTokenAddress
  LEFT JOIN prices.latest dp
    ON dp.blockchain = 'ethereum' AND dp.contract_address = ol.creditedTokenAddress
) o
ORDER BY o.collateral_usd DESC
