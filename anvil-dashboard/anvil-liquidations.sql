-- Anvil LOC Liquidations
-- Dune query 8423835: https://dune.com/queries/8423835
-- From dashboard: https://dune.com/anvil/anvil
--
-- Every letter-of-credit liquidation, largest first: LOCConverted (full — the
-- LOC's collateral is swapped into its credited token) and
-- LOCPartiallyLiquidated (partial), from the decoded LetterOfCredit tables
-- (anvil_ethereum.letterofcredit_evt_*). Collateral amount/fee are in the
-- LOC's collateral token; received value is in its credited token, priced in
-- USD on the event day. Known liquidator contracts are labeled; other parties
-- get the truncated-address treatment.
WITH
  created AS (
    SELECT
      id                     AS loc_id,
      collateralTokenAddress AS collateral_token,
      creditedTokenAddress   AS credited_token
    FROM anvil_ethereum.letterofcredit_evt_loccreated

    UNION ALL

    SELECT id, collateralTokenAddress, creditedTokenAddress
    FROM anvil_ethereum.letterofcredit_evt_loccreatedv2
  ),

  liquidations AS (
    SELECT
      evt_block_time              AS block_time,
      evt_tx_hash                 AS tx_hash,
      id                          AS loc_id,
      initiator,
      liquidator,
      liquidationAmount           AS liquidation_amt,
      liquidationFeeAmount        AS fee_amt,
      creditedTokenAmountReceived AS received_amt,
      'full'                      AS kind
    FROM anvil_ethereum.letterofcredit_evt_locconverted

    UNION ALL

    SELECT
      evt_block_time,
      evt_tx_hash,
      id,
      initiator,
      liquidator,
      liquidationAmount,
      liquidationFeeAmount,
      creditedTokenAmountReceived,
      'partial'
    FROM anvil_ethereum.letterofcredit_evt_locpartiallyliquidated
  ),

  liquidator_labels (addr, label) AS (
    VALUES
      (0x9ae1CAA5cE6fA330fcE98315159BCD433B1342b8, 'PassThroughLiquidator'),
      (0x8Aa57e442e4562c80FDDAD1b71ADF0BA75E2eb4C, 'Permit2PassThroughLiquidator'),
      (0x716321565e1EAbA200789E14ad92c9dA40B14589, 'UniswapLiquidator')
  )

SELECT
  ROW_NUMBER() OVER (ORDER BY lq.received_usd DESC) AS rank,
  '<a href="https://etherscan.io/tx/' || CAST(lq.tx_hash AS varchar) ||
    '" target="_blank">' || CAST(CAST(lq.block_time AS date) AS varchar) ||
    '</a>'                                          AS "date",
  CAST(lq.loc_id AS varchar)                        AS loc_id,
  lq.kind,
  lq.collateral_symbol                              AS collateral,
  lq.liquidation_amount                             AS collateral_liquidated,
  lq.fee_amount                                     AS liquidator_fee,
  lq.received_usd,
  COALESCE(
    ll.label,
    SUBSTR(CAST(lq.liquidator AS varchar), 1, 6) || '...' ||
      SUBSTR(CAST(lq.liquidator AS varchar), 39)
  ) || ' | <a href="https://etherscan.io/address/' || CAST(lq.liquidator AS varchar) ||
    '" target="_blank">Etherscan</a>'               AS liquidator,
  SUBSTR(CAST(lq.initiator AS varchar), 1, 6) || '...' ||
    SUBSTR(CAST(lq.initiator AS varchar), 39) ||
    ' | <a href="https://etherscan.io/address/' || CAST(lq.initiator AS varchar) ||
    '" target="_blank">Etherscan</a>'               AS initiator
FROM (
  SELECT
    l.*,
    COALESCE(ctk.symbol, CAST(c.collateral_token AS varchar))       AS collateral_symbol,
    CAST(l.liquidation_amt AS double)
      / POWER(10, COALESCE(ctk.decimals, 18))                       AS liquidation_amount,
    CAST(l.fee_amt AS double)
      / POWER(10, COALESCE(ctk.decimals, 18))                       AS fee_amount,
    CAST(l.received_amt AS double)
      / POWER(10, COALESCE(dtk.decimals, 18)) * p.price             AS received_usd
  FROM liquidations l
  JOIN created c ON c.loc_id = l.loc_id
  LEFT JOIN tokens.erc20 ctk
    ON ctk.blockchain = 'ethereum' AND ctk.contract_address = c.collateral_token
  LEFT JOIN tokens.erc20 dtk
    ON dtk.blockchain = 'ethereum' AND dtk.contract_address = c.credited_token
  LEFT JOIN prices.usd_daily p
    ON p.blockchain = 'ethereum' AND p.contract_address = c.credited_token
   AND p.day = CAST(l.block_time AS date)
) lq
LEFT JOIN liquidator_labels ll ON ll.addr = lq.liquidator
ORDER BY lq.received_usd DESC
LIMIT 25
