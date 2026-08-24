-- Anvil LOC Liquidations
-- Dune query 8423835: https://dune.com/queries/8423835
-- From dashboard: https://dune.com/anvil/anvil
--
-- Every letter-of-credit liquidation, largest first: LOCConverted (full — the
-- LOC's collateral is swapped into its credited token) and
-- LOCPartiallyLiquidated (partial). Raw logs on the LetterOfCredit proxy
-- (not decoded on Dune). Collateral amount/fee are in the LOC's collateral
-- token; received value is in its credited token, priced in USD on the event
-- day. Known liquidator contracts are labeled; other parties get the
-- truncated-address treatment.
WITH
  created AS (
    SELECT
      varbinary_to_int256(varbinary_substring(data, 289, 32)) AS loc_id,
      varbinary_substring(data, 45, 20)                       AS collateral_token,
      varbinary_substring(data, 237, 20)                      AS credited_token
    FROM ethereum.logs
    WHERE contract_address = 0x14db9a91933aD9433E1A0dB04D08e5D9EF7c4808
      AND topic0 IN (
        0x53105ebe61784910061b94c58c9a88c322e1c41ea202c65f724153c5d933eda8, -- LOCCreatedV2
        0x7a259165a2d5de0bc04a49e556d863bb8badf4d0b564e53efa7d66bc17d539f0  -- LOCCreated
      )
      AND block_time > TRY_CAST('2024-09-01' AS TIMESTAMP)
  ),

  liquidations AS (
    SELECT
      block_time,
      tx_hash,
      varbinary_to_int256(topic1)                            AS loc_id,
      varbinary_substring(topic2, 13, 20)                    AS initiator,
      varbinary_substring(topic3, 13, 20)                    AS liquidator,
      varbinary_to_int256(varbinary_substring(data, 1, 32))  AS liquidation_amt,
      varbinary_to_int256(varbinary_substring(data, 33, 32)) AS fee_amt,
      varbinary_to_int256(varbinary_substring(data, 65, 32)) AS received_amt,
      CASE WHEN topic0 = 0x21640bc6d24e28d13a51d024a2c86f8dd94df3889ab646059044fad4cf4eded4
           THEN 'full' ELSE 'partial' END                    AS kind
    FROM ethereum.logs
    WHERE contract_address = 0x14db9a91933aD9433E1A0dB04D08e5D9EF7c4808
      AND topic0 IN (
        0x21640bc6d24e28d13a51d024a2c86f8dd94df3889ab646059044fad4cf4eded4, -- LOCConverted
        0x103624043a1a382761fb8e47bd03d967b68fce58c92eabd1c990478babf49d8b  -- LOCPartiallyLiquidated
      )
      AND block_time > TRY_CAST('2024-09-01' AS TIMESTAMP)
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
    COALESCE(ctk.symbol, CAST(c.collateral_token AS varchar)) AS collateral_symbol,
    l.liquidation_amt / POWER(10, COALESCE(ctk.decimals, 18)) AS liquidation_amount,
    l.fee_amt / POWER(10, COALESCE(ctk.decimals, 18))         AS fee_amount,
    l.received_amt / POWER(10, COALESCE(dtk.decimals, 18))
      * p.price                                               AS received_usd
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
