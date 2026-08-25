-- Anvil Letters of Credit Activity
-- Dune query 8423834: https://dune.com/queries/8423834
-- From dashboard: https://dune.com/anvil/anvil
--
-- Weekly letter-of-credit lifecycle from the decoded LetterOfCredit tables
-- (anvil_ethereum.letterofcredit_evt_*, proxy 0x14db9a91933aD9433E1A0dB04D08e5D9EF7c4808):
--   LOCCreated / LOCCreatedV2 -> new credit issued (face value = creditedTokenAmount)
--   LOCRedeemed               -> beneficiary drew the credit
--   LOCCanceled               -> closed unused (face value from its creation event)
--   LOCConverted / LOCPartiallyLiquidated -> collateral liquidated into the
--     credited token (value = creditedTokenAmountReceived)
-- USD values price each amount in its credited token (mostly stablecoins) on
-- the event day. v1 and v2 ids come from one shared counter, so the creation
-- map covers both.
WITH
  created AS (
    SELECT
      evt_block_time       AS block_time,
      id                   AS loc_id,
      creditedTokenAddress AS credited_token,
      creditedTokenAmount  AS credited_amt
    FROM anvil_ethereum.letterofcredit_evt_loccreated

    UNION ALL

    SELECT evt_block_time, id, creditedTokenAddress, creditedTokenAmount
    FROM anvil_ethereum.letterofcredit_evt_loccreatedv2
  ),

  redeemed AS (
    SELECT
      evt_block_time      AS block_time,
      id                  AS loc_id,
      creditedTokenAmount AS credited_amt
    FROM anvil_ethereum.letterofcredit_evt_locredeemed
  ),

  canceled AS (
    SELECT evt_block_time AS block_time, id AS loc_id
    FROM anvil_ethereum.letterofcredit_evt_loccanceled
  ),

  liquidated AS (
    SELECT
      evt_block_time              AS block_time,
      id                          AS loc_id,
      creditedTokenAmountReceived AS credited_amt
    FROM anvil_ethereum.letterofcredit_evt_locconverted

    UNION ALL

    SELECT evt_block_time, id, creditedTokenAmountReceived
    FROM anvil_ethereum.letterofcredit_evt_locpartiallyliquidated
  ),

  -- every lifecycle event with its credited token and amount (redemptions,
  -- cancels, and liquidations resolve the token via the creation event)
  events AS (
    SELECT block_time, credited_token, credited_amt, 'created' AS kind
    FROM created

    UNION ALL

    SELECT r.block_time, c.credited_token, r.credited_amt, 'redeemed'
    FROM redeemed r
    JOIN created c ON c.loc_id = r.loc_id

    UNION ALL

    SELECT x.block_time, c.credited_token, c.credited_amt, 'canceled'
    FROM canceled x
    JOIN created c ON c.loc_id = x.loc_id

    UNION ALL

    SELECT l.block_time, c.credited_token, l.credited_amt, 'liquidated'
    FROM liquidated l
    JOIN created c ON c.loc_id = l.loc_id
  ),

  priced AS (
    SELECT
      DATE_TRUNC('week', e.block_time) AS week,
      e.kind,
      CAST(e.credited_amt AS double) / POWER(10, COALESCE(tk.decimals, 18)) * p.price AS usd
    FROM events e
    LEFT JOIN tokens.erc20 tk
      ON tk.blockchain = 'ethereum' AND tk.contract_address = e.credited_token
    LEFT JOIN prices.usd_daily p
      ON p.blockchain = 'ethereum' AND p.contract_address = e.credited_token
     AND p.day = CAST(e.block_time AS date)
  )

SELECT
  week,
  COUNT_IF(kind = 'created')                  AS created_count,
  SUM(CASE WHEN kind = 'created'    THEN usd END) AS created_usd,
  COUNT_IF(kind = 'redeemed')                 AS redeemed_count,
  -SUM(CASE WHEN kind = 'redeemed'  THEN usd END) AS redeemed_usd,
  COUNT_IF(kind = 'canceled')                 AS canceled_count,
  -SUM(CASE WHEN kind = 'canceled'  THEN usd END) AS canceled_usd,
  COUNT_IF(kind = 'liquidated')               AS liquidated_count,
  -SUM(CASE WHEN kind = 'liquidated' THEN usd END) AS liquidated_usd
FROM priced
GROUP BY 1
ORDER BY 1
