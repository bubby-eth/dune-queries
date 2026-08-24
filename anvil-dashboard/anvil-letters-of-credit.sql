-- Anvil Letters of Credit Activity
-- Dune query 8423834: https://dune.com/queries/8423834
-- From dashboard: https://dune.com/anvil/anvil
--
-- Weekly letter-of-credit lifecycle on the LetterOfCredit proxy
-- (0x14db9a91933aD9433E1A0dB04D08e5D9EF7c4808, not decoded on Dune):
--   LOCCreated / LOCCreatedV2 -> new credit issued (face value = creditedTokenAmount)
--   LOCRedeemed               -> beneficiary drew the credit
--   LOCCanceled               -> closed unused (face value from its creation event)
--   LOCConverted / LOCPartiallyLiquidated -> collateral liquidated into the
--     credited token (value = creditedTokenAmountReceived)
-- USD values price each amount in its credited token (mostly stablecoins) on
-- the event day. v1 and v2 creation events share data offsets for the fields
-- read here; ids come from one shared counter so the creation map covers both.
WITH
  created AS (
    SELECT
      block_time,
      tx_hash,
      varbinary_to_int256(varbinary_substring(data, 289, 32))      AS loc_id,
      varbinary_substring(data, 237, 20)                           AS credited_token,
      varbinary_to_int256(varbinary_substring(data, 257, 32))      AS credited_amt
    FROM ethereum.logs
    WHERE contract_address = 0x14db9a91933aD9433E1A0dB04D08e5D9EF7c4808
      AND topic0 IN (
        0x53105ebe61784910061b94c58c9a88c322e1c41ea202c65f724153c5d933eda8, -- LOCCreatedV2
        0x7a259165a2d5de0bc04a49e556d863bb8badf4d0b564e53efa7d66bc17d539f0  -- LOCCreated
      )
      AND block_time > TRY_CAST('2024-09-01' AS TIMESTAMP)
  ),

  redeemed AS (
    SELECT
      block_time,
      varbinary_to_int256(topic1)                                  AS loc_id,
      varbinary_to_int256(varbinary_substring(data, 1, 32))        AS credited_amt
    FROM ethereum.logs
    WHERE contract_address = 0x14db9a91933aD9433E1A0dB04D08e5D9EF7c4808
      AND topic0 = 0xa401b77f6ebc213dba24e20584bca0389bbd6391cf1498bbbf4feac74932f68a
      AND block_time > TRY_CAST('2024-09-01' AS TIMESTAMP)
  ),

  canceled AS (
    SELECT block_time, varbinary_to_int256(topic1) AS loc_id
    FROM ethereum.logs
    WHERE contract_address = 0x14db9a91933aD9433E1A0dB04D08e5D9EF7c4808
      AND topic0 = 0x1706888457c949ef810b6308f2cb8280ecb6d22a992c4b53cf15f25405020c10
      AND block_time > TRY_CAST('2024-09-01' AS TIMESTAMP)
  ),

  liquidated AS (
    SELECT
      block_time,
      varbinary_to_int256(topic1)                                  AS loc_id,
      varbinary_to_int256(varbinary_substring(data, 65, 32))       AS credited_amt
    FROM ethereum.logs
    WHERE contract_address = 0x14db9a91933aD9433E1A0dB04D08e5D9EF7c4808
      AND topic0 IN (
        0x21640bc6d24e28d13a51d024a2c86f8dd94df3889ab646059044fad4cf4eded4, -- LOCConverted
        0x103624043a1a382761fb8e47bd03d967b68fce58c92eabd1c990478babf49d8b  -- LOCPartiallyLiquidated
      )
      AND block_time > TRY_CAST('2024-09-01' AS TIMESTAMP)
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
      e.credited_amt / POWER(10, COALESCE(tk.decimals, 18)) * p.price AS usd
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
