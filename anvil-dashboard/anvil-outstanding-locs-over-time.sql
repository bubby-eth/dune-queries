-- Anvil Outstanding Letters of Credit Over Time
-- Dune query 8453867: https://dune.com/queries/8453867
-- From dashboard: https://dune.com/anvil/anvil
--
-- Daily count and USD face value of open letters of credit. Each LOC is open
-- from creation until its first terminal event (LOCRedeemed / LOCCanceled /
-- LOCConverted) or its latest expiration (LOCExtended-aware), whichever comes
-- first. Face value is the credited amount fixed at creation, priced daily in
-- its credited token (mostly stablecoins); the rare mid-life partial
-- liquidation is not netted out. One query, two widgets: area for
-- outstanding_usd, line for open_count.
WITH
  created AS (
    SELECT id, evt_block_date AS start_day,
           creditedTokenAddress AS token, creditedTokenAmount AS amt,
           expirationTimestamp
    FROM anvil_ethereum.letterofcredit_evt_loccreated

    UNION ALL

    SELECT id, evt_block_date, creditedTokenAddress, creditedTokenAmount,
           expirationTimestamp
    FROM anvil_ethereum.letterofcredit_evt_loccreatedv2
  ),

  terminal AS (
    SELECT id, MIN(day) AS end_day
    FROM (
      SELECT id, evt_block_date AS day FROM anvil_ethereum.letterofcredit_evt_locredeemed
      UNION ALL
      SELECT id, evt_block_date FROM anvil_ethereum.letterofcredit_evt_loccanceled
      UNION ALL
      SELECT id, evt_block_date FROM anvil_ethereum.letterofcredit_evt_locconverted
    )
    GROUP BY 1
  ),

  latest_expiry AS (
    SELECT id, MAX(newExpirationTimestamp) AS expiration
    FROM anvil_ethereum.letterofcredit_evt_locextended
    GROUP BY 1
  ),

  -- open interval per LOC: [start_day, end_day); NULL end_day = still open
  locs AS (
    SELECT
      c.id, c.start_day, c.token, c.amt,
      LEAST(
        COALESCE(t.end_day, DATE '9999-12-31'),
        CAST(FROM_UNIXTIME(COALESCE(le.expiration, c.expirationTimestamp)) AS date)
      ) AS end_day
    FROM created c
    LEFT JOIN terminal t      ON t.id = c.id
    LEFT JOIN latest_expiry le ON le.id = c.id
  ),

  day_spine AS (
    SELECT CAST(d AS date) AS day
    FROM (SELECT MIN(start_day) AS start_day FROM locs)
    CROSS JOIN UNNEST(SEQUENCE(start_day, CURRENT_DATE, INTERVAL '1' DAY)) AS t(d)
  ),

  open_by_day AS (
    SELECT s.day, l.token, l.amt, l.id
    FROM day_spine s
    JOIN locs l ON l.start_day <= s.day AND s.day < l.end_day
  )

SELECT
  o.day,
  COUNT(DISTINCT o.id) AS open_count,
  SUM(CAST(o.amt AS double) / POWER(10, COALESCE(tk.decimals, 18)) * p.price) AS outstanding_usd
FROM open_by_day o
LEFT JOIN tokens.erc20 tk
  ON tk.blockchain = 'ethereum' AND tk.contract_address = o.token
LEFT JOIN prices.usd_daily p
  ON p.blockchain = 'ethereum' AND p.contract_address = o.token AND p.day = o.day
GROUP BY 1
-- drop days (typically only today) with no daily price yet, like the other
-- over-time queries on this dashboard
HAVING SUM(CAST(o.amt AS double) / POWER(10, COALESCE(tk.decimals, 18)) * p.price) IS NOT NULL
ORDER BY 1
