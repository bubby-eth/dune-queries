WITH
  -- Configure contract periods
  addresses AS (
    SELECT
      0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc AS token_address,
      DATE '2020-08-01' AS start_date,
      DATE '2025-10-01' AS end_date
    UNION ALL
    SELECT
      0xAEEAa594e7dc112D67b8547fe9767a02c15B5597 AS token_address,
      DATE '2025-10-02' AS start_date,
      CAST(DATE_TRUNC('day', CURRENT_TIMESTAMP) AS DATE) AS end_date
  ),
  -- Minimum balance threshold in whole tokens
  thresh AS (
    SELECT 0 AS threshold
  ),
  -- Daily net transfers per holder, limited to each contract's active window
  transfers AS (
    SELECT
      DATE_TRUNC('day', tr.evt_block_time) AS day,
      tr."to" AS address,
      tr.contract_address AS token_address,
      CAST(tr.value AS INT256) AS amount
    FROM erc20_ethereum.evt_Transfer tr
    JOIN addresses a
      ON tr.contract_address = a.token_address
     AND DATE(tr.evt_block_time) BETWEEN a.start_date AND a.end_date

    UNION ALL

    SELECT
      DATE_TRUNC('day', tr.evt_block_time) AS day,
      tr."from" AS address,
      tr.contract_address AS token_address,
      -CAST(tr.value AS INT256) AS amount
    FROM erc20_ethereum.evt_Transfer tr
    JOIN addresses a
      ON tr.contract_address = a.token_address
     AND DATE(tr.evt_block_time) BETWEEN a.start_date AND a.end_date
  ),
  -- Balance on days with transfers plus the next change day
  balances_with_gap_days AS (
    SELECT
      t.day,
      t.address,
      SUM(t.amount) OVER (
        PARTITION BY t.address
        ORDER BY t.day
      ) AS balance,
      LEAD(t.day, 1, DATE_TRUNC('day', CURRENT_TIMESTAMP)) OVER (
        PARTITION BY t.address
        ORDER BY t.day
      ) AS next_day
    FROM transfers t
  ),
  -- Calendar days to fill gaps
  days AS (
    SELECT day
    FROM UNNEST(
      SEQUENCE(
        CAST(DATE_TRUNC('day', TIMESTAMP '2020-08-01 00:00:00') AS TIMESTAMP),
        CAST(DATE_TRUNC('day', CURRENT_TIMESTAMP) AS TIMESTAMP),
        INTERVAL '1' DAY
      )
    ) AS _u(day)
  ),
  -- Holder balances expanded to all days between transfer events
  balance_all_days AS (
    SELECT
      d.day,
      b.address,
      SUM(b.balance) / 1e18 AS balance_tokens
    FROM balances_with_gap_days b
    JOIN days d
      ON b.day <= d.day AND d.day < b.next_day
    GROUP BY 1, 2
  )
-- Daily holder count and day-over-day change
SELECT
  b.day AS "date",
  COUNT_IF(balance_tokens > (SELECT threshold FROM thresh)) AS "holders",
  COUNT_IF(balance_tokens > (SELECT threshold FROM thresh))
    - LAG(COUNT_IF(balance_tokens > (SELECT threshold FROM thresh))) OVER (ORDER BY b.day)
    AS "one_day_change"
FROM balance_all_days b
GROUP BY 1
ORDER BY 1;