-- Amp Holders Over Time
-- Dune query 381317: https://dune.com/queries/381317
-- From dashboard: https://dune.com/ampdotxyz/amp-token

WITH
  addresses AS (
    SELECT
      0xff20817765cb7f73d4bde2e66e067e58d11095c2 AS address
  ),
  thresh AS (
    SELECT
      1 AS threshhold
  ),
  decimals AS (
    SELECT
      18 AS decimals
  ),
  transfers AS (
    SELECT
      DAY,
      address,
      token_address,
      Sum(amount) AS amount
      /* Net inflow or outflow per day */
    FROM
      (
        SELECT
          DATE_TRUNC('day', evt_block_time) AS DAY,
          "to" AS address,
          tr.contract_address AS token_address,
          Cast(value AS INT256) AS amount
        FROM
          erc20_ethereum.evt_Transfer AS tr,
          addresses
        WHERE
          contract_address = address
        UNION ALL
        SELECT
          DATE_TRUNC('day', evt_block_time) AS DAY,
          "from" AS address,
          tr.contract_address AS token_address,
          - Cast(value AS INT256) AS amount
        FROM
          erc20_ethereum.evt_Transfer AS tr,
          addresses
        WHERE
          contract_address = address
      ) AS t
    GROUP BY
      1,
      2,
      3
  ),
  balances_with_gap_days AS (
    SELECT
      t.day,
      address,
      Sum(amount) OVER (
        PARTITION BY
          address
        ORDER BY
          t.day
      ) AS balance,
      /* balance per day with a transfer */
      LEAD(DAY, 1, CURRENT_TIMESTAMP) OVER (
        PARTITION BY
          address
        ORDER BY
          t.day
      ) AS next_day
      /* the day after a day with a transfer */
    FROM
      transfers AS t
  ),
  days AS (
    SELECT
      DAY
    FROM
      UNNEST (
        SEQUENCE(
          TRY_CAST('2020-08-01' AS TIMESTAMP),
          CAST(
            CAST(
              CAST(DATE_TRUNC('day', CURRENT_TIMESTAMP) AS TIMESTAMP) AS TIMESTAMP
            ) AS TIMESTAMP
          ),
          INTERVAL '1' DAY
        )
      ) AS _u (DAY)
  ),
  balance_all_days AS (
    SELECT
      d.day,
      address,
      SUM(balance) / 1e18 AS balance
    FROM
      balances_with_gap_days AS b
      INNER JOIN days AS d ON b.day <= d.day
      AND d.day < b.next_day
      /* Yields an observation for every day after the first transfer until the next day with transfer */
      LEFT JOIN thresh ON 1 = 1
      LEFT JOIN decimals ON 1 = 1
    WHERE
      balance / 1e18 > threshhold
    GROUP BY
      1,
      2
    ORDER BY
      1,
      2
  )
SELECT
  b.day AS "Date",
  COUNT(address) AS "AMP Holders",
  COUNT(address) - LAG(COUNT(address)) OVER (
    ORDER BY
      b.day
  ) AS "Change in 1 Day"
FROM
  balance_all_days AS b
GROUP BY
  1
ORDER BY
  1
