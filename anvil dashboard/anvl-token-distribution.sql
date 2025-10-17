-- Top holders + "All Others" for a given ERC-20
WITH
  params AS (
    SELECT CAST(0xAEEAa594e7dc112D67b8547fe9767a02c15B5597 AS varbinary) AS token
  ),

  -- Signed transfer rows (from = negative, to = positive)
  transfers AS (
    SELECT
      tr."from" AS addr,
      -CAST(tr.value AS INT256) AS amt
    FROM erc20_ethereum.evt_Transfer tr
    JOIN params p ON tr.contract_address = p.token

    UNION ALL

    SELECT
      tr."to"   AS addr,
      CAST(tr.value AS INT256) AS amt
    FROM erc20_ethereum.evt_Transfer tr
    JOIN params p ON tr.contract_address = p.token
  ),

  -- Resolve token decimals once
  token_meta AS (
    SELECT
      p.token,
      COALESCE(t.decimals, 18) AS decimals
    FROM params p
    LEFT JOIN tokens.erc20 t ON t.contract_address = p.token
  ),

  -- Aggregate balances (native units -> human units using decimals)
  balances AS (
    SELECT
      -- display like 0x1234...cdef
      CONCAT(
        '0x',
        SUBSTRING(hex(lower(addr)), 1, 4),
        '...',
        SUBSTRING(hex(lower(addr)), LENGTH(hex(lower(addr))) - 3, 4)
      ) AS address,
      SUM(amt) / CAST(power(10, (SELECT decimals FROM token_meta)) AS DOUBLE) AS balance
    FROM transfers
    GROUP BY 1
  ),

  top10 AS (
    SELECT address, balance
    FROM balances
    ORDER BY balance DESC
    LIMIT 10
  )

-- Final output: top 10 + "All Others"
SELECT address, balance
FROM top10

UNION ALL

SELECT
  '🌎 All Others' AS address,
  SUM(balance)    AS balance
FROM balances
WHERE address NOT IN (SELECT address FROM top10)
  AND balance > 0;