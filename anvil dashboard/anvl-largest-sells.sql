WITH address_balances AS (
  WITH transfers AS (
    SELECT
      tr."from" AS address,
      -CAST(tr.value AS INT256) AS amount
    FROM
      erc20_ethereum.evt_Transfer AS tr
    WHERE
      contract_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
    UNION ALL
    SELECT
      tr."to" AS address,
      CAST(tr.value AS INT256) AS amount
    FROM
      erc20_ethereum.evt_Transfer AS tr
    WHERE
      contract_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
  )
  SELECT
    address,
    SUM(amount) / 1e18 AS quantity
  FROM
    transfers
  GROUP BY
    address
  HAVING
    SUM(amount) > 0
  ORDER BY 
    quantity DESC
)
SELECT
    block_time,
    CONCAT(
        '<a href="https://etherscan.io/address/', 
        CAST(tx_from AS VARCHAR), 
        '" target="_blank">', 
        CONCAT(
            SUBSTRING(CAST(tx_from AS VARCHAR) FROM 1 FOR 5),
            '...',
            SUBSTRING(CAST(tx_from AS VARCHAR) FROM 39 FOR 4)),
        '</a>'
    ) AS tx_from,
    token_sold_amount,
    amount_usd,
    amount_usd / token_sold_amount AS price,
    CASE
        WHEN COALESCE(ab.quantity, 0) < 1e-6 THEN 0
        ELSE COALESCE(ab.quantity, 0)
    END AS remaining_balance,
    CONCAT(
        '<a href="https://etherscan.io/tx/', 
        CAST(tx_hash AS VARCHAR), 
        '" target="_blank">', 
        CONCAT(
            SUBSTRING(CAST(tx_hash AS VARCHAR) FROM 1 FOR 5),
            '...',
            SUBSTRING(CAST(tx_hash AS VARCHAR) FROM 63 FOR 4)), 
        '</a>'
    ) AS tx_hash,
    DATE_DIFF('day', block_time, CURRENT_TIMESTAMP) AS days_ago
FROM
    dex.trades
LEFT JOIN
    address_balances ab ON dex.trades.tx_from = ab.address
WHERE
    block_time > CURRENT_TIMESTAMP - INTERVAL '7' DAY
    AND token_sold_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
ORDER BY
    token_sold_amount DESC
LIMIT 10;