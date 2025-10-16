-- ============ Params ============
WITH
params AS (
  SELECT
    0xAEEAa594e7dc112D67b8547fe9767a02c15B5597 AS token_address
),

-- ============ Token Meta (decimals) ============
token_meta AS (
  SELECT
    p.token_address,
    COALESCE(t.decimals, 18) AS decimals
  FROM params p
  LEFT JOIN tokens.erc20 t
    ON t.contract_address = p.token_address
   AND t.blockchain = 'ethereum'
),

-- ============ Balance Diffs (supply-by-transfers) ============
anvl AS (
  -- Incoming transfers
  SELECT
    "to" AS holder,
    e.contract_address AS token_address,
    CAST("value" AS DECIMAL(38,0)) AS balance_change
  FROM erc20_ethereum.evt_Transfer e
  JOIN params p ON e.contract_address = p.token_address
  WHERE "to" <> 0x0000000000000000000000000000000000000000

  UNION ALL

  -- Outgoing transfers
  SELECT
    "from" AS holder,
    e.contract_address AS token_address,
    -CAST("value" AS DECIMAL(38,0)) AS balance_change
  FROM erc20_ethereum.evt_Transfer e
  JOIN params p ON e.contract_address = p.token_address
  WHERE "from" <> 0x0000000000000000000000000000000000000000
),

-- ============ Total Supply (base units) ============
ts AS (
  SELECT
    a.token_address,
    SUM(a.balance_change) AS raw_total_supply
  FROM anvl a
  GROUP BY a.token_address
),

-- ============ Scaling Factor (10 ^ decimals) ============
scale AS (
  SELECT
    tm.token_address,
    CAST(POWER(10, tm.decimals) AS DECIMAL(38,0)) AS scale
  FROM token_meta tm
),

-- ============ Latest Price from DEX trades ============
price_latest AS (
  SELECT
    d.token_bought_address AS token_address,
    CAST(d.amount_usd / NULLIF(d.token_bought_amount, 0) AS DOUBLE) AS price_usd,
    ROW_NUMBER() OVER (
      PARTITION BY d.token_bought_address
      ORDER BY d.block_time DESC, d.tx_hash DESC
    ) AS rn
  FROM dex.trades d
  JOIN params p ON d.token_bought_address = p.token_address
  WHERE d.blockchain = 'ethereum'
    AND d.amount_usd IS NOT NULL AND d.amount_usd <> 0
    AND d.token_bought_amount IS NOT NULL AND d.token_bought_amount <> 0
)

-- ============ Final Output ============
SELECT
  CAST(ts.raw_total_supply / s.scale AS DOUBLE) AS total_supply,
  ts.token_address                              AS address,
  pl.price_usd                                   AS price,
  CAST(ts.raw_total_supply / s.scale AS DOUBLE) * pl.price_usd AS fdv
FROM ts
JOIN scale s              ON s.token_address = ts.token_address
LEFT JOIN price_latest pl ON pl.token_address = ts.token_address AND pl.rn = 1;