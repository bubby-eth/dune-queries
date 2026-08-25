-- ANVL
-- Dune query 3839828: https://dune.com/queries/3839828
-- From dashboard: https://dune.com/anvil/anvil
--
-- Total supply, latest DEX price, and FDV for the ANVL token. Supply is
-- mints minus burns (zero-address transfers only — summing every holder's
-- balance change gives the same number at many times the scan cost); the
-- price is the most recent dex.trades fill. Both scans are bounded to the
-- token's Oct 2025 deployment.
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

-- ============ Total Supply (mints − burns, base units) ============
ts AS (
  SELECT
    e.contract_address AS token_address,
    SUM(CASE WHEN e."from" = 0x0000000000000000000000000000000000000000
             THEN CAST(e.value AS DECIMAL(38,0))
             ELSE -CAST(e.value AS DECIMAL(38,0)) END) AS raw_total_supply
  FROM erc20_ethereum.evt_Transfer e
  JOIN params p ON e.contract_address = p.token_address
  WHERE e.evt_block_time >= TIMESTAMP '2025-10-01'
    AND (   e."from" = 0x0000000000000000000000000000000000000000
         OR e."to"   = 0x0000000000000000000000000000000000000000)
  GROUP BY 1
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
    AND d.block_time >= TIMESTAMP '2025-10-01'
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
