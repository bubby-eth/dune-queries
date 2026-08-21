-- Amp DEX Liquidity
-- Dune query 2813063: https://dune.com/queries/2813063
-- From dashboard: https://dune.com/ampdotxyz/amp-token
--
-- TVL of AMP liquidity pools per DEX. Pools are discovered dynamically from
-- factory creation events (any new AMP pool is picked up automatically):
--   Uniswap V2 / Sushi:  factory_evt_paircreated (token0/token1 = AMP)
--   Uniswap V3:          factory_evt_poolcreated (token0/token1 = AMP)
--   Bancor:              legacy static list (protocol deprecated, no new pools)
-- TVL for those pools = current balance of every token the pool holds
-- (reconstructed from transfers), valued at prices.usd_latest.
--
-- Uniswap V4 is a singleton: all pools' tokens sit inside the PoolManager, so
-- per-pool address balances don't exist. The PoolManager's AMP balance is an
-- exact measure of the AMP side of all v4 AMP pools; total v4 TVL is
-- approximated as 2x that (balanced-liquidity assumption -- exact for the AMP
-- side, approximate for the paired side).
WITH
  pools AS (
    SELECT 'Uniswap V2' AS dex, pair AS pool
    FROM uniswap_v2_ethereum.factory_evt_paircreated
    WHERE token0 = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
       OR token1 = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2

    UNION ALL

    SELECT 'Uniswap V3', pool
    FROM uniswap_v3_ethereum.factory_evt_poolcreated
    WHERE token0 = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
       OR token1 = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2

    UNION ALL

    SELECT 'Sushi', pair
    FROM sushi_ethereum.factory_evt_paircreated
    WHERE token0 = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
       OR token1 = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2

    UNION ALL

    -- Bancor v2/v3 AMP pools (deprecated protocol; addresses are final)
    SELECT 'Bancor', pool
    FROM (VALUES
      (0x0f4a8a06c22ba49e98d15223a701062c40873f7a),
      (0x6a74941c1cf4151b3f15cdd84ee3abde713a999b),
      (0x2f9ec37d6ccfff1cab21733bdadede11c823ccb0)
    ) AS v(pool)
  ),

  -- every token flow in/out of any pool (single transfer scan, both sides)
  legs AS (
    SELECT f.dex, tr.contract_address AS token_address, f.amount
    FROM erc20_ethereum.evt_Transfer tr
    LEFT JOIN pools pt ON pt.pool = tr."to"
    LEFT JOIN pools pf ON pf.pool = tr."from"
    CROSS JOIN UNNEST(
      ARRAY[
        ROW(pt.dex,  CAST(tr.value AS int256)),
        ROW(pf.dex, -CAST(tr.value AS int256))
      ]
    ) AS f(dex, amount)
    WHERE (pt.pool IS NOT NULL OR pf.pool IS NOT NULL)
      AND f.dex IS NOT NULL
  ),

  balances AS (
    SELECT l.dex, l.token_address,
           SUM(l.amount) / POW(10, COALESCE(MAX(tk.decimals), 18)) AS amount
    FROM legs l
    LEFT JOIN tokens.erc20 tk
      ON tk.contract_address = l.token_address AND tk.blockchain = 'ethereum'
    GROUP BY 1, 2
    HAVING SUM(l.amount) > CAST(0 AS int256)
  ),

  addressable_tvl AS (
    SELECT b.dex AS pair_address, SUM(b.amount * px.price) AS tvl
    FROM balances b
    JOIN prices.usd_latest px
      ON px.contract_address = b.token_address AND px.blockchain = 'ethereum'
    WHERE b.amount * px.price > 1
    GROUP BY 1
    HAVING SUM(b.amount * px.price) > 1
  ),

  -- Uniswap V4: AMP side held by the PoolManager singleton, x2 (see header)
  v4_tvl AS (
    SELECT 'Uniswap V4' AS pair_address,
           2 * SUM(
             CASE WHEN tr."to" IN (SELECT contract_address FROM uniswap_v4_ethereum.poolmanager_evt_initialize)
                  THEN CAST(tr.value AS int256) ELSE -CAST(tr.value AS int256) END
           ) / 1e18 * (
             SELECT price FROM prices.usd_latest
             WHERE blockchain = 'ethereum'
               AND contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
           ) AS tvl
    FROM erc20_ethereum.evt_Transfer tr
    WHERE tr.contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
      AND (   tr."to"   IN (SELECT contract_address FROM uniswap_v4_ethereum.poolmanager_evt_initialize)
           OR tr."from" IN (SELECT contract_address FROM uniswap_v4_ethereum.poolmanager_evt_initialize))
  )

SELECT pair_address, tvl FROM addressable_tvl
UNION ALL
SELECT pair_address, tvl FROM v4_tvl WHERE tvl > 1
ORDER BY 1
