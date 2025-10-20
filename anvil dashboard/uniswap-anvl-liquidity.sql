WITH
-- Restrict analysis to a specific pool
pools AS (
  SELECT CAST(0x0cd3f76deaa204f2d25ccb8cba079e719143f44ec40a9b43d3dd2ca21b844922 AS VARBINARY) AS pool_id
),

-- Analysis start date and TVL cutoff window
start_cfg AS (
  SELECT DATE('2025-10-01') AS start_date
),
tvl_cut AS (
  SELECT date_trunc('day', now() - interval '24' hour) AS ts
),

-- Tokens we might need prices for (token1 side from the target pool)
params AS (
  SELECT DISTINCT d.token1 AS token_address
  FROM uniswap.tvl_daily d
  JOIN pools pl ON d.id = pl.pool_id
  WHERE d.blockchain = 'ethereum'
    AND d.version = '4'
),

-- Latest trade-derived prices per token
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
),

-- Carry ALL columns from tvl_daily, add period and computed tvl with token1 USD fallback
tvl_per_pool AS (
  SELECT
    d.block_date AS period,
    d.id,
    d.blockchain,
    d.version,
    d.token0,
    d.token1,
    d.token0_symbol,
    d.token1_symbol,
    d.token0_balance_raw,
    d.token1_balance_raw,
    d.token0_balance,
    d.token1_balance,
    d.token0_balance_usd,
    d.token1_balance_usd,
    -- replace token1_balance_usd if missing with token1_balance * latest price
    COALESCE(
      NULLIF(d.token1_balance_usd, 0),
      d.token1_balance * COALESCE(pl1.price_usd, 0)
    ) AS token1_balance_usd,
    -- recompute tvl with corrected token1_balance_usd
    COALESCE(d.token0_balance_usd, 0)
    + COALESCE(
        NULLIF(d.token1_balance_usd, 0),
        d.token1_balance * COALESCE(pl1.price_usd, 0)
      ) AS tvl
  FROM uniswap.tvl_daily d
  JOIN tvl_cut c  ON d.block_date <= c.ts
  JOIN pools pl   ON d.id = pl.pool_id
  LEFT JOIN (
    SELECT token_address, price_usd
    FROM price_latest
    WHERE rn = 1
  ) pl1 ON d.token1 = pl1.token_address
  WHERE d.blockchain = 'ethereum'
    AND d.version = '4'
),

-- Enrich with pair metadata
pool_data AS (
  SELECT
    t.*,
    p.pair_w_null,
    p.pool
  FROM tvl_per_pool t
  LEFT JOIN dune.uniswap_fnd.result_uniswap_v_4_all_pools_data p
    ON t.id = p.pool
   AND p.blockchain = 'ethereum'
),

-- Latest row per pool (keeps all columns from tvl_per_pool/pool_data)
tvl AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY pool ORDER BY period DESC) AS rn
    FROM pool_data
  )
  WHERE rn = 1
),

-- Volume windows
volume AS (
  SELECT
    p.pool,
    SUM(t.amount_usd) FILTER (WHERE t.block_time >= now() - INTERVAL '7'  DAY)   AS l7d_vol,
    SUM(t.amount_usd) FILTER (WHERE t.block_time >= now() - INTERVAL '30' DAY)   AS l30d_vol,
    SUM(t.amount_usd) FILTER (WHERE t.block_time >= now() - INTERVAL '24' HOUR)  AS l24h_vol
  FROM dex.trades t
  JOIN dune.uniswap_fnd.result_uniswap_v_4_all_pools_data p
    ON p.pool = t.maker
   AND p.blockchain = t.blockchain
  JOIN pools pl ON p.pool = pl.pool_id
  CROSS JOIN start_cfg s
  WHERE t.blockchain = 'ethereum'
    AND t.project    = 'uniswap'
    AND t.version    = '4'
    AND t.block_time >= s.start_date
  GROUP BY 1
),

-- Fee windows (from PoolManager swap fee)
fee AS (
  SELECT
    p.pool,
    SUM(x.fee_usd) FILTER (WHERE x.block_time >= now() - INTERVAL '30' DAY) AS l30d_fee,
    SUM(x.fee_usd) FILTER (WHERE x.block_time >= now() - INTERVAL '7'  DAY) AS l7d_fee,
    SUM(x.fee_usd) FILTER (WHERE x.block_time >= now() - INTERVAL '24' HOUR) AS l24h_fee
  FROM (
    SELECT
      t.block_time,
      t.blockchain,
      t.maker AS pool_addr,
      CAST(t.amount_usd AS DOUBLE) * CAST(ev.fee / 1e6 AS DOUBLE) AS fee_usd
    FROM uniswap_v4_ethereum.PoolManager_evt_Swap ev
    JOIN dex.trades t
      ON t.blockchain   = 'ethereum'
     AND t.project      = 'uniswap'
     AND t.version      = '4'
     AND t.block_number = ev.evt_block_number
     AND t.tx_hash      = ev.evt_tx_hash
     AND t.evt_index    = ev.evt_index
    CROSS JOIN start_cfg s
    WHERE t.block_time >= s.start_date
  ) x
  JOIN dune.uniswap_fnd.result_uniswap_v_4_all_pools_data p
    ON p.pool = x.pool_addr
   AND p.blockchain = x.blockchain
  JOIN pools pl ON p.pool = pl.pool_id
  GROUP BY 1
),

-- Final composition: keep ALL tvl columns and add metrics/links
complete_data AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY tvl.tvl DESC) AS rank,
    tvl.*,
    get_href(
      'https://app.uniswap.org/explore/pools/ethereum/' || CAST(tvl.pool AS VARCHAR),
      tvl.pair_w_null
    ) AS pair_url,
    v.l24h_vol, v.l7d_vol, v.l30d_vol,
    f.l24h_fee, f.l7d_fee, f.l30d_fee,
    CAST(v.l24h_vol AS DOUBLE) / NULLIF(CAST(tvl.tvl AS DOUBLE), 0) AS l24h_vol_tvl_turnover,
    ROUND( (f.l7d_fee  / NULLIF(tvl.tvl,0)) * 52.14, 2) AS apr_7d,
    ROUND( (f.l30d_fee / NULLIF(tvl.tvl,0)) * 12.17, 2) AS apr_30d
  FROM tvl
  LEFT JOIN volume v ON tvl.pool = v.pool
  LEFT JOIN fee    f ON tvl.pool = f.pool
)

SELECT *
FROM complete_data;