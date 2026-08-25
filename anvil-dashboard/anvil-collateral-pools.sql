-- Anvil Collateral Pools
-- Dune query 8424123: https://dune.com/queries/8424123
-- From dashboard: https://dune.com/anvil/anvil
--
-- One row per TimeBasedCollateralPool: collateral held in the vault under the
-- pool's account (TVL, from decoded CollateralVault events), active stakers
-- (accounts with positive pool units, from the decoded
-- anvil_ethereum.timebasedcollateralpool_evt_* tables, which cover all beacon
-- proxy instances), share of all pooled value, and first/last
-- pool activity linked to its tx. The pool list is discovered from the vault's
-- CollateralizableContractApprovalUpdated events (isCollateralPool = true), so
-- newly deployed pools show up automatically. Pool units convert 1:1 to tokens
-- until a pool claim or reset changes the ratio (none to date).
WITH
  pools AS (
    SELECT contractAddress AS pool
    FROM anvil_ethereum.collateralvault_evt_collateralizablecontractapprovalupdated
    WHERE isCollateralPool
    GROUP BY 1
    HAVING MAX_BY(approved, evt_block_number)  -- still approved
  ),

  -- vault-side balance of each pool account (same accounting as top depositors)
  vault_flows AS (
    SELECT toAccount AS account, tokenAddress AS token, CAST(amount AS int256) AS amt
    FROM anvil_ethereum.collateralvault_evt_fundsdeposited

    UNION ALL

    SELECT fromAccount, tokenAddress, -CAST(amountWithFee AS int256)
    FROM anvil_ethereum.collateralvault_evt_fundswithdrawn

    UNION ALL

    SELECT toAccount, tokenAddress, CAST(tokenAmount AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateraltransferred

    UNION ALL

    SELECT fromAccount, tokenAddress, -CAST(tokenAmount AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateraltransferred

    UNION ALL

    SELECT r.account, r.tokenAddress, -CAST(c.amountWithFee AS int256)
    FROM anvil_ethereum.collateralvault_evt_collateralclaimed c
    JOIN anvil_ethereum.collateralvault_evt_collateralreserved r
      ON r.reservationId = c.reservationId
  ),

  pool_balances AS (
    SELECT f.account AS pool, f.token, SUM(f.amt) AS raw_balance
    FROM vault_flows f
    JOIN pools p ON p.pool = f.account
    GROUP BY 1, 2
    HAVING SUM(f.amt) > CAST(0 AS int256)
  ),

  latest_price AS (
    SELECT contract_address, MAX_BY(price, day) AS price
    FROM prices.usd_daily
    WHERE blockchain = 'ethereum'
      AND contract_address IN (SELECT DISTINCT token FROM pool_balances)
    GROUP BY 1
  ),

  pool_values AS (
    SELECT
      pb.pool,
      SUM(pb.raw_balance / POWER(10, COALESCE(tk.decimals, 18)))            AS staked_tokens,
      MAX_BY(COALESCE(tk.symbol, CAST(pb.token AS varchar)),
             pb.raw_balance / POWER(10, COALESCE(tk.decimals, 18)) * lp.price) AS asset,
      SUM(pb.raw_balance / POWER(10, COALESCE(tk.decimals, 18)) * lp.price) AS tvl_usd
    FROM pool_balances pb
    LEFT JOIN tokens.erc20 tk
      ON tk.blockchain = 'ethereum' AND tk.contract_address = pb.token
    LEFT JOIN latest_price lp ON lp.contract_address = pb.token
    GROUP BY 1
  ),

  -- pool-side stake/unstake events (decoded TimeBasedCollateralPool tables)
  stake_events AS (
    SELECT
      s.contract_address              AS pool,
      s.account                       AS staker,
      CAST(s.poolUnitsIssued AS int256) AS units,
      s.evt_block_time                AS block_time,
      s.evt_tx_hash                   AS tx_hash
    FROM anvil_ethereum.timebasedcollateralpool_evt_collateralstaked s
    JOIN pools p ON p.pool = s.contract_address

    UNION ALL

    SELECT
      u.contract_address,
      u.account,
      -CAST(u.unitsToUnstake AS int256),
      u.evt_block_time,
      u.evt_tx_hash
    FROM anvil_ethereum.timebasedcollateralpool_evt_unstakeinitiated u
    JOIN pools p ON p.pool = u.contract_address
  ),

  pool_stakers AS (
    SELECT pool, COUNT(*) AS stakers
    FROM (
      SELECT pool, staker
      FROM stake_events
      GROUP BY 1, 2
      HAVING SUM(units) > CAST(0 AS int256)
    )
    GROUP BY 1
  ),

  pool_activity AS (
    SELECT
      pool,
      MIN(block_time)             AS first_at,
      MIN_BY(tx_hash, block_time) AS first_tx,
      MAX(block_time)             AS last_at,
      MAX_BY(tx_hash, block_time) AS last_tx
    FROM stake_events
    GROUP BY 1
  )

SELECT
  ROW_NUMBER() OVER (ORDER BY pv.tvl_usd DESC) AS rank,
  SUBSTR(CAST(pv.pool AS varchar), 1, 6) || '...' ||
    SUBSTR(CAST(pv.pool AS varchar), 39) ||
    ' | <a href="https://etherscan.io/address/' || CAST(pv.pool AS varchar) ||
    '" target="_blank">Etherscan</a>' AS pool,
  pv.asset,
  pv.staked_tokens,
  pv.tvl_usd,
  COALESCE(ps.stakers, 0)                          AS stakers,
  100.0 * pv.tvl_usd / SUM(pv.tvl_usd) OVER ()     AS share_pct,
  '<a href="https://etherscan.io/tx/' || CAST(pa.first_tx AS varchar) ||
    '" target="_blank">' || CAST(CAST(pa.first_at AS date) AS varchar) ||
    '</a>' AS first_stake,
  '<a href="https://etherscan.io/tx/' || CAST(pa.last_tx AS varchar) ||
    '" target="_blank">' || CAST(CAST(pa.last_at AS date) AS varchar) ||
    '</a>' AS last_activity
FROM pool_values pv
LEFT JOIN pool_stakers ps ON ps.pool = pv.pool
LEFT JOIN pool_activity pa ON pa.pool = pv.pool
ORDER BY pv.tvl_usd DESC
