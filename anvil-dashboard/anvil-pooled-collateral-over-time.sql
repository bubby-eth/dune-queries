-- Anvil Pooled Collateral Over Time
-- Dune query 8424124: https://dune.com/queries/8424124
-- From dashboard: https://dune.com/anvil/anvil
--
-- Daily view of the TimeBasedCollateralPool system: USD value of collateral
-- held in the vault under pool accounts (pooled_usd, vault-side decoded
-- accounting) and the number of active stakers across all pools (accounts with
-- positive pool units, from the decoded
-- anvil_ethereum.timebasedcollateralpool_evt_* tables). One query, two widgets: an area
-- chart mapping pooled_usd and a line chart mapping active_stakers.
-- Pool list is discovered from CollateralizableContractApprovalUpdated.
WITH
  pools AS (
    SELECT contractAddress AS pool
    FROM anvil_ethereum.collateralvault_evt_collateralizablecontractapprovalupdated
    WHERE isCollateralPool
    GROUP BY 1
  ),

  -- vault-side daily flows into/out of pool accounts, per token
  vault_flows AS (
    SELECT f.day, f.token, f.amt
    FROM (
      SELECT evt_block_date AS day, toAccount AS account, tokenAddress AS token,
             CAST(amount AS int256) AS amt
      FROM anvil_ethereum.collateralvault_evt_fundsdeposited

      UNION ALL

      SELECT evt_block_date, fromAccount, tokenAddress, -CAST(amountWithFee AS int256)
      FROM anvil_ethereum.collateralvault_evt_fundswithdrawn

      UNION ALL

      SELECT evt_block_date, toAccount, tokenAddress, CAST(tokenAmount AS int256)
      FROM anvil_ethereum.collateralvault_evt_collateraltransferred

      UNION ALL

      SELECT evt_block_date, fromAccount, tokenAddress, -CAST(tokenAmount AS int256)
      FROM anvil_ethereum.collateralvault_evt_collateraltransferred

      UNION ALL

      SELECT c.evt_block_date, r.account, r.tokenAddress, -CAST(c.amountWithFee AS int256)
      FROM anvil_ethereum.collateralvault_evt_collateralclaimed c
      JOIN anvil_ethereum.collateralvault_evt_collateralreserved r
        ON r.reservationId = c.reservationId
    ) f
    JOIN pools p ON p.pool = f.account
  ),

  daily_flows AS (
    SELECT day, token, SUM(amt) AS amt
    FROM vault_flows
    GROUP BY 1, 2
  ),

  day_spine AS (
    SELECT CAST(d AS date) AS day
    FROM (SELECT MIN(day) AS start_day FROM daily_flows)
    CROSS JOIN UNNEST(SEQUENCE(start_day, CURRENT_DATE, INTERVAL '1' DAY)) AS t(d)
  ),

  pooled_usd_daily AS (
    SELECT
      b.day,
      SUM(b.raw_balance / POWER(10, COALESCE(tk.decimals, 18)) * p.price) AS pooled_usd
    FROM (
      SELECT s.day, tok.token,
             SUM(COALESCE(df.amt, CAST(0 AS int256)))
               OVER (PARTITION BY tok.token ORDER BY s.day) AS raw_balance
      FROM day_spine s
      CROSS JOIN (SELECT DISTINCT token FROM daily_flows) tok
      LEFT JOIN daily_flows df ON df.token = tok.token AND df.day = s.day
    ) b
    LEFT JOIN tokens.erc20 tk
      ON tk.blockchain = 'ethereum' AND tk.contract_address = b.token
    LEFT JOIN prices.usd_daily p
      ON p.blockchain = 'ethereum' AND p.contract_address = b.token AND p.day = b.day
    GROUP BY 1
  ),

  -- pool-side stake/unstake units per staker per day (decoded pool tables)
  staker_flows AS (
    SELECT day, pool, staker, SUM(units) AS units
    FROM (
      SELECT
        s.evt_block_date                  AS day,
        s.contract_address                AS pool,
        s.account                         AS staker,
        CAST(s.poolUnitsIssued AS int256) AS units
      FROM anvil_ethereum.timebasedcollateralpool_evt_collateralstaked s
      JOIN pools p ON p.pool = s.contract_address

      UNION ALL

      SELECT
        u.evt_block_date,
        u.contract_address,
        u.account,
        -CAST(u.unitsToUnstake AS int256)
      FROM anvil_ethereum.timebasedcollateralpool_evt_unstakeinitiated u
      JOIN pools p ON p.pool = u.contract_address
    )
    GROUP BY 1, 2, 3
  ),

  -- accounts holding positive units on each day (per pool+staker running sum;
  -- DISTINCT so an account staked in several pools counts once)
  active_stakers_daily AS (
    SELECT day, COUNT(DISTINCT staker) AS active_stakers
    FROM (
      SELECT s.day, sf.pool, sf.staker,
             SUM(COALESCE(f.units, CAST(0 AS int256)))
               OVER (PARTITION BY sf.pool, sf.staker ORDER BY s.day) AS units
      FROM day_spine s
      CROSS JOIN (SELECT DISTINCT pool, staker FROM staker_flows) sf
      LEFT JOIN staker_flows f
        ON f.pool = sf.pool AND f.staker = sf.staker AND f.day = s.day
    )
    WHERE units > CAST(0 AS int256)
    GROUP BY 1
  )

SELECT
  u.day,
  u.pooled_usd,
  COALESCE(a.active_stakers, 0) AS active_stakers
FROM pooled_usd_daily u
LEFT JOIN active_stakers_daily a ON a.day = u.day
WHERE u.pooled_usd IS NOT NULL
ORDER BY u.day
