-- Amp CEX Withdrawals/Deposits Last 30 Days
-- Dune query 1017568: https://dune.com/queries/1017568
-- From dashboard: https://dune.com/ampdotxyz/amp-token

WITH
  cex_deposits AS (
    SELECT
      cex.cex_name,
      SUM(CAST(value AS INT256)) / 1e18 AS deposit_quantity
    FROM
      erc20_ethereum.evt_Transfer AS et
      LEFT JOIN cex.addresses AS cex ON (
        cex.address = et.to
        OR cex.address = et."from"
      )
    WHERE
      cex.address = et.to
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '30' day
      AND NOT cex.cex_name IS NULL
    GROUP BY
      1
  ),
  cex_withdrawals AS (
    SELECT
      cex.cex_name,
      SUM(CAST(value AS INT256)) / 1e18 AS withdrawal_quantity
    FROM
      erc20_ethereum.evt_Transfer AS et
      LEFT JOIN cex.addresses AS cex ON (
        cex.address = et.to
        OR cex.address = et."from"
      )
    WHERE
      cex.address = et."from"
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
      AND evt_block_time >= CURRENT_TIMESTAMP - INTERVAL '30' day
      AND NOT cex.cex_name IS NULL
    GROUP BY
      1
  )
SELECT
  COALESCE(wd.cex_name, d.cex_name) as exchange,
  COALESCE(wd.withdrawal_quantity, 0) as withdrawals,
  COALESCE(d.deposit_quantity, 0) * -1 as deposits,
  COALESCE(wd.withdrawal_quantity, 0) - COALESCE(d.deposit_quantity, 0) AS netflow
FROM
  cex_withdrawals AS wd
  FULL JOIN cex_deposits AS d ON (wd.cex_name = d.cex_name)
ORDER BY
  1
