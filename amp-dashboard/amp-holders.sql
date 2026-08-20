-- Amp Holders
-- Dune query 381175: https://dune.com/queries/381175
-- From dashboard: https://dune.com/ampdotxyz/amp-token

WITH
  amp_staking AS (
    SELECT
      "from" AS "staker",
      "contract_address" AS "token_address",
      CAST("value" as INT256) AS "balanceChange"
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      "to" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      AND "contract_address" = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    UNION ALL
    SELECT
      "to" AS "staker",
      "contract_address" AS "token_address",
      - CAST("value" as INT256) AS "balanceChange"
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      "from" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      AND "contract_address" = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
  ),
  amp_staking_grouped AS (
    SELECT
      "staker",
      "token_address",
      SUM("balanceChange") / 1e18 AS "tokens_staked"
    FROM
      amp_staking
    GROUP BY
      1,
      2
  ),
  positive_flow AS (
    SELECT
      COUNT("staker") AS "currently_staked",
      "token_address"
    FROM
      amp_staking_grouped
    WHERE
      "tokens_staked" > 0
    GROUP BY
      2
  ),
  transfers AS (
    SELECT
      "from" AS "holder",
      "contract_address" AS "token_address",
      - CAST("value" as INT256) AS "amount"
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    UNION ALL
    SELECT
      "to" AS "holder",
      "contract_address" AS "token_address",
      CAST("value" as INT256) AS "amount"
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
  ),
  transferamounts AS (
    SELECT
      "holder",
      "token_address",
      SUM("amount") / 1e18 AS "poolholdings"
      --   SUM("amount") / CAST(1e18 AS DOUBLE) AS "poolholdings"
    FROM
      transfers
    GROUP BY
      1,
      2
    ORDER BY
      3 DESC
  ),
  total_holders AS (
    SELECT
      COUNT(DISTINCT ("holder")) AS "holders",
      "token_address"
    FROM
      transferamounts
    WHERE
      poolholdings > 0
    GROUP BY
      2
  )
SELECT
  total_holders.holders AS "holders",
  positive_flow.currently_staked + total_holders.holders AS "total_on-chain_holders",
  positive_flow.currently_staked AS "staked"
FROM
  total_holders
  INNER JOIN positive_flow ON positive_flow.token_address = total_holders.token_address
