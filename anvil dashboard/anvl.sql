WITH
  anvl AS (
    SELECT
      "to" AS "holder",
      contract_address AS token_address,
      CAST("value" AS INT256) AS "balanceChange"
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      "to" <> 0x0000000000000000000000000000000000000000
      AND contract_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597 --0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc
    UNION ALL
    SELECT
      "from" AS "holder",
      contract_address AS token_address,
      - CAST("value" AS INT256) AS "balanceChange"
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      "from" <> 0x0000000000000000000000000000000000000000
      AND contract_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597 --0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc
  ),
  ts AS (
    SELECT
      token_address,
      SUM("balanceChange") / 1e18 AS "total_supply"
    FROM
      anvl
    GROUP BY
      token_address
  ),
  unclaimed AS (
    SELECT
      token_address,
      SUM("balanceChange") / 1e18 AS "unclaimed"
    FROM
      anvl
    WHERE
      holder IN (0xeFd194D4Ff955E8958d132319F31D2aB9f7E29Ac /* Claim address */)
    GROUP BY
      token_address
  ),
  price_query AS (
    SELECT
      amount_usd / token_bought_amount AS price,
      token_bought_address
    FROM
      dex.trades
    WHERE
      token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597 --0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc
      AND amount_usd IS NOT NULL
      AND amount_usd <> 0
    ORDER BY
      block_time DESC
    LIMIT
      1
  )
SELECT
  ts.total_supply AS "total_supply",
  unclaimed.unclaimed AS "unclaimed_tokens",
  ts.token_address AS "address",
  price_query.price AS "price",
  (ts.total_supply * price_query.price) AS "fdv"
FROM
  ts
  LEFT OUTER JOIN unclaimed ON unclaimed.token_address = ts.token_address
  LEFT OUTER JOIN price_query ON price_query.token_bought_address = ts.token_address;