WITH
  amp AS (
    SELECT
      "to" AS "holder",
      contract_address AS token_address,
      CAST("value" AS INT256) AS "balanceChange"
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      "to" <> 0x0000000000000000000000000000000000000000
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    UNION ALL
    SELECT
      "from" AS "holder",
      contract_address AS token_address,
      - CAST("value" AS INT256) AS "balanceChange"
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      "from" <> 0x0000000000000000000000000000000000000000
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
  ),
  ts AS (
    SELECT
      token_address,
      SUM(balanceChange) / 1e18 AS total_supply
    FROM
      amp
    GROUP BY
      1
  ),
  cs AS (
    SELECT
      token_address,
      SUM(balanceChange) / 1e18 AS circulating_supply
    FROM
      amp
    WHERE
      NOT holder IN (
        0xbcfc557f0b74250a6c4c671b2e6e4daf6af2b9d0,
        0x0c3a4a4416562ddccfda34e4fe681569fe60c7bd,
        0x780f9a570c1bec9f2dc761b9031c992cb3e2ae6e
      )
    GROUP BY
      1
  ),
  staked AS (
    SELECT
      token_address,
      SUM(balanceChange) / 1e18 AS staked_tokens
    FROM
      amp
    WHERE
      holder IN (
        0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      )
    GROUP BY
      1
  ),
  all_staked AS (
    SELECT
      ts.token_address,
      COALESCE(staked.staked_tokens, 0) AS staked_tokens
    FROM ts
    LEFT JOIN staked ON staked.token_address = ts.token_address
  ),
  price_query AS (
    SELECT
      *
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND contract_address = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    ORDER BY
      minute DESC NULLS FIRST
    LIMIT
      1
  )
SELECT
  ts.total_supply / 1e9 AS "Total Supply",
  cs.circulating_supply / 1e9 AS "Circulating Supply",
  all_staked.staked_tokens / 1e9 AS "Staked Tokens",
  all_staked.staked_tokens / cs.circulating_supply * 100 AS "Staked Ratio",
  ts.token_address AS "AMP Address",
  price_query.price AS "AMP Price",
  (all_staked.staked_tokens * price_query.price) / 1e6 AS "Capacity TVL",
  (cs.circulating_supply * price_query.price) / 1e6 AS "Market Cap",
  (ts.total_supply * price_query.price) / 1e6 AS "FDV",
  cs.circulating_supply AS "full_circulating_supply",
  ts.total_supply AS "full_total_supply"
FROM
  ts
  INNER JOIN cs ON cs.token_address = ts.token_address
  LEFT JOIN all_staked ON all_staked.token_address = ts.token_address
  LEFT JOIN price_query ON price_query.contract_address = ts.token_address