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
  v3_staked AS (
    SELECT
      SUM(token_amount) AS staked_tokens
    FROM (
      -- stake (positive)
      SELECT
        varbinary_to_uint256(varbinary_substring(data, 37, 32)) / 1e18 AS token_amount
      FROM
        ethereum.transactions
      WHERE
        "to" IN (
          0xd0415cf4558A0dBEE8242498D25284476bE3c8f2,
          0xA52125ced25602203BCeF6E78E865571306CaB2A,
          0xD57E335457b6f5d09ac69248230005a02F9B60CF,
          0xdB07414039F5e1618E3eCC8019C1C1ecb4b4C06A,
          0xE932d1a226E962D820a33363DF32FcC95D2559D2,
          0x9477dA44A61ceBCDD0383CD00Bf18A859FEb75b0,
          0xFF1D02F09A9C55cEFd37f57715FEe7E88278d34e,
          0x59e772F12938063bCa8A2B978791eBe225f5Bc3c,
          0xd80370093a305bbDA27B821bb6c6347989Bf709b,
          0x84706656fabFE15b2b77F292A656dD024607d332,
          0xa7f2B6aF8c536897f246B1EB62654cb9c886FA47,
          0x80E58Fe28F53CCbaD1c295ebAA6A8c13241D034b,
          0x1e73f41454D9806f0462Eb6C9FD2A3754cEE7Fc4,
          0xc163c2cC35e32350Aa92DEC2b53b68950942d72F,
          0x57F6f249DB02083362D43E2D02dD791068Df30C6,
          0xcfBbAE9DCE9a207BaB01E1589e345D3Edc65D842,
          0xCD234A11B26F42B391C2838Beb3DA3Bb3A590B66,
          0xB8706F2dd1Ce8A4328D254cF14271e0fbB5E268A,
          0x1693DeCE45b908Ed25244E8b7FFdE4760cB9Ca24
        )
        AND block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND varbinary_substring(data, 1, 4) = 0x3e12170f
      UNION ALL
      -- stakeReleasableTokensFrom (positive)
      SELECT
        varbinary_to_uint256(varbinary_substring(data, 69, 32)) / 1e18 AS token_amount
      FROM
        ethereum.transactions
      WHERE
        "to" IN (
          -- same list of addresses
          0xd0415cf4558A0dBEE8242498D25284476bE3c8f2,
          0xA52125ced25602203BCeF6E78E865571306CaB2A,
          0xD57E335457b6f5d09ac69248230005a02F9B60CF,
          0xdB07414039F5e1618E3eCC8019C1C1ecb4b4C06A,
          0xE932d1a226E962D820a33363DF32FcC95D2559D2,
          0x9477dA44A61ceBCDD0383CD00Bf18A859FEb75b0,
          0xFF1D02F09A9C55cEFd37f57715FEe7E88278d34e,
          0x59e772F12938063bCa8A2B978791eBe225f5Bc3c,
          0xd80370093a305bbDA27B821bb6c6347989Bf709b,
          0x84706656fabFE15b2b77F292A656dD024607d332,
          0xa7f2B6aF8c536897f246B1EB62654cb9c886FA47,
          0x80E58Fe28F53CCbaD1c295ebAA6A8c13241D034b,
          0x1e73f41454D9806f0462Eb6C9FD2A3754cEE7Fc4,
          0xc163c2cC35e32350Aa92DEC2b53b68950942d72F,
          0x57F6f249DB02083362D43E2D02dD791068Df30C6,
          0xcfBbAE9DCE9a207BaB01E1589e345D3Edc65D842,
          0xCD234A11B26F42B391C2838Beb3DA3Bb3A590B66,
          0xB8706F2dd1Ce8A4328D254cF14271e0fbB5E268A,
          0x1693DeCE45b908Ed25244E8b7FFdE4760cB9Ca24
        )
        AND block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND varbinary_substring(data, 1, 4) = 0x34048584
      UNION ALL
      -- unstake (negative)
      SELECT
        -varbinary_to_uint256(varbinary_substring(data, 37, 32)) / 1e18 AS token_amount
      FROM
        ethereum.transactions
      WHERE
        "to" IN (
          -- same list again
          0xd0415cf4558A0dBEE8242498D25284476bE3c8f2,
          0xA52125ced25602203BCeF6E78E865571306CaB2A,
          0xD57E335457b6f5d09ac69248230005a02F9B60CF,
          0xdB07414039F5e1618E3eCC8019C1C1ecb4b4C06A,
          0xE932d1a226E962D820a33363DF32FcC95D2559D2,
          0x9477dA44A61ceBCDD0383CD00Bf18A859FEb75b0,
          0xFF1D02F09A9C55cEFd37f57715FEe7E88278d34e,
          0x59e772F12938063bCa8A2B978791eBe225f5Bc3c,
          0xd80370093a305bbDA27B821bb6c6347989Bf709b,
          0x84706656fabFE15b2b77F292A656dD024607d332,
          0xa7f2B6aF8c536897f246B1EB62654cb9c886FA47,
          0x80E58Fe28F53CCbaD1c295ebAA6A8c13241D034b,
          0x1e73f41454D9806f0462Eb6C9FD2A3754cEE7Fc4,
          0xc163c2cC35e32350Aa92DEC2b53b68950942d72F,
          0x57F6f249DB02083362D43E2D02dD791068Df30C6,
          0xcfBbAE9DCE9a207BaB01E1589e345D3Edc65D842,
          0xCD234A11B26F42B391C2838Beb3DA3Bb3A590B66,
          0xB8706F2dd1Ce8A4328D254cF14271e0fbB5E268A,
          0x1693DeCE45b908Ed25244E8b7FFdE4760cB9Ca24
        )
        AND block_time > TRY_CAST('2024-11-01' AS TIMESTAMP)
        AND varbinary_substring(data, 1, 4) = 0xc2a672e0
    ) AS combined
  ),
  all_staked AS (
    SELECT
      ts.token_address,
      COALESCE(staked.staked_tokens, 0) + COALESCE(v3_staked.staked_tokens, 0) AS staked_tokens,
      COALESCE(staked.staked_tokens, 0) AS v2_staked
    FROM ts
    LEFT JOIN staked ON staked.token_address = ts.token_address
    LEFT JOIN v3_staked ON TRUE
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
  (all_staked.v2_staked * price_query.price) / 1e6 AS "Capacity TVL",
  (cs.circulating_supply * price_query.price) / 1e6 AS "Market Cap",
  (ts.total_supply * price_query.price) / 1e6 AS "FDV",
  cs.circulating_supply AS "full_circulating_supply",
  ts.total_supply AS "full_total_supply"
FROM
  ts
  INNER JOIN cs ON cs.token_address = ts.token_address
  LEFT JOIN all_staked ON all_staked.token_address = ts.token_address
  LEFT JOIN price_query ON price_query.contract_address = ts.token_address