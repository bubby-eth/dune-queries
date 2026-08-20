-- Amp DEX Liquidity
-- Dune query 2813063: https://dune.com/queries/2813063
-- From dashboard: https://dune.com/ampdotxyz/amp-token

WITH
  balance AS (
    SELECT
      pair_address,
      token_address,
      SUM(amount) AS amount
    FROM
      (
        SELECT
          CASE
            WHEN "to" = 0x75c80ce8fddfc61641bed16cd90c9123f0d9a020 THEN 'Uniswap V3'
            WHEN "to" = 0x4e57f830b0b4a82321071ead6ffd1df1575a16e2 THEN 'Uniswap V3'
            WHEN "to" = 0x68cfee5c451befdf760909a1f3721e3db9af4910 THEN 'Uniswap V3'
            WHEN "to" = 0x0221d724c1a37b8c54dd99fefddae2b903d193d6 THEN 'Uniswap V3'
            WHEN "to" = 0x074a35d73a5008ad2786b15c11279438e05a1db6 THEN 'Uniswap V3'
            WHEN "to" = 0x9d559eccb96ee61ab9a7c8c3b5b6387070dfa219 THEN 'Uniswap V3'
            WHEN "to" = 0x727ca1dcd2f5f76af9f4bbf805d041ff4e128167 THEN 'Uniswap V3'
            WHEN "to" = 0x1ad6fbe0cbe1ecdc5ca8c349773e65dade2a5acb THEN 'Uniswap V3'
            WHEN "to" = 0x08650bb9dc722c9c8c62e79c2bafa2d3fc5b3293 THEN 'Uniswap V2'
            WHEN "to" = 0xab400c46c830a2f87939dcfdcbfaaadf76f35721 THEN 'Uniswap V2'
            WHEN "to" = 0xcc72d6e3d26992c41349a8b49fbd12ef6d9a93fe THEN 'Uniswap V2'
            WHEN "to" = 0x15e86e6f65ef7ea1dbb72a5e51a07926fb1c82e3 THEN 'Sushi'
            WHEN "to" = 0xa17661e7f2cd96633c25edc5455eb29170105ff0 THEN 'Sushi'
            WHEN "to" = 0x0f4a8a06c22ba49e98d15223a701062c40873f7a THEN 'Bancor'
            WHEN "to" = 0x6a74941c1cf4151b3f15cdd84ee3abde713a999b THEN 'Bancor'
            WHEN "to" = 0x2f9ec37d6ccfff1cab21733bdadede11c823ccb0 THEN 'Bancor'
          END AS pair_address,
          tr.contract_address AS token_address,
          SUM(
            CAST(value AS DOUBLE) / POW(10, coalesce(tk.decimals, 18))
          ) AS amount
        FROM
          erc20_ethereum.evt_Transfer tr
          LEFT JOIN tokens.erc20 tk ON tk.contract_address = tr.contract_address
          AND tk.blockchain = 'ethereum'
        WHERE
          (
            "to" = 0x75c80ce8fddfc61641bed16cd90c9123f0d9a020 --USDC-AMP 1% V3
            OR "to" = 0x4e57f830b0b4a82321071ead6ffd1df1575a16e2 --ETH-AMP 1% UNI V3
            OR "to" = 0x68cfee5c451befdf760909a1f3721e3db9af4910 --ETH-AMP .3% UNI V3
            OR "to" = 0x0221d724c1a37b8c54dd99fefddae2b903d193d6 --DAI-AMP 1% UNI V3
            OR "to" = 0x074a35d73a5008ad2786b15c11279438e05a1db6 --BDSCI-AMP .3% UNI V3
            OR "to" = 0x9d559eccb96ee61ab9a7c8c3b5b6387070dfa219 --IDH-AMP .3% UNI V3
            OR "to" = 0x727ca1dcd2f5f76af9f4bbf805d041ff4e128167 --USDT-AMP 1% UNI V3
            OR "to" = 0x1ad6fbe0cbe1ecdc5ca8c349773e65dade2a5acb --USDC-AMP .3% V3
            OR "to" = 0x08650bb9dc722c9c8c62e79c2bafa2d3fc5b3293 --ETH-AMP UNI V2
            OR "to" = 0xab400c46c830a2f87939dcfdcbfaaadf76f35721 --AMP-FRAX UNI V2
            OR "to" = 0xcc72d6e3d26992c41349a8b49fbd12ef6d9a93fe --UNI-AMP UNI V2
            OR "to" = 0x15e86e6f65ef7ea1dbb72a5e51a07926fb1c82e3 --ETH-AMP SUSHI
            OR "to" = 0xa17661e7f2cd96633c25edc5455eb29170105ff0 --DAI-AMP SUSHI
            OR "to" = 0x0f4a8a06c22ba49e98d15223a701062c40873f7a --Bancor
            OR "to" = 0x6a74941c1cf4151b3f15cdd84ee3abde713a999b --Bancor
            OR "to" = 0x2f9ec37d6ccfff1cab21733bdadede11c823ccb0 --Bancor
          )
          AND evt_block_time >= CAST('2020-08-01' AS timestamp)
        GROUP BY
          1,
          2
        union all
        SELECT
          CASE
            WHEN "from" = 0x75c80ce8fddfc61641bed16cd90c9123f0d9a020 THEN 'Uniswap V3'
            WHEN "from" = 0x4e57f830b0b4a82321071ead6ffd1df1575a16e2 THEN 'Uniswap V3'
            WHEN "from" = 0x68cfee5c451befdf760909a1f3721e3db9af4910 THEN 'Uniswap V3'
            WHEN "from" = 0x0221d724c1a37b8c54dd99fefddae2b903d193d6 THEN 'Uniswap V3'
            WHEN "from" = 0x074a35d73a5008ad2786b15c11279438e05a1db6 THEN 'Uniswap V3'
            WHEN "from" = 0x9d559eccb96ee61ab9a7c8c3b5b6387070dfa219 THEN 'Uniswap V3'
            WHEN "from" = 0x727ca1dcd2f5f76af9f4bbf805d041ff4e128167 THEN 'Uniswap V3'
            WHEN "from" = 0x1ad6fbe0cbe1ecdc5ca8c349773e65dade2a5acb THEN 'Uniswap V3'
            WHEN "from" = 0x08650bb9dc722c9c8c62e79c2bafa2d3fc5b3293 THEN 'Uniswap V2'
            WHEN "from" = 0xab400c46c830a2f87939dcfdcbfaaadf76f35721 THEN 'Uniswap V2'
            WHEN "from" = 0xcc72d6e3d26992c41349a8b49fbd12ef6d9a93fe THEN 'Uniswap V2'
            WHEN "from" = 0x15e86e6f65ef7ea1dbb72a5e51a07926fb1c82e3 THEN 'Sushi'
            WHEN "from" = 0xa17661e7f2cd96633c25edc5455eb29170105ff0 THEN 'Sushi'
            WHEN "from" = 0x0f4a8a06c22ba49e98d15223a701062c40873f7a THEN 'Bancor'
            WHEN "from" = 0x6a74941c1cf4151b3f15cdd84ee3abde713a999b THEN 'Bancor'
            WHEN "from" = 0x2f9ec37d6ccfff1cab21733bdadede11c823ccb0 THEN 'Bancor'
          END AS pair_address,
          tr.contract_address AS token_address,
          SUM(
            CAST(value AS DOUBLE) / POW(10, coalesce(tk.decimals, 18))
          ) * (-1) AS amount
        from
          erc20_ethereum.evt_Transfer tr
          LEFT JOIN tokens.erc20 tk ON tk.contract_address = tr.contract_address
          AND tk.blockchain = 'ethereum'
        WHERE
          (
            "from" = 0x75c80ce8fddfc61641bed16cd90c9123f0d9a020 --USDC-AMP 1% V3
            OR "from" = 0x4e57f830b0b4a82321071ead6ffd1df1575a16e2 --ETH-AMP 1% UNI V3
            OR "from" = 0x68cfee5c451befdf760909a1f3721e3db9af4910 --ETH-AMP .3% UNI V3
            OR "from" = 0x0221d724c1a37b8c54dd99fefddae2b903d193d6 --DAI-AMP 1% UNI V3
            OR "from" = 0x074a35d73a5008ad2786b15c11279438e05a1db6 --BDSCI-AMP .3% UNI V3
            OR "from" = 0x9d559eccb96ee61ab9a7c8c3b5b6387070dfa219 --IDH-AMP .3% UNI V3
            OR "from" = 0x727ca1dcd2f5f76af9f4bbf805d041ff4e128167 --USDT-AMP 1% UNI V3
            OR "from" = 0x1ad6fbe0cbe1ecdc5ca8c349773e65dade2a5acb --USDC-AMP .3% V3
            OR "from" = 0x08650bb9dc722c9c8c62e79c2bafa2d3fc5b3293 --ETH-AMP UNI V2
            OR "from" = 0xab400c46c830a2f87939dcfdcbfaaadf76f35721 --AMP-FRAX UNI V2
            OR "from" = 0xcc72d6e3d26992c41349a8b49fbd12ef6d9a93fe --UNI-AMP UNI V2
            OR "from" = 0x15e86e6f65ef7ea1dbb72a5e51a07926fb1c82e3 --ETH-AMP SUSHI
            OR "from" = 0xa17661e7f2cd96633c25edc5455eb29170105ff0 --DAI-AMP SUSHI
            OR "from" = 0x0f4a8a06c22ba49e98d15223a701062c40873f7a --Bancor
            OR "from" = 0x6a74941c1cf4151b3f15cdd84ee3abde713a999b --Bancor
            OR "from" = 0x2f9ec37d6ccfff1cab21733bdadede11c823ccb0 --Bancor
          )
          AND evt_block_time >= CAST('2020-08-01' AS timestamp)
        GROUP BY
          1,
          2
      ) a
    GROUP BY
      1,
      2
    HAVING
      SUM(amount) > 0
  )
SELECT
  pair_address,
  SUM(amount_usd) AS tvl
from
  (
    SELECT
      pair_address,
      b.token_address,
      b.amount,
      b.amount * px.price AS amount_usd
    from
      balance b
      LEFT JOIN prices.usd_latest px ON px.contract_address = b.token_address
      AND px.blockchain = 'ethereum'
    WHERE
      b.amount * px.price > 1
  ) a
GROUP BY
  1
HAVING
  SUM(amount_usd) > 1
ORDER BY
  1
