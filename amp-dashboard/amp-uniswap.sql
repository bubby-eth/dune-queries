-- Amp Uniswap
-- Dune query 545607: https://dune.com/queries/545607
-- From dashboard: https://dune.com/ampdotxyz/amp-token

WITH
  prices AS (
    SELECT
      DATE_TRUNC('day', minute) AS day,
      contract_address,
      AVG(price) AS price
    FROM
      prices.usd
    WHERE
      blockchain = 'ethereum'
      AND minute >= CURRENT_TIMESTAMP - INTERVAL '30' day
      AND contract_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
    GROUP BY
      1,
      2
    ORDER BY
      1 DESC
  ),
  amp_sold AS (
    SELECT
      DATE_TRUNC('day', block_time) AS date,
      SUM(token_sold_amount * a.price) AS "sold",
      project,
      project_contract_address,
      CASE
        WHEN project_contract_address = 0x75c80ce8fddfc61641bed16cd90c9123f0d9a020 THEN 'USDC-AMP V3'
        WHEN project_contract_address = 0x4e57f830b0b4a82321071ead6ffd1df1575a16e2 THEN 'ETH-AMP V3'
        WHEN project_contract_address = 0x68cfee5c451befdf760909a1f3721e3db9af4910 THEN 'ETH-AMP V3'
        WHEN project_contract_address = 0x0221d724c1a37b8c54dd99fefddae2b903d193d6 THEN 'DAI-AMP V3'
        WHEN project_contract_address = 0x074a35d73a5008ad2786b15c11279438e05a1db6 THEN 'BDSCI-AMP V3'
        WHEN project_contract_address = 0x9d559eccb96ee61ab9a7c8c3b5b6387070dfa219 THEN 'IDH-AMP V3'
        WHEN project_contract_address = 0x727ca1dcd2f5f76af9f4bbf805d041ff4e128167 THEN 'USDT-AMP V3'
        WHEN project_contract_address = 0x1ad6fbe0cbe1ecdc5ca8c349773e65dade2a5acb THEN 'USDC-AMP V3'
        WHEN project_contract_address = 0x08650bb9dc722c9c8c62e79c2bafa2d3fc5b3293 THEN 'ETH-AMP V2'
        WHEN project_contract_address = 0xab400c46c830a2f87939dcfdcbfaaadf76f35721 THEN 'AMP-FRAX V2'
        WHEN project_contract_address = 0xcc72d6e3d26992c41349a8b49fbd12ef6d9a93fe THEN 'UNI-AMP V2'
      END AS pair_address
    FROM
      dex."trades"
      LEFT JOIN prices a ON DATE_TRUNC('day', block_time) = a.day
    WHERE
      blockchain = 'ethereum'
      AND block_time > CURRENT_TIMESTAMP - INTERVAL '30' day
      AND token_sold_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
      AND project = 'uniswap'
      AND (
        project_contract_address = 0x75c80ce8fddfc61641bed16cd90c9123f0d9a020 --USDC-AMP 1% V3
        OR project_contract_address = 0x4e57f830b0b4a82321071ead6ffd1df1575a16e2 --ETH-AMP 1% UNI V3
        OR project_contract_address = 0x68cfee5c451befdf760909a1f3721e3db9af4910 --ETH-AMP .3% UNI V3
        OR project_contract_address = 0x0221d724c1a37b8c54dd99fefddae2b903d193d6 --DAI-AMP 1% UNI V3
        OR project_contract_address = 0x074a35d73a5008ad2786b15c11279438e05a1db6 --BDSCI-AMP .3% UNI V3
        OR project_contract_address = 0x9d559eccb96ee61ab9a7c8c3b5b6387070dfa219 --IDH-AMP .3% UNI V3
        OR project_contract_address = 0x727ca1dcd2f5f76af9f4bbf805d041ff4e128167 --USDT-AMP 1% UNI V3
        OR project_contract_address = 0x1ad6fbe0cbe1ecdc5ca8c349773e65dade2a5acb --USDC-AMP .3% V3
        OR project_contract_address = 0x08650bb9dc722c9c8c62e79c2bafa2d3fc5b3293 --ETH-AMP UNI V2
        OR project_contract_address = 0xab400c46c830a2f87939dcfdcbfaaadf76f35721 --AMP-FRAX UNI V2
        OR project_contract_address = 0xcc72d6e3d26992c41349a8b49fbd12ef6d9a93fe --UNI-AMP UNI V2
      )
    GROUP BY
      1,
      3,
      4,
      5
    ORDER BY
      1 DESC
  ),
  amp_bought AS (
    SELECT
      DATE_TRUNC('day', block_time) AS date,
      SUM(token_bought_amount * a.price) AS "bought",
      project,
      project_contract_address,
      CASE
        WHEN project_contract_address = 0x75c80ce8fddfc61641bed16cd90c9123f0d9a020 THEN 'USDC-AMP V3'
        WHEN project_contract_address = 0x4e57f830b0b4a82321071ead6ffd1df1575a16e2 THEN 'ETH-AMP V3'
        WHEN project_contract_address = 0x68cfee5c451befdf760909a1f3721e3db9af4910 THEN 'ETH-AMP V3'
        WHEN project_contract_address = 0x0221d724c1a37b8c54dd99fefddae2b903d193d6 THEN 'DAI-AMP V3'
        WHEN project_contract_address = 0x074a35d73a5008ad2786b15c11279438e05a1db6 THEN 'BDSCI-AMP V3'
        WHEN project_contract_address = 0x9d559eccb96ee61ab9a7c8c3b5b6387070dfa219 THEN 'IDH-AMP V3'
        WHEN project_contract_address = 0x727ca1dcd2f5f76af9f4bbf805d041ff4e128167 THEN 'USDT-AMP V3'
        WHEN project_contract_address = 0x1ad6fbe0cbe1ecdc5ca8c349773e65dade2a5acb THEN 'USDC-AMP V3'
        WHEN project_contract_address = 0x08650bb9dc722c9c8c62e79c2bafa2d3fc5b3293 THEN 'ETH-AMP V2'
        WHEN project_contract_address = 0xab400c46c830a2f87939dcfdcbfaaadf76f35721 THEN 'AMP-FRAX V2'
        WHEN project_contract_address = 0xcc72d6e3d26992c41349a8b49fbd12ef6d9a93fe THEN 'UNI-AMP V2'
      END AS pair_address
    FROM
      dex."trades"
      LEFT JOIN prices a ON DATE_TRUNC('day', block_time) = a.day
    WHERE
      blockchain = 'ethereum'
      AND block_time > CURRENT_TIMESTAMP - INTERVAL '30' day
      AND token_bought_address = 0xfF20817765cB7f73d4bde2e66e067E58D11095C2
      AND project = 'uniswap'
      AND (
        project_contract_address = 0x75c80ce8fddfc61641bed16cd90c9123f0d9a020 --USDC-AMP 1% V3
        OR project_contract_address = 0x4e57f830b0b4a82321071ead6ffd1df1575a16e2 --ETH-AMP 1% UNI V3
        OR project_contract_address = 0x68cfee5c451befdf760909a1f3721e3db9af4910 --ETH-AMP .3% UNI V3
        OR project_contract_address = 0x0221d724c1a37b8c54dd99fefddae2b903d193d6 --DAI-AMP 1% UNI V3
        OR project_contract_address = 0x074a35d73a5008ad2786b15c11279438e05a1db6 --BDSCI-AMP .3% UNI V3
        OR project_contract_address = 0x9d559eccb96ee61ab9a7c8c3b5b6387070dfa219 --IDH-AMP .3% UNI V3
        OR project_contract_address = 0x727ca1dcd2f5f76af9f4bbf805d041ff4e128167 --USDT-AMP 1% UNI V3
        OR project_contract_address = 0x1ad6fbe0cbe1ecdc5ca8c349773e65dade2a5acb --USDC-AMP .3% V3
        OR project_contract_address = 0x08650bb9dc722c9c8c62e79c2bafa2d3fc5b3293 --ETH-AMP UNI V2
        OR project_contract_address = 0xab400c46c830a2f87939dcfdcbfaaadf76f35721 --AMP-FRAX UNI V2
        OR project_contract_address = 0xcc72d6e3d26992c41349a8b49fbd12ef6d9a93fe --UNI-AMP UNI V2
      )
    GROUP BY
      1,
      3,
      4,
      5
    ORDER BY
      1 DESC
  )
SELECT
  amp_bought.date as date,
  COALESCE(bought, 0) + COALESCE(sold, 0) AS volume,
  CASE
    WHEN amp_bought.pair_address = 'AMP-FRAX V2' THEN (COALESCE(bought, 0) + COALESCE(sold, 0)) * 0.003
    WHEN amp_bought.pair_address = 'ETH-AMP V2' THEN (COALESCE(bought, 0) + COALESCE(sold, 0)) * 0.003
    WHEN amp_bought.pair_address = 'USDC-AMP V3' THEN (COALESCE(bought, 0) + COALESCE(sold, 0)) * 0.01
    WHEN amp_bought.pair_address = 'ETH-AMP V3' THEN (COALESCE(bought, 0) + COALESCE(sold, 0)) * 0.01
  END AS fees,
  amp_bought.pair_address
FROM
  amp_bought
  LEFT JOIN amp_sold ON amp_bought.date = amp_sold.date
  AND amp_bought.project = amp_sold.project
  AND amp_bought.project_contract_address = amp_sold.project_contract_address
ORDER BY
  1 DESC
