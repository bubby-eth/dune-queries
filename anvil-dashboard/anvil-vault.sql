-- Anvil Vault
-- Dune query 4668282: https://dune.com/queries/4668282
-- From dashboard: https://dune.com/anvil/anvil
WITH
    token_transfers AS (
        SELECT
            "to" AS holder,
            contract_address AS token_address,
            CAST("value" AS INT256) AS balanceChange
        FROM
            erc20_ethereum.evt_Transfer
        WHERE
            "to" <> 0x0000000000000000000000000000000000000000
            AND contract_address IN (
                0xdAC17F958D2ee523a2206206994597C13D831ec7, -- USDT (1e6)
                0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48, -- USDC (1e6)
                0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2, -- WETH (1e18)
                0xfF20817765cB7f73d4bde2e66e067E58D11095C2, -- AMP (1e18)
                0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9, -- AAVE (1e18)
                0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf, -- cbBTC (1e8)
                0xBe9895146f7AF43049ca1c1AE358B0541Ea49704, -- cbETH (1e18)
                0xc00e94cb662c3520282e6f5717214004a7f26888, -- COMP (1e18)
                0x514910771af9ca656af840dff83e8264ecf986ca, -- LINK (1e18)
                0x9D39A5DE30e57443BfF2A8307A4256c8797A3497, -- sUSDe (1e18)
                0x1f9840a85d5af5bf1d1762f925bdaddc4201f984, -- UNI (1e18)
                0xdC035D45d973E3EC169d2276DDab16f1e407384F, -- USDS (1e18)
                0x2260fac5e5542a773aa44fbcfedf7c193bc2c599, -- WBTC (1e8)
                0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0 -- wstETH (1e18)
            )
        UNION ALL
        SELECT
            "from" AS holder,
            contract_address AS token_address,
            - CAST("value" AS INT256) AS balanceChange
        FROM
            erc20_ethereum.evt_Transfer
        WHERE
            "from" <> 0x0000000000000000000000000000000000000000
            AND contract_address IN (
                0xdAC17F958D2ee523a2206206994597C13D831ec7,
                0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,
                0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,
                0xfF20817765cB7f73d4bde2e66e067E58D11095C2,
                0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9,
                0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf,
                0xBe9895146f7AF43049ca1c1AE358B0541Ea49704,
                0xc00e94cb662c3520282e6f5717214004a7f26888,
                0x514910771af9ca656af840dff83e8264ecf986ca,
                0x9D39A5DE30e57443BfF2A8307A4256c8797A3497,
                0x1f9840a85d5af5bf1d1762f925bdaddc4201f984,
                0xdC035D45d973E3EC169d2276DDab16f1e407384F,
                0x2260fac5e5542a773aa44fbcfedf7c193bc2c599,
                0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0
            )
    ),
    staked AS (
        SELECT
            token_address,
            SUM(balanceChange) AS raw_balance
        FROM
            token_transfers
        WHERE
            holder = 0x5d2725fdE4d7Aa3388DA4519ac0449Cc031d675f
        GROUP BY
            token_address
    ),
    scaled AS (
        SELECT
            token_address,
            CASE token_address
                WHEN 0xdAC17F958D2ee523a2206206994597C13D831ec7 THEN raw_balance / 1e6
                WHEN 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 THEN raw_balance / 1e6
                WHEN 0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf THEN raw_balance / 1e8
                WHEN 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599 THEN raw_balance / 1e8
                ELSE raw_balance / 1e18
            END AS staked_tokens,
            CASE token_address
                WHEN 0xdAC17F958D2ee523a2206206994597C13D831ec7 THEN 'USDT'
                WHEN 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 THEN 'USDC'
                WHEN 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 THEN 'WETH'
                WHEN 0xfF20817765cB7f73d4bde2e66e067E58D11095C2 THEN 'AMP'
                WHEN 0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9 THEN 'AAVE'
                WHEN 0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf THEN 'cbBTC'
                WHEN 0xBe9895146f7AF43049ca1c1AE358B0541Ea49704 THEN 'cbETH'
                WHEN 0xc00e94cb662c3520282e6f5717214004a7f26888 THEN 'COMP'
                WHEN 0x514910771af9ca656af840dff83e8264ecf986ca THEN 'LINK'
                WHEN 0x9D39A5DE30e57443BfF2A8307A4256c8797A3497 THEN 'sUSDe'
                WHEN 0x1f9840a85d5af5bf1d1762f925bdaddc4201f984 THEN 'UNI'
                WHEN 0xdC035D45d973E3EC169d2276DDab16f1e407384F THEN 'USDS'
                WHEN 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599 THEN 'WBTC'
                WHEN 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0 THEN 'wstETH'
                ELSE 'Unknown'
            END AS ticker
        FROM
            staked
    )
SELECT
    s.ticker AS asset,
    s.staked_tokens AS quantity,
    s.staked_tokens * COALESCE(p.price, 0) AS amount
FROM
    scaled s
    LEFT JOIN LATERAL (
        SELECT
            p.price
        FROM
            prices.usd p
        WHERE
            p.blockchain = 'ethereum'
            AND p.contract_address = s.token_address
        ORDER BY
            p.minute DESC NULLS FIRST
        LIMIT
            1
    ) p ON TRUE
ORDER BY
    amount DESC
