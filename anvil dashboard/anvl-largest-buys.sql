SELECT
    block_time,
    CONCAT(
        '<a href="https://etherscan.io/address/', 
        CAST(tx_from AS VARCHAR), 
        '" target="_blank">', 
        CONCAT(
            SUBSTRING(CAST(tx_from AS VARCHAR) FROM 1 FOR 5),
            '...',
            SUBSTRING(CAST(tx_from AS VARCHAR) FROM 39 FOR 4)
        ),
        '</a>'
    ) AS tx_from,
    token_bought_amount,
    amount_usd,
    amount_usd / token_bought_amount AS price,
    CONCAT(
        '<a href="https://etherscan.io/tx/', 
        CAST(tx_hash AS VARCHAR), 
        '" target="_blank">', 
        CONCAT(
            SUBSTRING(CAST(tx_hash AS VARCHAR) FROM 1 FOR 5),
            '...',
            SUBSTRING(CAST(tx_hash AS VARCHAR) FROM 63 FOR 4)
        ), 
        '</a>'
    ) AS tx_hash,
    DATE_DIFF('day', block_time, CURRENT_TIMESTAMP) AS days_ago
FROM
    dex.trades
WHERE
    block_time > CURRENT_TIMESTAMP - INTERVAL '7' DAY
    AND token_bought_address = 0xAEEAa594e7dc112D67b8547fe9767a02c15B5597
ORDER BY
    token_sold_amount DESC
LIMIT 10;