SELECT
    block_time,
    CONCAT(
        '<a href="https://etherscan.io/address/', 
        CAST(tx_from AS VARCHAR), 
        '" target="_blank">', 
        CONCAT(
            SUBSTRING(CAST(tx_from AS VARCHAR) FROM 1 FOR 5),
            '...',
            SUBSTRING(CAST(tx_from AS VARCHAR) FROM 39 FOR 4)),
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
            SUBSTRING(CAST(tx_hash AS VARCHAR) FROM 63 FOR 4)), 
        '</a>'
    ) AS tx_hash
FROM
    dex.trades
WHERE
    block_time > CURRENT_TIMESTAMP - INTERVAL '7' DAY
    AND token_bought_address = 0x2Ca9242c1810029Efed539F1c60D68B63AD01BFc
ORDER BY
    token_sold_amount DESC
LIMIT 10;