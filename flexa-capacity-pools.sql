WITH
    capacity_query AS (
        SELECT
            call_block_time,
            _from,
            _to,
            CAST(_value AS INT256) / 1e18 AS balance,
            _partition,
            call_tx_hash,
            output_0,
            _data,
            contract_address,
            CASE
                WHEN output_0 = 0xcccccccc2862b8cb21caedb8706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Gemini'
                WHEN output_0 = 0xccccccccd6bbe6bf0c2471ba706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'SPEDN'
                WHEN output_0 = 0xcccccccc7a0208a97d8ac263706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Lightning'
                WHEN output_0 = 0xcccccccc01ef57163c6d4d34706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Polygon'
                WHEN output_0 = 0xcccccccc9e74da6b6cf54fc8706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Bitcoin'
                WHEN output_0 = 0xcccccccc5504c217e344479d706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Tezos'
                WHEN output_0 = 0xccccccccac3507679b14453a706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Celo'
                WHEN output_0 = 0xcccccccc186b8f18e6ee4b42706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Litecoin'
                WHEN output_0 = 0xcccccccc327b7f0c08214931706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Ethereum'
                WHEN output_0 = 0xcccccccc3110fc85041345c2706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Bitcoin Cash'
                WHEN output_0 = 0xcccccccc17e127f74077433c706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Cardano'
                WHEN output_0 = 0xcccccccc81dbde757c384900706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Dogecoin'
                WHEN output_0 = 0xcccccccc22315143f82c475f706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Solana'
                WHEN output_0 = 0xcccccccc5eb25f6982e04b21706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Zcash'
            END AS "pool"
        FROM
            amp_ethereum.Amp_call_transferByPartition
        WHERE
            call_success = true
            AND _to = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578 /*staking to capacity from wallet*/
            AND _from <> 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
            AND call_block_time >= CURRENT_TIMESTAMP - INTERVAL '30' day
        UNION ALL
        SELECT
            call_block_time,
            _from,
            _to,
            - (CAST(_value AS INT256) / 1e18) AS balance,
            _partition,
            call_tx_hash,
            output_0,
            _data,
            contract_address,
            CASE
                WHEN _partition = 0xcccccccc2862b8cb21caedb8706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Gemini'
                WHEN _partition = 0xccccccccd6bbe6bf0c2471ba706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'SPEDN'
                WHEN _partition = 0xcccccccc7a0208a97d8ac263706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Lightning'
                WHEN _partition = 0xcccccccc01ef57163c6d4d34706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Polygon'
                WHEN _partition = 0xcccccccc9e74da6b6cf54fc8706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Bitcoin'
                WHEN _partition = 0xcccccccc5504c217e344479d706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Tezos'
                WHEN _partition = 0xccccccccac3507679b14453a706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Celo'
                WHEN _partition = 0xcccccccc186b8f18e6ee4b42706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Litecoin'
                WHEN _partition = 0xcccccccc327b7f0c08214931706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Ethereum'
                WHEN _partition = 0xcccccccc3110fc85041345c2706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Bitcoin Cash'
                WHEN _partition = 0xcccccccc17e127f74077433c706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Cardano'
                WHEN _partition = 0xcccccccc81dbde757c384900706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Dogecoin'
                WHEN _partition = 0xcccccccc22315143f82c475f706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Solana'
                WHEN _partition = 0xcccccccc5eb25f6982e04b21706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Zcash'
            END AS "pool"
        FROM
            amp_ethereum.Amp_call_transferByPartition
        WHERE
            call_success = true
            AND _from = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
            AND _to <> 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
            AND call_block_time >= CURRENT_TIMESTAMP - INTERVAL '30' day
        UNION ALL
        SELECT
            call_block_time,
            _from,
            _to,
            CAST(_value AS INT256) / 1e18 AS balance,
            _partition,
            call_tx_hash,
            output_0,
            _data,
            contract_address,
            CASE
                WHEN output_0 = 0xcccccccc2862b8cb21caedb8706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Gemini'
                WHEN output_0 = 0xccccccccd6bbe6bf0c2471ba706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'SPEDN'
                WHEN output_0 = 0xcccccccc7a0208a97d8ac263706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Lightning'
                WHEN output_0 = 0xcccccccc01ef57163c6d4d34706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Polygon'
                WHEN output_0 = 0xcccccccc9e74da6b6cf54fc8706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Bitcoin'
                WHEN output_0 = 0xcccccccc5504c217e344479d706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Tezos'
                WHEN output_0 = 0xccccccccac3507679b14453a706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Celo'
                WHEN output_0 = 0xcccccccc186b8f18e6ee4b42706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Litecoin'
                WHEN output_0 = 0xcccccccc327b7f0c08214931706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Ethereum'
                WHEN output_0 = 0xcccccccc3110fc85041345c2706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Bitcoin Cash'
                WHEN output_0 = 0xcccccccc17e127f74077433c706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Cardano'
                WHEN output_0 = 0xcccccccc81dbde757c384900706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Dogecoin'
                WHEN output_0 = 0xcccccccc22315143f82c475f706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Solana'
                WHEN output_0 = 0xcccccccc5eb25f6982e04b21706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Zcash'
            END AS "pool"
        FROM
            amp_ethereum.Amp_call_transferByPartition
        WHERE
            call_success = true
            AND _to = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
            AND _from = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
            AND call_block_time >= CURRENT_TIMESTAMP - INTERVAL '30' day
        UNION ALL
        SELECT
            call_block_time,
            _from,
            _to,
            - (CAST(_value AS INT256) / 1e18) AS balance,
            _partition,
            call_tx_hash,
            output_0,
            _data,
            contract_address,
            CASE
                WHEN _partition = 0xcccccccc2862b8cb21caedb8706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Gemini'
                WHEN _partition = 0xccccccccd6bbe6bf0c2471ba706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'SPEDN'
                WHEN _partition = 0xcccccccc7a0208a97d8ac263706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Lightning'
                WHEN _partition = 0xcccccccc01ef57163c6d4d34706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Polygon'
                WHEN _partition = 0xcccccccc9e74da6b6cf54fc8706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Bitcoin'
                WHEN _partition = 0xcccccccc5504c217e344479d706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Tezos'
                WHEN _partition = 0xccccccccac3507679b14453a706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Celo'
                WHEN _partition = 0xcccccccc186b8f18e6ee4b42706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Litecoin'
                WHEN _partition = 0xcccccccc327b7f0c08214931706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Ethereum'
                WHEN _partition = 0xcccccccc3110fc85041345c2706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Bitcoin Cash'
                WHEN _partition = 0xcccccccc17e127f74077433c706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Cardano'
                WHEN _partition = 0xcccccccc81dbde757c384900706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Dogecoin'
                WHEN _partition = 0xcccccccc22315143f82c475f706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Solana'
                WHEN _partition = 0xcccccccc5eb25f6982e04b21706d7f8b3445d8dfc790c524e3990ef014e7c578 THEN 'Zcash'
            END AS "pool"
        FROM
            amp_ethereum.Amp_call_transferByPartition
        WHERE
            call_success = true
            AND _from = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
            AND _to = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
            AND call_block_time >= CURRENT_TIMESTAMP - INTERVAL '30' day
    ),
    price_query AS (
        SELECT
            *
        FROM
            prices.usd
        WHERE
            blockchain = 'ethereum'
            AND "contract_address" = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
        ORDER BY
            minute DESC
        LIMIT
            1
    )
SELECT
    "pool",
    SUM(cq."balance") AS total,
    SUM(cq."balance") * MAX(price_query."price") AS value,
    CASE
        WHEN SUM(cq."balance") > 0 THEN '🟢'
        ELSE '🔴'
    END AS event_type,
    CASE
        WHEN SUM(cq."balance") >= 0 THEN SUM(cq."balance")
    END AS total_positive,
    CASE
        WHEN SUM(cq."balance") < 0 THEN SUM(cq."balance")
    END AS total_negative
FROM
    capacity_query AS cq
    LEFT OUTER JOIN price_query ON price_query.contract_address = cq.contract_address
GROUP BY
    1
ORDER BY
    2 DESC