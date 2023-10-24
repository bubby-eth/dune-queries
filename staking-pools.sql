WITH
  capacity_query AS (
    SELECT
      tr."evt_block_time",
      CAST(tr."value" AS INT256) / 1e18 AS balance,
      CASE
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc2862b8cb21caedb8706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x68e8e004d1a8340008cbacc904502d9dc6823a049c1a9c13f018223119f6a702%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x8f6c52757ad3cf0b6ccb1436de2c87e00cbe2b9d511473fe7202035ad6931e51%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x5091e0621e2a3bc9c25f0673ddcbc039a568e60065dac1fc788acdf9325067eb%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x8f60a13f6dc893b6574764d86cd0464643ac0f051ab3366fe0e2a80bb7605274%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x6f8bc431fe6cffe520b6de8b28453b902d9c721b4b7659c60dd1da0406f82d30%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%xc05318703b8952e765e3e6c9a3485168d9187121b5947dc4499c21924065c679%' THEN 'SPEDN'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x7aae115e9e233f66db16442f643e526ff0a8b97a1dc73d49643c9ba12f4038f4%' THEN 'SPEDN'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffccccccccd6bbe6bf0c2471ba706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'SPEDN'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc7a0208a97d8ac263706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Lightning'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc01ef57163c6d4d34706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Polygon'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc9e74da6b6cf54fc8706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Bitcoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc5504c217e344479d706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Tezos'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffccccccccac3507679b14453a706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Celo'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc186b8f18e6ee4b42706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Litecoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc327b7f0c08214931706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Ethereum'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc3110fc85041345c2706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Bitcoin Cash'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc17e127f74077433c706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Cardano'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc81dbde757c384900706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Dogecoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc22315143f82c475f706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Solana'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc5eb25f6982e04b21706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Zcash'
        ELSE 'OTHER'
      END AS "pool",
      tx."data",
      tx."to",
      tx."from",
      tr."evt_tx_hash",
      tr."contract_address"
    FROM
      erc20_ethereum.evt_Transfer AS tr
      INNER JOIN ethereum."transactions" AS tx ON tr."evt_tx_hash" = tx."hash"
    WHERE
      tr."to" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      AND tr."from" <> 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      AND tr."contract_address" = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    UNION ALL
    SELECT
      tr."evt_block_time",
      - CAST(tr."value" AS INT256) / 1e18 AS balance,
      CASE
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc2862b8cb21caedb8706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Gemini'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc2862b8cb21caedb8706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x2a2418539d181226b25ef31dbe70f289c5bc4cd33a647b1fd88693306fe0ea8b%' THEN 'Gemini'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dccccccccd6bbe6bf0c2471ba706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'SPEDN'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dccccccccd6bbe6bf0c2471ba706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'SPEDN'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc7a0208a97d8ac263706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Lightning'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc7a0208a97d8ac263706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Lightning'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc01ef57163c6d4d34706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Polygon'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc01ef57163c6d4d34706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Polygon'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc9e74da6b6cf54fc8706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Bitcoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc9e74da6b6cf54fc8706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Bitcoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc5504c217e344479d706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Tezos'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc5504c217e344479d706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Tezos'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dccccccccac3507679b14453a706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Celo'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dccccccccac3507679b14453a706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Celo'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc186b8f18e6ee4b42706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Litecoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc186b8f18e6ee4b42706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Litecoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc327b7f0c08214931706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Ethereum'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc327b7f0c08214931706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Ethereum'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc3110fc85041345c2706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Bitcoin Cash'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc3110fc85041345c2706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Bitcoin Cash'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc17e127f74077433c706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Cardano'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc17e127f74077433c706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Cardano'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc81dbde757c384900706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Dogecoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc81dbde757c384900706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Dogecoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc22315143f82c475f706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Solana'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc22315143f82c475f706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Solana'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc5eb25f6982e04b21706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Zcash'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc5eb25f6982e04b21706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Zcash'
        ELSE 'OTHER'
      END AS "pool",
      tx."data",
      tx."to",
      tx."from",
      tr."evt_tx_hash",
      tr."contract_address"
    FROM
      erc20_ethereum.evt_Transfer AS tr
      INNER JOIN ethereum."transactions" AS tx ON tr."evt_tx_hash" = tx."hash"
    WHERE
      tr."from" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      AND tr."to" <> 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      AND tr."contract_address" = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    UNION ALL
    SELECT
      tr."evt_block_time",
      CAST(tr."value" AS INT256) / 1e18 AS balance,
      CASE
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc2862b8cb21caedb8706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x68e8e004d1a8340008cbacc904502d9dc6823a049c1a9c13f018223119f6a702%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x8f6c52757ad3cf0b6ccb1436de2c87e00cbe2b9d511473fe7202035ad6931e51%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x5091e0621e2a3bc9c25f0673ddcbc039a568e60065dac1fc788acdf9325067eb%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x8f60a13f6dc893b6574764d86cd0464643ac0f051ab3366fe0e2a80bb7605274%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x6f8bc431fe6cffe520b6de8b28453b902d9c721b4b7659c60dd1da0406f82d30%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%xc05318703b8952e765e3e6c9a3485168d9187121b5947dc4499c21924065c679%' THEN 'SPEDN'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x7aae115e9e233f66db16442f643e526ff0a8b97a1dc73d49643c9ba12f4038f4%' THEN 'SPEDN'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffccccccccd6bbe6bf0c2471ba706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'SPEDN'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc7a0208a97d8ac263706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Lightning'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc01ef57163c6d4d34706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Polygon'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc9e74da6b6cf54fc8706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Bitcoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc5504c217e344479d706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Tezos'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffccccccccac3507679b14453a706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Celo'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc186b8f18e6ee4b42706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Litecoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc327b7f0c08214931706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Ethereum'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc3110fc85041345c2706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Bitcoin Cash'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc17e127f74077433c706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Cardano'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc81dbde757c384900706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Dogecoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc22315143f82c475f706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Solana'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccc5eb25f6982e04b21706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Zcash'
        ELSE 'OTHER'
      END AS "pool",
      tx."data",
      tx."to",
      tx."from",
      tr."evt_tx_hash",
      tr."contract_address"
    FROM
      erc20_ethereum.evt_Transfer AS tr
      INNER JOIN ethereum."transactions" AS tx ON tr."evt_tx_hash" = tx."hash"
    WHERE
      tr."to" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      AND tr."from" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      AND tr."contract_address" = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
    UNION ALL
    SELECT
      tr."evt_block_time",
      - CAST(tr."value" AS INT256) / 1e18 AS balance,
      CASE
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc2862b8cb21caedb8706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Gemini'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc2862b8cb21caedb8706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Gemini'
        WHEN TRY_CAST (tr."evt_tx_hash" AS VARCHAR) LIKE '%x2a2418539d181226b25ef31dbe70f289c5bc4cd33a647b1fd88693306fe0ea8b%' THEN 'Gemini'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dccccccccd6bbe6bf0c2471ba706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'SPEDN'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dccccccccd6bbe6bf0c2471ba706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'SPEDN'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc7a0208a97d8ac263706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Lightning'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc7a0208a97d8ac263706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Lightning'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc01ef57163c6d4d34706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Polygon'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc01ef57163c6d4d34706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Polygon'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc9e74da6b6cf54fc8706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Bitcoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc9e74da6b6cf54fc8706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Bitcoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc5504c217e344479d706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Tezos'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc5504c217e344479d706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Tezos'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dccccccccac3507679b14453a706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Celo'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dccccccccac3507679b14453a706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Celo'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc186b8f18e6ee4b42706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Litecoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc186b8f18e6ee4b42706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Litecoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc327b7f0c08214931706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Ethereum'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc327b7f0c08214931706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Ethereum'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc3110fc85041345c2706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Bitcoin Cash'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc3110fc85041345c2706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Bitcoin Cash'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc17e127f74077433c706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Cardano'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc17e127f74077433c706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Cardano'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc81dbde757c384900706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Dogecoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc81dbde757c384900706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Dogecoin'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc22315143f82c475f706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Solana'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc22315143f82c475f706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Solana'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%x2036a94dcccccccc5eb25f6982e04b21706d7f8b3445d8dfc790c524e3990ef014e7c578%' THEN 'Zcash'
        WHEN TRY_CAST (tx."data" AS VARCHAR) LIKE '%2036a94dcccccccc5eb25f6982e04b21706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000706d7f8b3445d8dfc790c524e3990ef014e7c578000000000000000000000000%' THEN 'Zcash'
        ELSE 'OTHER'
      END AS "pool",
      tx."data",
      tx."to",
      tx."from",
      tr."evt_tx_hash",
      tr."contract_address"
    FROM
      erc20_ethereum.evt_Transfer AS tr
      INNER JOIN ethereum."transactions" AS tx ON tr."evt_tx_hash" = tx."hash"
    WHERE
      tr."from" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      AND tr."to" = 0x706d7f8b3445d8dfc790c524e3990ef014e7c578
      AND tr."contract_address" = 0xff20817765cb7f73d4bde2e66e067e58d11095c2
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
  SUM(cq."balance") * MAX(price_query."price") AS value
FROM
  capacity_query AS cq
  LEFT OUTER JOIN price_query ON price_query.contract_address = cq.contract_address
WHERE
  "pool" <> 'OTHER'
GROUP BY
  1
ORDER BY
  total DESC