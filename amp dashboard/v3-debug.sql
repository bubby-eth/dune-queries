UNION ALL
--stake (positive) multisig
select
    varbinary_to_uint256(varbinary_substring(substr(data, 357, 352), 37, 32)) / 1e18 as token_amount
from ethereum.transactions
where block_time > TRY_CAST('2024-11-01' as TIMESTAMP)
  and varbinary_substring(substr(data, 357, 352), 1, 4) = 0x3e12170f
  and varbinary_ltrim(substr(data, 5, 32)) in (
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
UNION ALL
  --stakeReleasableTokensFrom (positive) multisig
  select
    varbinary_to_uint256(varbinary_substring(substr(data, 357, 352), 37, 32)) / 1e18 as token_amount
from ethereum.transactions
where block_time > TRY_CAST('2024-11-01' as TIMESTAMP)
  and varbinary_substring(substr(data, 357, 352), 1, 4) = 0x34048584
  and varbinary_ltrim(substr(data, 5, 32)) in (
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
UNION ALL
  -- unstake (negative) multisig
  select
    - varbinary_to_uint256(varbinary_substring(substr(data, 357, 352), 37, 32)) / 1e18 as token_amount
from ethereum.transactions
where block_time > TRY_CAST('2024-11-01' as TIMESTAMP)
  and varbinary_substring(substr(data, 357, 352), 1, 4) = 0xc2a672e0
  and varbinary_ltrim(substr(data, 5, 32)) in (
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




  //more debugs
    select
  hash,
  varbinary_to_uint256(varbinary_substring(substr(data, 357, 352), 37, 32)) / 1e18 as token_amount_1,
  varbinary_to_uint256(varbinary_substring(substr(data, 357, 352), 69, 32)) / 1e18 as token_amount_2, -- stakeReleasableTokensFrom (positive) multisig 
  varbinary_substring(substr(data, 357, 352), 1, 4),
  varbinary_substring(substr(data, 357, 352), 5, 32),
  varbinary_substring(substr(data, 357, 352), 37, 32),
  varbinary_substring(substr(data, 357, 352), 69, 32),
  varbinary_substring(substr(data, 357, 352), 101, 32),
  varbinary_substring(substr(data, 357, 352), 133, 32),
  varbinary_substring(substr(data, 357, 352), 165, 32),
  varbinary_substring(substr(data, 357, 352), 197, 32),
  varbinary_substring(substr(data, 357, 352), 229, 32),
  varbinary_substring(substr(data, 357, 352), 261, 32),
  varbinary_substring(substr(data, 357, 352), 293, 32),
  varbinary_substring(substr(data, 357, 352), 325, 32),

    -- varbinary_ltrim(substr(data, 5, 32)),
    substr(data, 357, 352)
from ethereum.transactions
where block_time > TRY_CAST('2024-11-01' as TIMESTAMP)
  and varbinary_substring(substr(data, 357, 352), 1, 4) in (0x34048584, 0x3e12170f, 0xc2a672e0)
  and varbinary_ltrim(substr(data, 5, 32)) in (
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