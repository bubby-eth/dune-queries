# Dune decoding submissions for Anvil

Submit at https://dune.com/contracts/new (project name: `anvil`, chain: Ethereum).
Already decoded, no action needed: CollateralVault
(`anvil_ethereum.collateralvault_*`).

Status 2026-08-25: backfill landed for items 1-6 — decoded row counts match
raw logs exactly (LOC lifecycle, TBCP stake/unstake across all 20 pools,
Governor proxy VoteCast/proposals, Timelock). The four raw-log queries
(8423834, 8423835, 8424123, 8424124) have been rewritten onto the decoded
tables. Item 0 (the new ANVL token) is still NOT decoded — `anvil_evt_*` has
0 rows for 0xAEEA...5597 — so queries 8425041/8425043 stay on raw logs;
recheck ~Oct 2026 and resubmit if still absent.

## 0. Current ANVL token — HIGH priority (NEW)

`anvil_ethereum.anvil_evt_*` covers only the PRE-migration token
(0x2ca9242c1810029EFED539F1c60D68B63AD01BFc). ANVL migrated ~Oct 2025 to a new
100B-supply token, and live governance verifiably uses the new one (recent
VoteCast weights match its delegation records). The new token is completely
undecoded — the raw-log parsing in anvl-top-delegates.sql and
anvl-delegation-over-time.sql can be replaced once it decodes.

    0xAEEAa594e7dc112D67b8547fe9767a02c15B5597

- Contract name: Anvil (standard ERC20Votes; ABI verified on Etherscan)

## 1. LetterOfCredit (proxy) — HIGH priority

Unlocks decoded LOC lifecycle events (created/redeemed/canceled/liquidated);
replaces the raw-log parsing in anvil-letters-of-credit.sql and anvil-liquidations.sql.

    0x14db9a91933aD9433E1A0dB04D08e5D9EF7c4808

- Contract name: LetterOfCredit
- Check "this is a proxy" — implementation (ABI source):

    0x24573B112456d3a96c97fB460B436e8CA870e27E

## 2. TimeBasedCollateralPool (beacon proxy instances) — HIGH priority

Unlocks decoded stake/unstake events for all collateral pools; replaces the
raw-log parsing in the AMP dashboard v3 staking queries too. Submit ONE pool
address and check "there are several instances of this contract" so all beacon
proxies decode together.

    0xd0415cf4558A0dBEE8242498D25284476bE3c8f2

- Contract name: TimeBasedCollateralPool
- Check "this is a proxy" — implementation (ABI source):

    0xCc437a7Bb14f07de09B0F4438df007c8F64Cf29f

- Other known instances (for reference / spot checks):

    0xA52125ced25602203BCeF6E78E865571306CaB2A
    0xD57E335457b6f5d09ac69248230005a02F9B60CF
    0xdB07414039F5e1618E3eCC8019C1C1ecb4b4C06A
    0xE932d1a226E962D820a33363DF32FcC95D2559D2
    0x9477dA44A61ceBCDD0383CD00Bf18A859FEb75b0
    0xFF1D02F09A9C55cEFd37f57715FEe7E88278d34e
    0x59e772F12938063bCa8A2B978791eBe225f5Bc3c
    0xd80370093a305bbDA27B821bb6c6347989Bf709b
    0x84706656fabFE15b2b77F292A656dD024607d332
    0xa7f2B6aF8c536897f246B1EB62654cb9c886FA47
    0x80E58Fe28F53CCbaD1c295ebAA6A8c13241D034b
    0x1e73f41454D9806f0462Eb6C9FD2A3754cEE7Fc4
    0xc163c2cC35e32350Aa92DEC2b53b68950942d72F
    0x57F6f249DB02083362D43E2D02dD791068Df30C6
    0xcfBbAE9DCE9a207BaB01E1589e345D3Edc65D842
    0xCD234A11B26F42B391C2838Beb3DA3Bb3A590B66
    0xB8706F2dd1Ce8A4328D254cF14271e0fbB5E268A
    0x1693DeCE45b908Ed25244E8b7FFdE4760cB9Ca24
    0x603f0200e863784e03cD262bB5266d819DD0eAf0

## 3. AnvilGovernorDelegator (proxy) — HIGH priority

Governance is currently decoded on the implementation address only
(0xfe1118…a361), but proposals/votes emit from this proxy — so those events
are NOT being captured today. Needed for any governance analytics.

    0x00e83d0698FAf01BD080A4Dd2927e6aB7C4874c9

- Contract name: AnvilGovernor
- Check "this is a proxy" — implementation (ABI source):

    0xfe1118cE38818EA3C167929eacb6310CDc42a361

## 4. AnvilTimelock — MEDIUM priority

Governance execution pipeline (queued/executed proposals).

    0x4eeB7c5BB75Fc0DBEa4826BF568FD577f62cad21

## 5. Liquidators — LOW priority

Nice-to-have for liquidation-path analytics; the LOC events already carry
liquidator addresses, so decode these only if convenient.

    0x9ae1CAA5cE6fA330fcE98315159BCD433B1342b8   PassThroughLiquidator
    0x8Aa57e442e4562c80FDDAD1b71ADF0BA75E2eb4C   Permit2PassThroughLiquidator
    0x716321565e1EAbA200789E14ad92c9dA40B14589   UniswapLiquidator

## 6. PythPriceOracle — LOW priority

Only needed for oracle-health analytics (price update events).

    0xC6f3405c861Fa0dca04EC4BA59Bc189D1d56Ee05

## Skip

- LetterOfCredit ProxyAdmin (0x12225bB1…341D) — admin plumbing, no analytics value
- TimeBasedCollateralPool Beacon (0x1f00D6f7…9E14) — upgrade plumbing only
