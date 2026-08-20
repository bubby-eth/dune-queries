# Anvil pools, Flexa Capacity v3, and AMP — architecture notes

Reference for how Flexa Capacity v3 custodies AMP collateral on Ethereum mainnet using
Anvil's contracts, and how to measure it correctly (on Dune or anywhere else).
Everything here was verified against on-chain state, verified source code, and the
transaction history as of 2026-08-20.

## The big picture

```
staker wallet ──stake()──▶ TimeBasedCollateralPool (BeaconProxy, one per pool)
                                   │  creates/updates a collateral reservation
                                   ▼
                           CollateralVault  ◀── holds all AMP (ERC-20 balances)
                                   ▲
Flexa (claim router) ──claim()─────┘  can draw on reservations to cover payments
```

Three layers:

| Layer | Address | Role |
|---|---|---|
| AMP token | `0xfF20817765cB7f73d4bde2e66e067E58D11095C2` | The collateral asset (ERC-20) |
| Pool contracts (×20 active) | listed by the [Flexa API](https://api.flexa.co/collateral_pools) | Per-app/per-network staking accounting: units, epochs, unstake queues |
| CollateralVault | `0x5d2725fdE4d7Aa3388DA4519ac0449Cc031d675f` | Custodies every AMP token; tracks balances and reservations |

**The single most important nuance: pools never hold AMP.** `AMP.balanceOf(pool)` is ~0.
All tokens sit in the CollateralVault, credited to the pool's *account* there and locked
in a *collateral reservation*. Any balance metric must be derived from the vault, not
the pool and not the token.

## Proxy and beacon setup

- Each pool is an OpenZeppelin **`BeaconProxy`** (EIP-1967 beacon slot
  `0xa3f0ad74...3d50`).
- All pools point at the shared **UpgradeableBeacon** `0x1f00D6F7C18a8eDF4F8Bb4EAd8a898abdD9c9E14`.
- The beacon's implementation is **`TimeBasedCollateralPool`**
  `0xCc437a7Bb14f07de09B0F4438df007c8F64Cf29f`.

Consequences:
- One beacon upgrade retargets **all 20 pools at once** — decoded ABIs, selectors, and
  event shapes can change for every pool in a single transaction.
- Etherscan shows the pools under "Read/Write as Proxy"; Dune decodes them (if at all)
  under the implementation's ABI. Event/call logs carry the **proxy** (pool) address as
  `contract_address`, never the implementation.

## The CollateralVault

Anvil's vault (`CollateralVault`, verified source) is a generic collateral custodian.
Per account and token it tracks two buckets:

- **`reserved`** — locked in a collateral reservation, actively backing something
  (for Flexa: payments). This is "the pool's collateral."
- **`available`** — in the vault but not reserved. For pools this is exit liquidity in
  transit: unstaked AMP waiting for `releaseEligibleTokens` to deliver it to stakers.
  Do **not** count it as pool collateral.

### Reservations

A reservation is `(collateralizableContract, account, tokenAddress, feeBasisPoints,
tokenAmount, claimableTokenAmount)` keyed by an auto-incrementing `reservationId`
(**ids are never reused**). For a Flexa pool, both `collateralizableContract` and
`account` are the pool address; each pool has exactly one live reservation per token
(Lightning's AMP reservation is id 18, created 2024-11-12).

- `tokenAmount` = reserved collateral (the headline figure).
- `claimableTokenAmount` = `tokenAmount` net of the claim fee
  (`feeBasisPoints` = 50 → claimable ≈ tokenAmount / 1.005). This is the true maximum
  drawable to cover payments.

### Event accounting (exact, from source)

| Event | Effect on reservation `tokenAmount` |
|---|---|
| `CollateralReserved(id, account, …, amount, claimableAmount, feeBps)` | created with `tokenAmount = amount` |
| `CollateralReservationModified(id, oldAmount, newAmount, …)` | `oldAmount → newAmount` (every processed stake/unstake) |
| `CollateralClaimed(id, amountWithFee, fee, remainderReleased)` | `-= amountWithFee`; if `remainderReleased = true` the reservation is **deleted** (remainder moves to the account's `available`) |
| `CollateralReleased(id, amount)` | reservation **deleted**; `amount` was the full remaining balance |

Reconstruction rule for a pool's reserved collateral from these events:
sum `Reserved.amount` + `(newAmount − oldAmount)` − `amountWithFee` (non-terminal claims
only), and force any reservation that saw `CollateralReleased` or a claim with
`remainderReleased = true` to exactly 0. Because ids are never reused and new
reservations (e.g. after `resetPool`) emit a fresh `CollateralReserved` keyed by the
pool's account, the reconstruction survives claims, releases, and resets.

Verified: this reproduces `getCollateralReservation(18).tokenAmount` to the wei.

### Governance / approvals

Only approved "collateralizable contracts" may create reservations
(`CollateralizableContractApprovalUpdated` events, `_updates` batches). Anvil
governance approved **70 pool contracts** for Flexa across two proposals:

- **Batch 1 — the 20 active pools.** Forum:
  [Proposal: Support for Flexa Capacity v3](https://forum.anvil.xyz/t/proposal-support-for-flexa-capacity-v3/93)
  → on-chain vote:
  [Tally proposal 24716934…038910](https://www.tally.xyz/gov/anvil/proposal/24716934911389170016757989901499273074878070323414038081847621593479638038910)
- **Batch 2 — 50 additional pools that have never created a reservation** —
  pre-approved spare capacity, not (yet) listed by the
  [Flexa API](https://api.flexa.co/collateral_pools). Forum:
  [Proposal: Approve 50 additional TimeBasedCollateralPools for Flexa Capacity](https://forum.anvil.xyz/t/proposal-approve-50-additional-timebasedcollateralpools-for-flexa-capacity/136)
  → on-chain vote:
  [Tally proposal 42330922…676832](https://www.tally.xyz/gov/anvil/proposal/42330922814698805060927905860016485953702258320535482888406024398956999676832)

The vault also charges a withdrawal fee (basis points, settable by the vault owner)
on withdrawals from `available` to a wallet.

## TimeBasedCollateralPool mechanics

### Units vs tokens

Stakers own **pool units**, not token amounts. Units convert to tokens pro-rata against
the pool's reservation. The ratio starts 1:1 and only diverges when collateral is
**claimed** (tokens leave, units stay) or the pool is **reset**. As of 2026-08-20 no
Flexa pool has ever had a successful claim or reset, so units = tokens 1:1 everywhere.
Don't build that assumption into anything long-lived.

- `getPoolUnits(token)` / `getAccountPoolUnits(account, token)` → `(total,
  pendingUnstake, releasable)` in units.
- `getAccountPoolBalance(account, token)` → a staker's balance in tokens.
- There is **no pool-level "total tokens" view on the pool contract** — the token
  figure lives in the vault reservation.

### Epochs and unstaking

- Epoch length: `epochPeriodSeconds` = **43,200 s (12 h)**; first epoch started at
  unix 1729526447 (2024-10-21). Epoch ids are absolute (e.g. ~1336 in Aug 2026).
- `unstake(token, units)` doesn't move tokens; it queues units as pending for the
  current epoch (state tracks at most two pending epoch buckets:
  `firstPendingUnstakeEpoch` / `secondPendingUnstakeEpoch`).
- **Pending unstakes remain inside the reservation — still claimable collateral —**
  until an epoch boundary passes and exits are processed.
- `releaseEligibleTokens(account, tokens[])` (permissionless poke, also run
  automatically during other operations) processes matured exits: shrinks the
  reservation (`CollateralReservationModified`), moves tokens to `available`, and
  delivers them out. This is why flow-based dashboards that subtract unstakes at
  request time drift slightly from the on-chain reservation between epoch boundaries.

### Staking entry points (all end up as reservation increases)

| Function | Selector | Path |
|---|---|---|
| `stake(token, amount, data)` | `0x3e12170f` | stake AMP already deposited in the vault |
| `depositAndStake(token, amount, data)` | `0x0475ad03` | pull AMP from wallet (ERC-20 approval to the **vault**) and stake in one tx |
| `stakeReleasableTokensFrom(pool, token, amount, data)` | `0x34048584` | restake tokens that were pending release |
| `unstake(token, units)` | `0xc2a672e0` | queue exit |
| `releaseEligibleTokens(account, tokens[])` | `0x9da5e959` | process matured exits |

Note the approval nuance: because the vault custodies tokens, wallet approvals for
`depositAndStake` are granted to the **CollateralVault**, and in-vault staking
authority is a separate per-account allowance
(`accountCollateralizableTokenAllowances`) granted to the pool, adjustable by
signature (EIP-712 typehashes in the vault).

### Roles and admin (AccessControl)

- `CLAIMANT_ROLE` — may `claim(tokens[], amounts[])`: draws collateral from the
  reservation to `defaultClaimDestinationAccount`
  (`0xb550CE16E66d6d4Cd10dB120D204856C7Ca0D823`) or a per-token override. This is how
  Flexa would actually consume collateral to cover a payment default.
- `CLAIM_ROUTER_ROLE` — configures where claims land
  (`setDefaultClaimDestinationAccount`, `setTokenClaimDestinationAccountOverride`).
- `RESETTER_ROLE` — may `resetPool(tokens[])`: terminates the reservation and starts a
  new one (new reservation id); unit/token ratio re-bases.
- `ADMIN_ROLE` / `DEFAULT_ADMIN_ROLE` — role management.
- `releaseEligibleTokens` has **no role gate** — anyone can poke exit processing.
- `initialize(...)` — proxy initializer (vault, epoch config, role holders).

### Observed usage (direct transactions to the Lightning pool, all-time to 2026-08-20)

| Method | Successful | Failed |
|---|---|---|
| `stake` | 1,181 | 6 |
| `stakeReleasableTokensFrom` | 550 | 4 |
| `unstake` | 535 | 5 |
| `depositAndStake` | 484 | 1 |
| `releaseEligibleTokens` | 185 | 0 |
| `claim` | 0 | 1 |

No other method has ever been invoked in a direct transaction. A handful of additional
calls arrive as internal calls (multisig wrappers, ERC-4337 `handleOps` bundlers), so
tx-level scans undercount slightly; trace- or event-based queries do not.

## Measuring on Dune

- Decoded vault tables: `anvil_ethereum.collateralvault_evt_collateralreserved`,
  `..._evt_collateralreservationmodified`, `..._evt_collateralclaimed`,
  `..._evt_collateralreleased` (also `fundsdeposited` / `fundswithdrawn` for
  wallet↔vault flows).
- **Reserved collateral per pool**: apply the event reconstruction rule above, keying
  reservations by the pool's address in `CollateralReserved.account`.
- Deposit counts: count reservation increases (`Reserved` + upward `Modified`), which
  captures every entry path uniformly, including multisig and 4337 flows.
- Pitfalls:
  - `AMP.balanceOf(pool)` ≈ 0 — meaningless.
  - Vault `available` is exit liquidity, not collateral.
  - `reserved` includes not-yet-processed pending unstakes.
  - The claimable (payment-capacity) figure is `reserved / 1.005` (50 bps claim fee).
  - The pool address ↔ name mapping comes from
    [`https://api.flexa.co/collateral_pools`](https://api.flexa.co/collateral_pools)
    (public, no auth; address in `id`, name in `entity.name`). Dune can't call APIs,
    so the list must be hard-coded into queries and refreshed when Flexa activates a
    new pool.

## Key addresses

| What | Address |
|---|---|
| AMP token | `0xfF20817765cB7f73d4bde2e66e067E58D11095C2` |
| CollateralVault | `0x5d2725fdE4d7Aa3388DA4519ac0449Cc031d675f` |
| Pool beacon (UpgradeableBeacon) | `0x1f00D6F7C18a8eDF4F8Bb4EAd8a898abdD9c9E14` |
| TimeBasedCollateralPool impl | `0xCc437a7Bb14f07de09B0F4438df007c8F64Cf29f` |
| Default claim destination | `0xb550CE16E66d6d4Cd10dB120D204856C7Ca0D823` |
| 20 active pools (name ↔ address) | [api.flexa.co/collateral_pools](https://api.flexa.co/collateral_pools) |
| 50 approved-but-unused pools | batch 2 of the governance approvals above |

## Appendix: full pool function selector map

```
0x0475ad03 depositAndStake(address,uint256,bytes)     0x9da5e959 releaseEligibleTokens(address,address[])
0x3e12170f stake(address,uint256,bytes)               0x62fb5b3e resetPool(address[])
0x34048584 stakeReleasableTokensFrom(address,address,uint256,bytes)
0xc2a672e0 unstake(address,uint256)                   0x74725001 claim(address[],uint256[])
0x41c8b268 initialize(address,uint256,address,address,address,address,address)
0x2f2ff15d grantRole(bytes32,address)                 0xd547741f revokeRole(bytes32,address)
0x36568abe renounceRole(bytes32,address)              0x248a9ca3 getRoleAdmin(bytes32)
0x91d14854 hasRole(bytes32,address)                   0x01ffc9a7 supportsInterface(bytes4)
0x7b218818 setDefaultClaimDestinationAccount(address)
0x765a3286 setTokenClaimDestinationAccountOverride(address,address)
0x75b238fc ADMIN_ROLE()                               0x8a867368 CLAIMANT_ROLE()
0xa0254de4 CLAIM_ROUTER_ROLE()                        0x79ca6620 RESETTER_ROLE()
0xa217fddf DEFAULT_ADMIN_ROLE()                       0xd8dfeb45 collateral()
0xfc088742 defaultClaimDestinationAccount()           0x96cf77e2 epochPeriodSeconds()
0xd5305e81 firstEpochStartTimeSeconds()               0xb97dd9e2 getCurrentEpoch()
0x5e8de33d getEpochEndTimestamp(uint256)              0xda984e82 getPoolUnits(address)
0xc93c5b40 getAccountPoolBalance(address,address)     0xf223ace5 getAccountPoolUnits(address,address)
0x25c4f050 getAccountTokenState(address,address)      0x6a811d01 getTokenContractState(address)
0x9c88d00e getClaimableCollateral(address[])          0xe0b1a29a calculateEpochExitBalance(address,uint256)
0xb37497e2 getAccountExitUnitsAndTokens(address,uint256,uint256,bool)
0xa0d2dd4c getTokenEpochExitBalance(address,uint256)  0x03dd2719 getTokenResetExitBalance(address,uint256)
0x9530cbc8 getTotalAccountUnitsPendingUnstake(address,address)
0x247ccf6e getTotalContractUnitsPendingUnstake(address)
0x4d6dffac tokenClaimDestinationAccountOverrides(address)
0xcb4004a8 tokenResetEpoch(address,uint256)
```
