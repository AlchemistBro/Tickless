# 0009 — Deposit address derivation

Status: accepted (Phase 5)

## Context
Every user needs a unique deposit address per chain (BTC on regtest, ETH and the ERC-20 Test USDT on Anvil). SPEC §2 requires HD derivation from an xpub so that the deposit service never holds private keys; the hot-wallet signer (Phase 6) is a separate service with its own key.

## Decision
- **Only extended public keys.** The `wallet` service is configured with one account-level xpub per chain (`WALLET_BTC_XPUB`, `WALLET_ETH_XPUB`). `derive.NewBTC/NewETH` refuse an extended private key (`ErrPrivateKey`) and an xpub of the wrong network; BTC mainnet is refused outright (`regtest` or `testnet` only).
- **Paths.** BTC: BIP84 `m/84'/1'/0'/0/i`, native segwit p2wpkh (`bcrt1q…`); BIP84 rather than BIP44 because the address type is p2wpkh and BIP84 is the path convention for it. ETH: BIP44 `m/44'/60'/0'/0/i`, EIP-55 checksummed. One ETH address receives both ETH and Test USDT. Only the non-hardened tail `/0/i` is derived in the service, which is all an xpub allows.
- **Index allocation.** `chain_counters` hands out the next index per chain inside the transaction that stores `(user, chain) -> (index, address)`; `PRIMARY KEY (user_id, chain)` makes concurrent first requests of one user agree on one address (`TestConcurrentFirstCallsAgreeOnOneAddressPerUserAndChain`). A lost race leaves a gap in the indexes; no wallet restores these keys from a seed with a gap limit, so gaps are harmless.
- **Watched before returned.** The wallet registers the address with chainwatcher (`RegisterAddress`, idempotent) before returning it to the user, so no deposit can arrive at an address nobody scans. If chainwatcher is unavailable the request fails with `UNAVAILABLE` and the stored address is reused on retry (`TestAddressIsReturnedOnlyAfterRegistrationAndSurvivesAFailure`).
- **Address matching** in chainwatcher uses a lower-cased key: bech32 is lower case by definition and Ethereum hex is case-insensitive (the mixed case is only a checksum).

## Devnet keys (deliberate simplification)
The devnet xpubs in `deploy/compose` were produced by `services/wallet/cmd/devxpub`, which generates a random seed in memory, prints the two account xpubs and discards the seed. Nothing can spend from the derived deposit addresses: deposits on the local devnets are receive-only, and the hot wallet that pays withdrawals (Phase 6) is funded separately. A production deployment would keep the seed offline (or in an HSM) and sweep deposit addresses with a separately authorised process; that is out of scope here.

## Alternatives considered
- **Random keys per user stored in the database:** the deposit service would hold private keys, the opposite of the requirement.
- **One shared address plus a memo/tag:** not a native concept on Bitcoin or plain ETH transfers; users forget memos.
- **Deriving in chainwatcher:** mixes the watcher (reads chains) with key material policy; the registry would still need an owner. The wallet owns derivation, chainwatcher owns watching.

## Consequences
- The wallet service is safe to expose to the rest of the system: a breach reveals addresses, not funds.
- Address derivation is proven against the key tree itself (`TestXPubDerivationMatchesTheKeyTree`) and its formats/uniqueness by property tests; the end-to-end suite pays real regtest/Anvil transactions to derived addresses.
