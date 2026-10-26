# 0010 — Chain scanning and reorg handling

Status: accepted (Phase 5)

## Context
Deposits must be credited once (SPEC §6 invariant 4) and never if their block leaves the chain before the confirmation threshold (invariant 5), and the system must recover from a reorganisation by itself. Nodes are polled over JSON-RPC (bitcoind, Anvil); the watcher can crash at any moment.

## Decision
- **Chain-agnostic scanner** (`chainwatcher/internal/scan`) over two interfaces: `Chain` (tip height, block at height on the best chain) and `Store` (head, hash at height, apply block, rollback, confirm). BTC and ETH adapters only translate a block into transfers.
- **Store every processed block's hash and parent hash.** Before extending, the scanner checks that its stored head is still the node's block at that height (this also catches a chain that became *shorter*, e.g. after `invalidateblock`) and that the next block's parent is the head. On a mismatch it walks back to the highest stored height whose hash still matches — the common ancestor — and calls `Rollback(ancestor)`: blocks above it are deleted and PENDING deposits in them become REORGED (event `DEPOSIT_EVENT_TYPE_REORGED`). Then it rescans the winning branch.
- **Atomic blocks.** `ApplyBlock` records the block and its deposits in one transaction; `(chain, txid, index)` is UNIQUE. A crash or a lost connection after COMMIT is harmless: the next step re-reads the head from the database. A REORGED deposit that is mined again on the new branch becomes PENDING again in its new block — the same deposit, never a second one.
- **Credit only at the threshold, only after validation.** `Confirm(head, required)` runs after the head was validated in the same step and moves PENDING deposits with `head - height + 1 >= required` to CONFIRMED, enqueuing `DEPOSIT_EVENT_TYPE_CONFIRMED` in the same transaction (outbox, ADR 0005). CONFIRMED is final (a trigger refuses any later update). The ledger credits on that event with business key `deposit:{chain}:{txid}:{index}`, so redelivery credits once.
- **Deposit identity.** BTC `(txid, vout)`; ERC-20 `(txhash, logIndex)`; the value of an ETH transaction uses index `-1` so it can never collide with log index 0 of the same transaction. Only successful transactions count (receipt status 1); block and parent hashes are taken from the node's JSON, never recomputed.
- **Leadership.** One scanner per chain holds a PostgreSQL session advisory lock on a direct connection (pgbouncer's transaction pooling cannot hold session locks); a second instance waits.
- **Defaults:** BTC 2 confirmations, ETH 12 (SPEC §1), polling every 500 ms in compose.

## Reorg deeper than the threshold
Not undone automatically: a credited deposit may already have been traded. `Rollback` reports how many CONFIRMED deposits were in the removed blocks; the scanner logs an error and increments `chainwatcher_deep_reorg_deposits_total`, and the reconciler (Phase 7) flags the ledger/on-chain difference. Choosing the threshold is the defence; this is the documented residual risk.

## Simplifications
- ETH sent to a deposit address by a contract (an internal call) is not detected — only top-level transactions and the token's `Transfer` logs.
- bitcoind does not return transactions disconnected by `invalidateblock` to its mempool (observed in the regtest test); in tests the sender rebroadcasts, as a real wallet would.

## Alternatives considered
- **Wait for N confirmations before recording anything:** simpler, but users would not see pending deposits (SPEC §1 asks to show them) and a reorg past N would go unnoticed.
- **ZMQ / websocket block notifications:** lower latency, but still need the same parent-hash bookkeeping for missed notifications; polling keeps one code path. The credit target (≤ 10 s after the threshold) is met with 500 ms polling: ~0.5 s measured end to end.
- **Recording only deposit transactions, not blocks:** cannot detect which deposits a reorg removed without re-asking the node for every pending transaction.

## Consequences
- Invariants 4 and 5 are proved at three levels: 300 random chain histories with reorgs, shorter chains, restarts and store failures before and after COMMIT against an in-memory reference store *and* the same suite against PostgreSQL; a real bitcoind with a forced 3-block reorg; and the running stack (`TestForcedThreeBlockReorgNeverCreditsAndRecovers`).
- The `blocks` table grows by one row per block per chain; pruning old rows below `head - max reorg depth` is a later optimisation.
