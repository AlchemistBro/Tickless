# 0004 — Idempotent engine commands (dedupe window)

Status: accepted (Phase 3)

## Context
Commands reach the engine through the orders service's transactional outbox, which is at-least-once: if the relay crashes after Kafka acknowledged a batch but before it marks the rows published, the batch is published again (SPEC chaos scenario "kill the outbox relay between commit and publish" and "duplicate every Kafka message").
For a *live* order a duplicate `PlaceOrder` is caught by the engine's order index. But for an order that already **finished** (filled or cancelled) the id is no longer in the book, so a late duplicate would be accepted as a brand-new order that no hold backs — the ledger could not settle its trades.

## Decision
- The engine remembers the ids of the last **N = 262,144** place commands in a ring + open-addressing set (`engine/dedupe.go`). A place whose id is in the index **or** the ring is answered with `Rejected(DUPLICATE_ORDER)` and changes nothing.
- Eviction is strictly FIFO, so the structure is deterministic; it is part of the snapshot (the window size is stored in it), so recovery and replay keep rejecting the same duplicates.
- Consumers treat `Rejected(DUPLICATE_ORDER)` and `Rejected(UNKNOWN_ORDER)` as **no-ops for the original order**: the ledger must not release the hold of the live original (`TestRejectedAndCancelledReleaseButDuplicateAndUnknownDoNot`), and the orders service ignores duplicates.
- A duplicate *cancel* is harmless: the second one is `Rejected(UNKNOWN_ORDER)`. If our database still shows the order open when such a rejection arrives, the cancel overtook the place command, and the cancel is re-sent a bounded number of times (`retryCancel`).
- Order id 0 is invalid.

## Why a window and not "all ids ever"
Unbounded memory. A duplicate can only come from the outbox relay re-publishing its last unmarked batch (seconds after the original) or a Kafka-level duplicate; both arrive long before 262k newer commands (≈ 2 minutes at 2,000 orders/s). Documented limit: duplicates delayed by more than N commands are not detected; the ledger's hold-per-order invariant (`consumeHold` fails for an order without an active hold) is the second line of defence and would surface the problem loudly instead of corrupting balances.

## Alternatives considered
- **Monotonic command sequence per market** with `seq <= last` ignored: breaks when outbox rows become visible out of commit order across concurrent transactions.
- **Persist processed command ids in a database from the engine:** puts a DB on the hot path.
- **Kafka transactions (exactly-once between outbox relay and topic):** does not cover the "published but not marked" crash window without a transactional DB<->Kafka bridge.

## Consequences
- 2 MiB of extra engine memory and 2 MiB of snapshot per market; zero allocations preserved (benchmarks unchanged: `make bench`).
- Model-based tests run the naive reference with the same window (a small one, to exercise eviction) and `TestDuplicatePlaceOfFinishedOrderIsRejected` pins the scenario above.
