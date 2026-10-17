# 0002 — Matching engine concurrency model and book structure

Status: accepted (Phase 2)

## Context
Matching must be deterministic (same command stream => same events, SPEC §6.6), fast (≥ 1M cmd/s, p99 < 20 µs, 0 allocs/op after warm-up) and simple enough to reason about. Locks, atomics and scheduler non-determinism work against all three.

## Decision
- **One goroutine owns one market** (`engine.Engine` is not safe for concurrent use and contains no locks). Parallelism comes from running many markets, each pinned to a Kafka partition (`matching.commands` is partitioned by market).
- **Pure state machine:** `Process(cmd, out) out`. Time arrives inside the command (stamped upstream), the engine never reads a clock, never iterates a Go `map`, and has no randomness. All output goes to a caller-owned, reusable slice.
- **Integers only:** `int64` price ticks / quantity lots (ADR 0001). Market orders are IOC/FOK orders with an optional protective price bound (0 = unbounded); this lets the `orders` service reserve a bounded quote amount for market buys.
- **Book structure:** each side is a slice of price levels sorted by `key` (price for bids, -price for asks) so the **best level is the last element**; each level holds an intrusive doubly-linked FIFO of orders. Cancel is O(1) via an **order index** (open addressing, linear probing, backward-shift delete → no tombstones, no allocation on churn). Orders and levels come from slab pools.
- **Events:** `Accepted`, `Rejected`, `Trade`, `Cancelled`, `BookDelta`, each with a gap-free per-market sequence number. One `BookDelta` (absolute new level quantity) is emitted per level touched by a command, after the level is processed.
- **Self-trade prevention = cancel newest:** when the incoming order reaches a resting order of the same user it stops; fills already made stand; the remainder is cancelled (`CancelSelfTrade`). FOK pre-checks liquidity up to the first own order.
- **Cancel-all** is not an engine command: the `orders` service expands it into single `CmdCancel`s because it owns the user's open-order list. This keeps the engine free of per-user indexes.
- `NewSized` pre-allocates pools/index/level slices for a configured capacity; beyond it the engine still works and grows.

## Why a sorted slice and not a B-tree or skip list
Activity concentrates at the touch, so inserting/removing levels at the tail moves few elements; `copy` on a slice of pointers is a tight memmove, binary search is cache friendly, and iteration/snapshots are a linear scan. A B-tree or skip list gives O(log n) worst-case inserts but adds pointer chasing on the common path and more code.

Measured (docs/perf/engine/bench.txt, Linux, Core Ultra 7 155H): mixed workload with ~20k resting orders: **~165-180 ns/command (≈6M cmd/s), p99 ≈ 0.5 µs, 0 allocs**. The pathological case — **100,000 levels per side with uniformly random depth inserts — costs ~30 µs per add+cancel pair**, i.e. it would miss the p99 target.

Mitigation: the `orders` service will enforce a price band around the last/mid price (Phase 3), bounding the number of live levels. If real books ever need unbounded depth, the level container is isolated in `book.go` and can be swapped for a B-tree without touching matching logic; the model-based test suite (`model_test.go`) guards such a change.

## Alternatives considered
- `map[uint64]*order` for the index: Go maps may allocate on churn and are slower; they also invite non-deterministic iteration.
- A per-market goroutine fed by a channel inside the library: that is how the service will drive the engine, but the library stays synchronous so tests and replay need no goroutines.
- Lock-free multi-writer books: no benefit; a single market is inherently serial.

## Consequences
- Throughput of one market is bounded by one core; the measured headroom (>6x the target) makes that acceptable.
- Correctness is verified three ways: table tests per type/TIF, a differential test against a naive reference model on random streams, and fuzzing (`FuzzEngineAgainstModel`) which also checks structural invariants after every command.
