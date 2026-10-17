# 0003 — Event log and recovery of the matching engine

Status: accepted (Phase 2 library; service wiring in Phase 3)

## Context
The engine keeps the order book in memory only. After a crash it must resume with an identical book, losing no commands and emitting no divergent events (SPEC §5 Recovery, §6.6).

## Decision
- The **command log is the source of truth**: Kafka topic `matching.commands`, partitioned by market, written by the `orders` service through its transactional outbox. Engine state is a pure function of that log (ADR 0002), so replay is exact.
- **Snapshots** (`Engine.Snapshot(offset)`) are written every N commands: a canonical binary encoding of the sequence counters and every resting order in book/FIFO order, prefixed by the log **offset of the next command** and suffixed by a SHA-256 over the whole file. `Restore` verifies the checksum, every field and all book invariants, re-checks canonical byte equality, and never panics on arbitrary input (`FuzzRestore`).
- **Recovery** = load the newest valid snapshot, seek the consumer to its offset, replay. Output events carry per-market sequence numbers; downstream consumers (ledger, marketdata) dedupe by `(market, seq)`, so events re-emitted during replay are harmless.
- `StateHash()` (SHA-256 of the canonical state, independent of the snapshot offset) is the "book hash" used by tests and, in later phases, by the chaos test and the reconciler.
- Proof: `TestSnapshotReplayEqualsUninterruptedRun` snapshots at ~60 cut points of a 2,500-command random stream, restores, replays the remainder and requires identical events and an identical final hash; `FuzzEngineAgainstModel` repeats this at the midpoint of fuzzed streams.

## Recovery must fail loudly (Phase 3.1)
Replay is only correct if the log still contains every command since the checkpoint. The service therefore **refuses to start** (`ErrRefuseToStart`) instead of silently diverging:
- **Log truncated:** for each partition the resume offset (checkpoint offset, or 0 without a checkpoint) is compared with the log start offset; `resume < log start` means commands were deleted, the book cannot be rebuilt, and an empty book would also restart `seq` at 1, which consumers would dedupe away on `(market, seq)`. Empty snapshot directory with log start 0 is a normal fresh start.
- **Unrestorable checkpoints:** if a partition directory contains checkpoint files but none restores, starting empty is refused. A torn *newest* file falls back to the previous valid one.
- **Partition count changed:** markets map to partitions by key hash, so every checkpoint records the partition count of `matching.commands`; a different count is refused.
- **Retention:** `matching.commands` and `matching.events` are created with `retention.ms=-1` (`deploy/compose/kafka-topics.sh`); the command log is the source of truth and must not expire.
Each refusal has a test (`app/recovery_test.go`, `TestRefusesToStart*`).

## Alternatives considered
- **Rebuilding the book from `matching.events`:** requires reconstructing order state from deltas; the command log is smaller and replay reuses the real code path.
- **Persisting the book in PostgreSQL on the hot path:** violates "no DB on the hot path" and caps throughput.
- **Protobuf-encoded snapshots:** the format is internal, small, versioned and must be byte-canonical; a hand-written fixed layout is simpler to make canonical.

## Consequences
- Snapshot cost is proportional to resting orders (≈7 ms for 20k orders, docs/perf/engine/bench.txt); the service must take it between commands, not concurrently with them.
- Replay time after a crash is bounded by N commands at ≥ 1M cmd/s.
- The snapshot version field allows format evolution; unknown versions are rejected and the log replayed from an older snapshot.
