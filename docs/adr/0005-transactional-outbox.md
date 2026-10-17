# 0005 — Transactional outbox and idempotent consumers

Status: accepted (Phase 3)

## Context
Services that change database state and must tell others (orders -> matching commands, ledger -> balance events, identity -> user events) face the dual-write problem: committing to PostgreSQL and publishing to Kafka cannot be atomic. SPEC §3 asks for exactly-once *effects* on top of at-least-once delivery.

## Decision
**Producers - transactional outbox** (`pkg/outbox`):
- The message is inserted into the service's own `outbox` table **in the same transaction** as the domain change. The W3C trace context is stored with it, so the consumer continues the trace.
- A relay in each service polls `SELECT ... WHERE published_at IS NULL ORDER BY id LIMIT n FOR UPDATE SKIP LOCKED`, publishes the batch to Kafka (idempotent producer, acks=all), then marks the rows published and commits. A transaction-scoped advisory lock lets only one relay instance publish at a time, so all messages leave in **outbox `id` order** (a `BIGSERIAL` assigned when the row is inserted).

**`id` order is not commit order.** Two concurrent transactions can commit in the opposite order of their ids, so a row with a lower id may become visible after a higher id has already been published; the relay then publishes the lower id later. This is acceptable for unrelated messages (they have no ordering relation). What must hold is *place before cancel for the same order*, and it does: the orders service only writes a cancel command for an order whose status is `NEW` or later, and that status is committed in the very transaction that inserts the place command. The cancel row is therefore inserted strictly *after* the place row has committed, so it gets a higher id and any relay snapshot that can see the cancel also sees the place; the relay publishes place first. (Cancelling a `PENDING_HOLD` order is refused, `ErrNotCancellable`.) Should the engine ever still see a cancel for an unknown order, the orders service re-sends it a bounded number of times (`retryCancel`, ADR 0004).
- A crash between "Kafka acked" and "marked published" re-publishes the batch: **at-least-once**. This is a tested behaviour (`TestRelayRepublishesAfterCrashBeforeCommit`), and `CHAOS_DUPLICATE_PUBLISH=true` makes every producer publish every message twice for the chaos run.
- Metrics: `outbox_backlog`, `outbox_oldest_age_seconds` (alert in Phase 7).

**Consumers - dedupe in the same transaction as the effect** (`pkg/idempotency`): each handler records `(consumer, event_id)` in `processed_events` inside the transaction that applies the effect (`ON CONFLICT DO NOTHING`, effect skipped when it already exists). Event ids are `(market, seq)` for matching events; business keys (`trade:<market>:<trade_id>`, `hold:<order>`, `release:<order>`, `credit:<key>`) make ledger entries independently idempotent. Handlers are batch-transactional: a failure rolls back the whole batch and the batch is redelivered.

## Why polling and not logical replication (Debezium/pgoutput)
Polling needs no extra infrastructure, works behind pgbouncer, is trivial to reason about and test, and its latency (20 ms idle poll, immediate when busy) is far below the 50 ms end-to-end target at our volumes. The cost is repeated cheap index scans (`outbox_unpublished` is a partial index) and a per-service relay goroutine. Logical replication would remove the polling and keep ordering by WAL position, at the price of a replication slot per service, a CDC component to operate and no pooler compatibility; it is the documented upgrade path if relay load ever matters.

## Alternatives considered
- Publishing from application code after commit ("commit then send"): loses messages on crash.
- Kafka transactions + DB: cannot span PostgreSQL.
- A single shared outbox table: violates "no shared tables".

## Consequences
- Every consumer must be idempotent; `processed_events` is pruned by age (rows older than the maximum replay horizon).
- Ordering is guaranteed per Kafka partition (market or user key) and, within one outbox, by row id - not by commit time and not globally.
