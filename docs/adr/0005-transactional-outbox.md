# 0005 — Transactional outbox and idempotent consumers

Status: accepted (Phase 3)

## Context
Services that change database state and must tell others (orders -> matching commands, ledger -> balance events, identity -> user events) face the dual-write problem: committing to PostgreSQL and publishing to Kafka cannot be atomic. SPEC §3 asks for exactly-once *effects* on top of at-least-once delivery.

## Decision
**Producers - transactional outbox** (`pkg/outbox`):
- The message is inserted into the service's own `outbox` table **in the same transaction** as the domain change. The W3C trace context is stored with it, so the consumer continues the trace.
- A relay in each service polls `SELECT ... WHERE published_at IS NULL ORDER BY id LIMIT n FOR UPDATE SKIP LOCKED`, publishes the batch to Kafka (idempotent producer, acks=all), then marks the rows published and commits. A transaction-scoped advisory lock lets only one relay instance publish at a time, which preserves commit order across replicas (needed so a cancel never overtakes its place command).
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
- Ordering is guaranteed per Kafka partition (market or user key), not globally.
