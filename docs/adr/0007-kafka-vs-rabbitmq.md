# 0007 — Kafka vs RabbitMQ split

Status: accepted (Phase 3; RabbitMQ adopted in Phase 6)

## Context
The system has two kinds of asynchronous traffic: an ordered, replayable record of facts (commands, trades, ledger movements, deposits) and work items that need per-message acknowledgement, delayed retry and dead-lettering (emails, withdrawal review tasks).

## Decision
- **Kafka (KRaft, franz-go)** carries everything that is an event log: `matching.commands`, `matching.events`, `ledger.events`, `deposits.events`, `withdrawals.events`, `users.events`, `candles.live`. Partitioned by market (matching) or user id (others) so per-key order holds. Properties we rely on: replay from an offset (matching recovery, ADR 0003), multiple independent consumer groups (ledger, orders, market data each read `matching.events`), retention as the audit trail.
- **RabbitMQ (amqp091-go)** carries *tasks*: notification emails, withdrawal review tasks and delayed retries. Properties we rely on: ack/nack per message, TTL + dead-letter exchanges for retry with backoff and a DLQ, competing consumers without partition-ownership constraints.

## Rule of thumb
If losing the order of messages would be a bug or you may need to re-read history -> Kafka. If each message is a unit of work that either succeeds, is retried later, or is parked for a human -> RabbitMQ.

## Alternatives considered
- Kafka only: retries/DLQ/delay need hand-built retry topics and consumer pausing; head-of-line blocking by a poison email would stall other users on the same partition.
- RabbitMQ only: no cheap replay, weak ordering at scale, no consumer-group fan-out of a single log.

## Consequences
- Two brokers to operate; both have health checks, dashboards (Phase 7) and lag/queue-depth alerts.
- Domain events that trigger work (e.g. `users.events: UserSignedUp`) are consumed from Kafka by the notifier's bridge, which enqueues the email task in RabbitMQ - the Kafka offset is committed only after the task is safely enqueued.
