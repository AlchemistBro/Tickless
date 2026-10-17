# Phase 3.1: review fixes before Phase 4

Source: external review of phases 1-3. Work through every item below before starting Phase 4.

## Why (short)
- A permanent handler error makes `kafka.Consume` retry the whole cross-partition batch forever; ledger settlement stops for all markets and `kafka_consumer_lag` freezes because it is only updated on poll.
- Topics have no `retention.ms`, so the broker default (7 days) applies. If checkpoints are lost, matching silently restarts from an empty book on a truncated log, `seq` restarts at 1, and the ledger dedupe on `market:seq` silently skips the new events.
- The frozen-user check fails open on Redis errors.
- Signed API-key requests can be replayed within the 30 s window; `POST /orders` without `client_order_id` then creates a second order.
- `email_verified` is never enforced.
- The repository has no remote, so CI has never run.

## Tasks
Before Phase 4, fix the review findings below. Treat them as Phase 3.1 in PROGRESS.md
with one checkbox per item and evidence (test name or command). Keep all existing tests green.
Do not weaken any test.

1. Poison events must not wedge consumers silently.
   - pkg/kafka Consume: process records per partition; classify handler errors as
     transient (retry with backoff) or permanent (kafka.Permanent(err) wrapper).
   - On a permanent error: stop consuming THAT partition only, keep others running,
     expose gauge consumer_partition_stalled{group,topic,partition}=1, log the record
     coordinates. The ledger must never skip a settlement event: record it in a new
     ledger table settlement_failures (market, seq, offset, error, first_seen).
   - ledger ApplyEvents: one transaction per partition batch, not across partitions.
   - Compute kafka_consumer_lag from committed offsets (kadm Lag), so it keeps growing
     while a partition is stalled.
   - Add Prometheus alerts now: ConsumerPartitionStalled (for 1m) and KafkaConsumerLagHigh,
     each with a runbook in docs/runbooks/.
   - Tests: integration test where one event makes consumeHold fail permanently: the
     partition stalls, other partitions keep settling, the gauge is 1, lag grows.

2. Matching recovery must fail loudly instead of diverging.
   - Create matching.commands and matching.events with retention.ms=-1 (kafka-topics.sh).
   - On start, for each partition compare the resume offset with the log start offset;
     if resume < log start, refuse to start with a clear error.
   - If the snapshot dir contains checkpoint files but none restores, refuse to start
     (do not fall back to an empty partition silently). Empty dir + log start 0 is fine.
   - Store the partition count in the checkpoint; refuse to start if it changed.
   - Update ADR 0003 and ADR 0002 accordingly. Tests for each refusal.

3. Gateway: fail closed on the frozen check. If Redis errors, return 503.
   On gateway start, rebuild the frozen_users set from identity (new RPC ListFrozen).
   Test with a broken Redis client fake.

4. API-key requests: require client_order_id on POST /orders when authenticated by API key
   (422 otherwise), so a replayed signed request is idempotent. Update OpenAPI and tests.

5. Enforce email_verified for placing orders and (later) withdrawals; keep
   IDENTITY_AUTO_VERIFY for local dev. Test that an unverified user gets 403.

6. Ledger: forbid UPDATE and DELETE on postings and journal_entries with a trigger.
   Test that direct SQL fails.

7. Fix ADR 0005 wording (id order, not commit order; explain why place->cancel is safe).
   Note in ADR 0004 that a duplicate beyond the dedupe window stalls the partition
   (finding 1) instead of double-settling.

8. Create a GitHub remote (I will give you the URL), push, and get GitHub Actions green.
   Fix runbook_url to the real repository. Until CI is green, Phase 1 "CI" is not done:
   uncheck it in PROGRESS.md.

When done, update PROGRESS.md and README "Known limitations", then stop and report.
