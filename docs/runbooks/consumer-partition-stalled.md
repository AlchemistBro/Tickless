# Runbook: ConsumerPartitionStalled

**Alert:** `ConsumerPartitionStalled` - `consumer_partition_stalled{group,topic,partition} == 1` for 1 minute.

**What it means:** a handler returned a *permanent* error for a record of that partition (a poison record). The consumer stopped consuming **only that partition** and did **not** commit past it; all other partitions continue. Nothing is skipped: everything behind the record waits, so for `ledger-settle` the trades of the markets on that partition are not settled until this is resolved.

**Impact:** for `ledger-settle`, holds stay held and balances do not move for the affected markets. For other groups see their handler.

## Diagnose
1. Find the coordinates in the service log (search `PARTITION STALLED`): group, topic, partition, first/last offset, error.
2. Ledger only: the failing event is stored in the database
   ```sql
   -- psql ... -d ledger
   SELECT market, seq, kafka_partition, kafka_offset, error, attempts, first_seen FROM settlement_failures ORDER BY first_seen;
   ```
3. Typical causes: `ledger: order N has no active hold` (an order reached the engine without a hold - an orders/ledger saga bug or a manual data fix), a duplicate beyond the engine dedupe window (ADR 0004), or an unbalanced/overflowing amount.
4. Inspect the record: `kafka-console-consumer.sh --topic matching.events --partition P --offset OFFSET --max-messages 1` (value is protobuf).

## Resolve
1. Fix the cause *in the data or code*, never by deleting the event: e.g. create the missing hold through the ledger (`PlaceHold`), or deploy the fix.
2. Restart the stalled service (`docker compose restart ledger`): the partition is re-read from the last committed offset and the event is applied; the dedupe table keeps already-applied events harmless.
3. Confirm: `consumer_partition_stalled` returns to 0 and `kafka_consumer_lag` drains.
4. Leave the `settlement_failures` row for the audit trail (or annotate it); it is not an input to anything.

## If the event is truly invalid and must be abandoned
Do not skip offsets by hand. Write an ADR/incident note, then apply the compensating ledger entries explicitly (new journal entries, the ledger is append-only) and only then move the group offset with `kafka-consumer-groups.sh --reset-offsets --to-offset OFFSET+1`.
