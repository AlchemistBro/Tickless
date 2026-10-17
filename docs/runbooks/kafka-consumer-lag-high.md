# Runbook: KafkaConsumerLagHigh

**Alert:** `KafkaConsumerLagHigh` - `kafka_consumer_lag > 1000` for 2 minutes on some `{group, topic, partition}`.

`kafka_consumer_lag` is the distance between the group's **committed** offset and the log end, computed by the consumer every 5 s. It keeps growing while a partition is stalled or the consumer is down, unlike a "last fetched" measure.

## Diagnose
1. Is the partition stalled? Check `consumer_partition_stalled` (alert `ConsumerPartitionStalled`, runbook `consumer-partition-stalled.md`).
2. Is the service up? `docker compose ps`, `/readyz`. A crashed consumer shows lag on all its partitions.
3. Is it slow? Look at the service's latency histograms in Grafana (ledger settlement, orders event apply) and PostgreSQL load. Lag that rises then falls is a burst; lag that rises steadily means the consumer cannot keep up.
4. Compare with the producer rate (`matching_events_total` rate).

## Resolve
- Stalled: follow the stalled runbook.
- Down: restart it; the group resumes from its committed offset with no loss.
- Slow: scale the consumer (more instances up to the partition count) or fix the slow dependency (database).
