# 0008 — Why ClickHouse for trades and candles

Status: accepted (Phase 4)

## Context
Candles (OHLCV) for six intervals and an unbounded, append-only history of trades must be served quickly for any market and range, and must always agree exactly with the trades. Writes are small and ordered per market, reads are range scans plus aggregation.

## Decision
- **`trades`** is a ClickHouse `MergeTree` ordered by `(market, ts, trade_id)`: time-range scans for one market read contiguous blocks.
- **Six materialized views** (1m, 5m, 15m, 1h, 4h, 1d) fold every inserted block into `AggregatingMergeTree` tables with `argMinState/argMaxState` (open/close keyed by `(ts, trade_id)` so equal timestamps resolve deterministically), `max/min/sum` simple aggregates and an `Int128` turnover `sum(price*qty)` (it exceeds `Int64` on ETH-sized lots). Candles are therefore *derived by the database from the same rows*, at insert time, for every interval independently: nothing can drift, no rollup job exists, and a backfill is just an `INSERT SELECT`.
- **Idempotent ingestion:** the matching engine numbers trades per market with a monotonic `trade_id`, and replays re-emit identical trades (ADR 0003). The candles service inserts only `trade_id` above the stored maximum of each market (cached; refreshed from ClickHouse after a rebalance), in one block per Kafka batch. Materialized views would double count a duplicate insert, so deduplicating *before* the insert is what keeps candles exact under redelivery (proved by `TestInsertIsIdempotentPerMarketAndSurvivesRestart` and `make chaos-dup`).
- Candles are stored as **ticks / lots integers** like everywhere else (ADR 0001); the marketdata API formats them with the market configuration. Turnover is a decimal string because it can exceed 64 bits.
- **Live updates:** after each insert the candles service re-reads the touched candles and publishes them to `candles.live`; marketdata fans them out on `candles.{market}.{interval}`. This feed is best effort; history over REST is exact.

## Alternatives considered
- **PostgreSQL** (plain table + `date_bin` queries, or TimescaleDB continuous aggregates): fine at small volume, but aggregation over millions of rows per request or an extra refresh job; the ledger database should not also carry analytical scans.
- **Computing candles in the service** (in-memory + periodic persistence): needs its own recovery story and cannot answer arbitrary historical ranges.
- **Redis sorted sets / TimeSeries:** no ad-hoc aggregation, weaker durability.
- **`ReplacingMergeTree` for dedupe:** deduplicates only eventually (at merge time) and does not protect the materialized views from double counting.

## Consequences
- One more stateful system, but it holds only derived/analytical data: it can be rebuilt by replaying `matching.events` (retention is infinite, ADR 0003) into `trades`.
- Interval boundaries are `toStartOfInterval` in UTC (`4h` and `1d` align to epoch midnight); the test suite recomputes buckets independently in Go to prove it.
- A single ClickHouse node locally; replication/sharding is out of scope.
