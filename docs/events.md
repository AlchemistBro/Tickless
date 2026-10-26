# Kafka topics and event schemas

All messages are Protobuf (`api/proto`, generated into `pkg/gen`). Every record carries W3C `traceparent` headers so a consumer continues the trace of the request that caused the event. Delivery is **at-least-once**; consumers deduplicate (see ADR 0005).

| Topic | Key (= partition by) | Producer | Value | Consumers |
|---|---|---|---|---|
| `matching.commands` | market | orders (outbox) | `matching.v1.Command` | matching |
| `matching.events` | market | matching | `matching.v1.Event` | ledger, orders, candles (consumer groups); marketdata and the gateway private feed (own offsets / live) |
| `ledger.events` | user id | ledger (outbox) | `ledger.v1.BalanceChanged` | gateway private WS |
| `users.events` | user id | identity (outbox) | `users.v1.UserEvent` (oneof `signed_up`, `frozen`) | gateway (frozen set), notifier (P6) |
| `deposits.events` | user id | chainwatcher (outbox) | `chainwatcher.v1.DepositEvent` | ledger (group `ledger-deposits`), notifier (P6) |
| `withdrawals.events` | user id | withdrawals (P6) | planned | ledger, notifier |
| `candles.live` | market | candles | `candles.v1.CandleUpdate` | marketdata (public WS `candles.*`) |

Topics are created by the `kafka-init` job in `deploy/compose` (6 partitions, replication 1 locally).

## matching.v1.Command
`market`, `type` (PLACE/CANCEL), `order_id`, `user_id`, `side`, `order_type` (LIMIT/MARKET), `tif` (GTC/IOC/FOK), `post_only`, `price` (ticks; for market orders a protective bound, 0 = none), `qty` (lots), `time_unix_nano`.
Cancel-all never appears here: the orders service expands it into single cancels. Duplicate commands are harmless (ADR 0004).

## matching.v1.Event
Per market, `seq` is gap-free and strictly increasing; consumers dedupe on `(market, seq)`. Types:

| Type | Meaning | Fields used |
|---|---|---|
| `ACCEPTED` | place passed validation | order_id, user_id, side, price, qty |
| `REJECTED` | no effect on the book | order_id, reason |
| `TRADE` | fill at the **maker** price | order_id/user_id = taker, maker_order_id/maker_user_id, side = taker side, price, qty, taker_leaves, maker_leaves, trade_id |
| `CANCELLED` | remaining qty leaves the market | order_id, qty = remaining, reason (USER, IOC remainder, FOK unfilled, SELF_TRADE) |
| `BOOK_DELTA` | new total resting qty at a level (0 = removed) | side, price, qty |

Event order for a command: `ACCEPTED`, then per level swept `TRADE`* + one `BOOK_DELTA`, then at most one terminal `CANCELLED`, or the resting `BOOK_DELTA`. Replaying commands re-emits **identical** events (same seq), see ADR 0003.

## ledger.v1.BalanceChanged
`user_id`, `asset`, `available`, `held` (smallest units as decimal strings), `entry_id`, `reason` (`hold`, `release`, `trade`, `credit`). Published once per touched balance per ledger transaction.

## users.v1.UserEvent
`meta`, `user_id`, oneof `signed_up{email, verification_token}` / `frozen{frozen}`.

## Common
`common.v1.EventMeta{event_id (UUIDv7), traceparent, produced_at}`; `common.v1.Amount{units}` = integer count of the smallest unit as a base-10 string (ADR 0001).

## candles.v1.CandleUpdate
`meta`, `market`, `interval` (`1m 5m 15m 1h 4h 1d`), `candle` {`open_time_unix`, `open/high/low/close` (ticks), `volume_lots`, `turnover` (tick-lots, decimal string), `trades`}. Always the complete current state of that candle; published after each ClickHouse insert (best effort, see ADR 0008). The candles service dedupes trades by `(market, trade_id)` before inserting.

## chainwatcher.v1.DepositEvent
`meta`, `type` (`DETECTED` seen in a block below the threshold, `CONFIRMED` threshold reached, `REORGED` its block left the chain before the threshold), `user_id`, `chain` (`btc`, `eth`), `txid`, `index` (BTC vout, ERC-20 log index, `-1` for the value of an ETH transaction), `asset`, `amount` (smallest units), `address`, `block_height`, `block_hash`, `confirmations`.
A deposit can go `DETECTED -> REORGED -> DETECTED` (mined again on the new branch) and ends `CONFIRMED` at most once. The ledger credits on `CONFIRMED` only, with business key `deposit:{chain}:{txid}:{index}`; see ADR 0010.
