# WebSocket API

Two endpoints on the gateway (`ws://localhost:8080`; the web terminal reaches them through nginx on `:8088`). JSON text frames only. **All money is a decimal string.** Messages have a `type`; channel messages also have a `channel`. A client that cannot keep up (queue of 4096 messages) is disconnected: reconnect and resubscribe, every subscription starts with a fresh snapshot.

Client operations (both endpoints): `{"op":"ping"}` -> `{"type":"pong"}`. Errors: `{"type":"error","message":"..."}`.

## Public: `/ws/public` (no authentication)

```json
{"op":"subscribe","channels":["book.BTC-USDT","trades.BTC-USDT","ticker.BTC-USDT","candles.BTC-USDT.1m"]}
{"op":"unsubscribe","channels":["book.BTC-USDT"]}
```
Max 50 subscriptions and 100 connections per client address. Every subscribe is acknowledged with `{"type":"subscribed","channel":"..."}`.

| Channel | Messages |
|---|---|
| `book.{market}` | `snapshot` right after subscribing, then `delta`s |
| `trades.{market}` | `trades_snapshot` (last 50), then one `trade` per execution |
| `ticker.{market}` | `ticker` snapshot, then at most 4 updates per second while trades occur |
| `candles.{market}.{interval}` | `candle` whenever trades change that candle; intervals `1m 5m 15m 1h 4h 1d` |

### Order book protocol (exact rebuild + resync)
```json
{"type":"snapshot","channel":"book.BTC-USDT","seq":120,"bids":[["50000","0.5"],["49999.5","1"]],"asks":[["50001","0.2"]]}
{"type":"delta","channel":"book.BTC-USDT","seq":125,"prev":120,"side":"sell","price":"50001","qty":"0.1"}
```
* Levels are best first in the snapshot (bids high to low, asks low to high). A `delta` sets the **absolute** total quantity at `price`; `qty` `"0"` removes the level.
* `seq` is the matching engine's sequence number of the change; `prev` is the `seq` of the previous *book* message you were sent. The snapshot carries the `seq` of the last book change.
* **Client rule:** apply a delta only if `prev == lastSeq`, then set `lastSeq = seq`. Otherwise a message was missed: drop the local book and send `unsubscribe` + `subscribe` (or reconnect) to get a new snapshot. The server takes the snapshot and registers the subscription atomically, so no delta can fall between a snapshot and the first delta.
* The reference implementation is `web/src/core/book.ts`; the guarantee "snapshot + deltas equals the engine book after every command" is tested in `services/marketdata/internal/state` and over the wire in `TestWSClientRebuildsExactBookFromSnapshotAndDeltas`.

```json
{"type":"trade","channel":"trades.BTC-USDT","id":"41","price":"50001","qty":"0.1","side":"buy","time":"2026-10-18T10:00:00.123456789Z"}   // side = taker side
{"type":"ticker","channel":"ticker.BTC-USDT","market":"BTC-USDT","last":"50001","open":"49000","high":"50500","low":"48900","volume":"12.5","quote_volume":"612500.1","trades":314,"change_pct":"2.04"}
{"type":"candle","channel":"candles.BTC-USDT.1m","t":1792274100,"o":"50000","h":"50001","l":"49999","c":"50001","v":"0.4","quote_volume":"20000.3","n":2}
```

## Private: `/ws/private` (own events only)

The first message must authenticate within 10 seconds (browsers cannot set headers on WebSocket requests, and a token in the URL would leak into logs):

```json
{"op":"auth","token":"<access token>"}        ->  {"type":"authenticated","user_id":"7","channels":["orders","fills","balances"]}
```
An invalid token, a frozen user, or a failure to check the frozen state (fail closed) gets an `error` and the connection is closed. After authentication the connection receives, without any subscribe operation, only the user's own events:

```json
{"type":"order","event":"accepted","order_id":"100","market":"BTC-USDT","side":"buy","price":"50000","qty":"0.5"}
{"type":"order","event":"cancelled","order_id":"100","market":"BTC-USDT","qty":"0.3","reason":"REASON_CANCEL_USER"}
{"type":"order","event":"rejected","order_id":"101","market":"BTC-USDT","reason":"REASON_POST_ONLY_WOULD_CROSS"}
{"type":"fill","order_id":"100","market":"BTC-USDT","side":"buy","role":"maker","price":"50000","qty":"0.2","leaves":"0.3","trade_id":"9"}
{"type":"balance","asset":"USDT","available":"80000","held":"0"}
```
`fill.side` is the user's own side; `role` is `taker` or `maker`; `leaves` is what remains of the user's order. Fees are booked by the ledger and show up in the next `balance` message and in `GET /v1/ledger`. The private feed carries live events only; load history and current state over REST.

## Measured delivery latency
`ws_delivery_seconds` (Kafka record timestamp -> WebSocket write) on the local stack: **p99 9.3 ms** after a burst of 60 trades (`TestWebSocketDeliveryLatencyP99`, SPEC §5 target < 100 ms).
