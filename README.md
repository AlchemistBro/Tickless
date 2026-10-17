# Tickless

A self-hostable spot crypto exchange in Go (regtest / local devnets only). Spec: [SPEC.md](SPEC.md). Status: [PROGRESS.md](PROGRESS.md).

**Current state: Phase 4** — users can sign up, trade over REST or in the **web terminal** against seeded balances; every fill is settled in a double-entry ledger; market data (order book, trades, ticker, candles) is served over REST and WebSocket. Services: `gateway`, `identity`, `orders`, `matching`, `ledger`, `marketdata`, `candles` on PostgreSQL + Kafka + Redis + ClickHouse, traced end to end.

```
 client --REST/HMAC--> gateway --gRPC--> identity   (users, JWT, API keys)
                          |  \--gRPC--> orders --PlaceHold(gRPC)--> ledger
                          |               | outbox (same tx as the order)
                          |               v
                          |        Kafka matching.commands --> matching (engine per partition, checkpoints)
                          |                                        | Kafka matching.events
                          |                  +---------------------+----------------------+
                          |                  v                                            v
                          \--gRPC--> ledger (settle trades, release holds)     orders (fills, status)
                                           | outbox -> ledger.events
```

## Quickstart
```
make up      # Postgres+pgbouncer, Redis, Kafka (KRaft), RabbitMQ, ClickHouse, OTel, Jaeger, Prometheus, Alertmanager, Grafana, Mailpit
make test    # unit tests with -race (uses a golang container when no C compiler is available)
make lint    # golangci-lint + buf lint
make fuzz    # fuzz targets: pkg/money, matching engine
make bench   # matching engine vs SPEC §5 targets (PASS/FAIL) + pprof
make test-integration  # testcontainers: PostgreSQL + Kafka per service
make e2e     # black-box suite against the running stack
make chaos-dup         # every Kafka message published twice; scenarios must still end identical
make down
make seed EMAIL=me@example.com ASSET=USDT AMOUNT=100000   # dev only: give a registered user funds
make web-dev   # hot-reloading terminal on :5173 (proxies to the gateway)
make web-test  # typecheck + unit tests of the terminal
```
| UI | URL |
|---|---|
| **Web terminal** | http://localhost:8088 |
| Grafana | http://localhost:3000 (admin/admin) |
| Jaeger | http://localhost:16686 |
| Prometheus / Alertmanager | :9090 / :9093 |
| Mailpit | http://localhost:8025 |
| RabbitMQ | http://localhost:15672 (tickless/tickless) |

All credentials are local-development placeholders. Host ports: gateway `:8080`, Postgres `:5433` (5432 is often taken by a local install), ledger gRPC `:9101` (dev seeding).

## Try it (curl)
```bash
curl -s localhost:8080/v1/markets | jq
curl -s -XPOST localhost:8080/v1/auth/signup -d '{"email":"me@example.com","password":"correct horse battery"}'
TOKEN=$(curl -s -XPOST localhost:8080/v1/auth/login -d '{"email":"me@example.com","password":"correct horse battery"}' | jq -r .access_token)
curl -s -XPOST localhost:8080/v1/orders -H "Authorization: Bearer $TOKEN" \
  -d '{"market":"BTC-USDT","side":"buy","type":"limit","price":"50000.00","quantity":"0.1"}'   # 422 until you have funds
```
Balances can be seeded in development through the ledger AdminCredit RPC (`LEDGER_ALLOW_SEED=true` in compose; see `e2e/harness_test.go`). Dev-only switches are all flagged `DEV ONLY` in `deploy/compose/docker-compose.yml`: `LEDGER_ALLOW_SEED`, `IDENTITY_AUTO_VERIFY` (until the notifier delivers mail in Phase 6) and ephemeral signing/encryption keys.

The public API is specified in [api/openapi/openapi.yaml](api/openapi/openapi.yaml); a unit test keeps it in lockstep with the router and validates real responses against it. Kafka topics and schemas: [docs/events.md](docs/events.md). Invariants and the tests that prove them: [docs/invariants.md](docs/invariants.md).

## The terminal is made of widgets
The UI in [web/](web/) is a grid of **widgets**; each panel (chart, order book, trades, order entry, balances, ...) is one small file in `web/src/widgets/` and is discovered automatically. To add a panel, describe it to an AI assistant (or write it yourself) following [web/WIDGETS.md](web/WIDGETS.md); it appears in the **Widgets** menu, can be dragged and resized, and a malformed widget is rejected without breaking the others. The WebSocket protocol it uses is specified in [docs/websocket.md](docs/websocket.md).

## Architecture decisions
- [0001 Money representation](docs/adr/0001-money-representation.md)
- [0002 Matching engine concurrency model and book structure](docs/adr/0002-matching-engine-concurrency-model.md)
- [0003 Event log and recovery](docs/adr/0003-event-log-and-recovery.md)
- [0004 Idempotent engine commands](docs/adr/0004-idempotent-engine-commands.md)
- [0005 Transactional outbox and idempotent consumers](docs/adr/0005-transactional-outbox.md)
- [0006 Order placement saga, holds and rounding](docs/adr/0006-order-placement-saga-holds-and-rounding.md)
- [0007 Kafka vs RabbitMQ](docs/adr/0007-kafka-vs-rabbitmq.md)
- [0008 Why ClickHouse](docs/adr/0008-why-clickhouse.md)

## Matching engine results (`make bench`, Linux, Core Ultra 7 155H)
| Target (SPEC §5) | Result |
|---|---|
| ≥ 1M cmd/s single market | ~6.5 M cmd/s (mixed add/cancel/match, ~20k resting orders) |
| p99 < 20 µs | ~0.5 µs |
| 0 allocs/op after warm-up | 0 mallocs in 5,000,000 commands |

Details, profiles and the known worst case (100k levels, ~30 µs): [docs/perf/engine](docs/perf/engine/README.md).

## Market data results (local stack)
| Target (SPEC §5 / phase 4) | Result |
|---|---|
| WS client rebuilds the exact book from snapshot + deltas | proven against the engine after every command and over a real socket |
| Candles for all intervals equal the trades | proven against an independent aggregation, also under duplicate Kafka delivery |
| Book delta visible on WS < 100 ms (p99) | **9.3 ms** (Kafka timestamp -> socket write, Prometheus histogram) |

## Known limitations (so far)
- Candles are published live best-effort over `candles.live`; the REST history from ClickHouse is the exact source.
- Public market data is served from memory by `marketdata`; an instance needs the Redis snapshot (or a full replay of `matching.events`) to start.
- The private WebSocket authenticates with an access token (no API-key signing) and carries live events only.
- A poison event stalls ONE partition of its consumer group (alert `ConsumerPartitionStalled`, runbook in `docs/runbooks/`); the ledger records it in `settlement_failures` and never skips it. Operators resolve it explicitly.
- Matching refuses to start (instead of guessing) if its command log was truncated, its checkpoints cannot be restored or the partition count changed. In local development after such a change, reset the stack with `make down` (removes volumes).
- A transient handler error (e.g. database down) retries with backoff and blocks the other partitions of that consumer while it lasts.
- A duplicate `PlaceOrder` delayed by more than 262,144 commands is not detected by the engine; it cannot double-settle but stalls the ledger partition (ADR 0004).
- The frozen-user set and API-key lookups are cached (Redis / 30 s in the gateway): revoking an API key can take up to 30 s on other gateway instances.
- Deposits, withdrawals, notifications, reconciliation, Kubernetes and the web UI are later phases (see [PROGRESS.md](PROGRESS.md)).
- Email verification is bypassed in the local stack (`IDENTITY_AUTO_VERIFY=true`) until the notifier exists; the verification token is already published on `users.events`.
- Postings/orders are not yet partitioned by month (Phase 9, with EXPLAIN evidence).
- Single-node Kafka/PostgreSQL locally; no replication.
