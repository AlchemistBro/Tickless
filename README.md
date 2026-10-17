# Tickless

A self-hostable spot crypto exchange in Go (regtest / local devnets only). Spec: [SPEC.md](SPEC.md). Status: [PROGRESS.md](PROGRESS.md).

**Current state: Phase 3** — two users can sign up, trade over REST against seeded balances, and every fill is settled in a double-entry ledger. Services: `gateway`, `identity`, `orders`, `matching`, `ledger`, on PostgreSQL + Kafka + Redis, traced end to end.

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
```
| UI | URL |
|---|---|
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

## Architecture decisions
- [0001 Money representation](docs/adr/0001-money-representation.md)
- [0002 Matching engine concurrency model and book structure](docs/adr/0002-matching-engine-concurrency-model.md)
- [0003 Event log and recovery](docs/adr/0003-event-log-and-recovery.md)
- [0004 Idempotent engine commands](docs/adr/0004-idempotent-engine-commands.md)
- [0005 Transactional outbox and idempotent consumers](docs/adr/0005-transactional-outbox.md)
- [0006 Order placement saga, holds and rounding](docs/adr/0006-order-placement-saga-holds-and-rounding.md)
- [0007 Kafka vs RabbitMQ](docs/adr/0007-kafka-vs-rabbitmq.md)

## Matching engine results (`make bench`, Linux, Core Ultra 7 155H)
| Target (SPEC §5) | Result |
|---|---|
| ≥ 1M cmd/s single market | ~6.5 M cmd/s (mixed add/cancel/match, ~20k resting orders) |
| p99 < 20 µs | ~0.5 µs |
| 0 allocs/op after warm-up | 0 mallocs in 5,000,000 commands |

Details, profiles and the known worst case (100k levels, ~30 µs): [docs/perf/engine](docs/perf/engine/README.md).

## Known limitations (so far)
- Market data (order book / candles / WebSocket), deposits, withdrawals, notifications, reconciliation, Kubernetes and the web UI are later phases (see [PROGRESS.md](PROGRESS.md)).
- Email verification is bypassed in the local stack (`IDENTITY_AUTO_VERIFY=true`) until the notifier exists; the verification token is already published on `users.events`.
- Postings/orders are not yet partitioned by month (Phase 9, with EXPLAIN evidence).
- Single-node Kafka/PostgreSQL locally; no replication.
