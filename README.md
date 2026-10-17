# Tickless

A self-hostable spot crypto exchange in Go (regtest / local devnets only). Spec: [SPEC.md](SPEC.md). Status: [PROGRESS.md](PROGRESS.md).

**Current state: Phase 2** — `pkg/money`, the matching engine library (`services/matching/engine`), protobuf tooling, full local infrastructure.

## Quickstart
```
make up      # Postgres+pgbouncer, Redis, Kafka (KRaft), RabbitMQ, ClickHouse, OTel, Jaeger, Prometheus, Alertmanager, Grafana, Mailpit
make test    # unit tests with -race (uses a golang container when no C compiler is available)
make lint    # golangci-lint + buf lint
make fuzz    # fuzz targets: pkg/money, matching engine
make bench   # matching engine vs SPEC §5 targets (PASS/FAIL) + pprof
make down
```
| UI | URL |
|---|---|
| Grafana | http://localhost:3000 (admin/admin) |
| Jaeger | http://localhost:16686 |
| Prometheus / Alertmanager | :9090 / :9093 |
| Mailpit | http://localhost:8025 |
| RabbitMQ | http://localhost:15672 (tickless/tickless) |

All credentials are local-development placeholders.

## Architecture decisions
- [0001 Money representation](docs/adr/0001-money-representation.md)
- [0002 Matching engine concurrency model and book structure](docs/adr/0002-matching-engine-concurrency-model.md)
- [0003 Event log and recovery](docs/adr/0003-event-log-and-recovery.md)

## Matching engine results (`make bench`, Linux, Core Ultra 7 155H)
| Target (SPEC §5) | Result |
|---|---|
| ≥ 1M cmd/s single market | ~6.5 M cmd/s (mixed add/cancel/match, ~20k resting orders) |
| p99 < 20 µs | ~0.5 µs |
| 0 allocs/op after warm-up | 0 mallocs in 5,000,000 commands |

Details, profiles and the known worst case (100k levels, ~30 µs): [docs/perf/engine](docs/perf/engine/README.md).
