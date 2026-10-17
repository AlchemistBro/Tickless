# Tickless

A self-hostable spot crypto exchange in Go (regtest / local devnets only). Spec: [SPEC.md](SPEC.md). Status: [PROGRESS.md](PROGRESS.md).

**Current state: Phase 1** — monorepo skeleton, `pkg/money`, protobuf tooling, full local infrastructure.

## Quickstart
```
make up      # Postgres+pgbouncer, Redis, Kafka (KRaft), RabbitMQ, ClickHouse, OTel, Jaeger, Prometheus, Alertmanager, Grafana, Mailpit
make test    # unit tests with -race (uses a golang container when no C compiler is available)
make lint    # golangci-lint + buf lint
make fuzz    # fuzz targets for pkg/money
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
