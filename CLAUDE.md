# Tickless — working rules

Full requirements are in [SPEC.md](SPEC.md) (source of truth). Progress and evidence: [PROGRESS.md](PROGRESS.md).

## Rules (from SPEC §0)
- Work strictly phase by phase. Do not start a phase until every acceptance criterion of the previous one is checked in PROGRESS.md **with evidence** (test name, benchmark output, command).
- Before coding a phase: write a short plan in PROGRESS.md under it. After: tick boxes, add "What was done / what is left".
- No stubs, no TODO shortcuts, no fake implementations presented as real. Deliberate simplifications are stated in a code comment, the ADR and the README.
- Non-obvious architectural choices get an ADR in `docs/adr/NNNN-title.md` (context, decision, alternatives, consequences).
- Keep `make test`, `make lint`, `make up` green at the end of every session. Small conventional-commit messages, English.
- Run tests/linters after every meaningful change. Never skip, disable or weaken a test to make it pass.
- Ambiguity: choose the simplest option that keeps SPEC §6 invariants, record in an ADR, continue.
- Never commit real private keys, mainnet RPC URLs or real-money paths. Regtest / local devnets / public testnets only.
- No floats in money paths. Money = integers (see `pkg/money`, ADR 0001).

## Layout
```
api/proto/        protobuf (buf)          api/openapi/   public REST spec
services/<name>/  one Go module each     pkg/           shared libs (module github.com/tickless/pkg)
deploy/compose/   local stack            deploy/helm/   charts
deploy/observability/  prometheus, grafana, otel, alertmanager
load/k6/  e2e/  web/  docs/adr/  docs/runbooks/  docs/perf/
```
Modules are tied together by `go.work`.

## Commands
| Command | What |
|---|---|
| `make test` | unit tests with race detector for all modules |
| `make lint` | golangci-lint + buf lint |
| `make bench` | engine benchmarks (phase 2+) |
| `make proto` | buf generate |
| `make up` / `make down` | compose stack (waits for healthy) |
| `make fuzz` | short fuzz run over fuzz targets |

Environment: Windows host with Git Bash/GNU make; Docker Desktop; Go toolchain in PATH.
