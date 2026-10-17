# Progress

Legend: `[x]` done with evidence, `[ ]` open.

## Phase 1 — Skeleton and money
**Plan:** go.work + `pkg` module; `pkg/money` (Int128, decimals, fee rounding) with unit + fuzz tests vs `math/big`; ADR 0001; buf (lint/breaking/generate) with common proto; compose with Postgres, pgbouncer, Redis, Kafka (KRaft), RabbitMQ, ClickHouse, OTel collector, Jaeger, Prometheus, Alertmanager, Grafana, Mailpit — all with healthchecks; Makefile; GitHub Actions + GitLab CI.
- [x] `make up` brings everything healthy — all 13 containers Healthy + `scripts/wait-up.sh` probes (otel, jaeger, grafana, prometheus, mailpit) ok; Kafka topics created; psql via pgbouncer to `ledger` db ok; Prometheus targets up
- [x] `make test` green — `pkg/money` 89.8% cov, run with `-race` in golang:1.27 container (no gcc on host); fuzz targets run 10s each, no findings
- [x] `make lint` green — golangci-lint 0 issues, buf lint ok
- [x] Grafana and Jaeger reachable — Grafana provisioned "Overview" dashboard found via API; Jaeger :16686 ok
- [x] ADR for money (0001)

**What was done / what is left:** SPEC/CLAUDE/PROGRESS, `pkg/money` (Int128, Asset parse/format, Fee, MarketSpec, differential fuzz vs math/big), buf + common proto, compose stack, observability configs, GitHub+GitLab CI, Makefile. Left: CI pipelines are unverified until pushed to a remote; `buf breaking` only runs on PRs/MRs (needs a baseline); no service metrics yet (phase 3).

## Phase 2 — Matching engine as a library
**Plan:** module `services/matching`, package `engine` (int64 ticks/lots, ADR 0001). Sorted-slice price levels (best at tail) + intrusive FIFO lists + custom open-addressing order index + slab pools => 0 allocs after warm-up. `Process(cmd, out []Event) []Event` is a pure deterministic state machine (time comes from the command). Market orders = IOC/FOK with protective price bound; STP cancels the taker (newest); cancel-all is expanded by `orders` into single cancels. Snapshot = canonical binary + sha256, includes caller's Kafka offset. Tests: table tests per type/TIF, model-based differential test vs naive reference, fuzz (engine vs model, snapshot parser), snapshot+replay hash equality at every cut point, invariant checker. `cmd/enginebench` measures throughput/p99/allocs against SPEC §5 and exits non-zero on miss; `make bench` runs it plus `go test -bench`; pprof saved to docs/perf/engine/. ADRs 0002 (concurrency model + book structure), 0003 (event log and recovery).
- [x] §5 engine targets met and printed by `make bench` — Linux container, 5M mixed cmds: 6.49 M cmd/s (≥1M), p99 487 ns (<20 µs), 0 mallocs in 5M cmds; `-benchmem`: BenchmarkMixed/Add/Match 0 allocs/op (docs/perf/engine/bench.txt, README.md)
- [x] Invariant 6 (replay from any snapshot) — `TestSnapshotReplayEqualsUninterruptedRun` (~60 cut points, events + final hash identical), `FuzzEngineAgainstModel` (midpoint snapshot)
- [x] Invariant 8 (filled ≤ qty) — `TestFilledNeverExceedsQuantityAndConservation` (filled+cancelled+resting == qty for every order, 10 seeds); fee rounding toward exchange: `FuzzFeeNeverUnderCollects` (pkg/money)
- [x] Model-based + fuzz — `TestEngineMatchesNaiveModel` (40 seeds x 3000 cmds, event-for-event vs naive reference), `FuzzEngineAgainstModel` and `FuzzRestore` 45 s each with no findings; `make fuzz` green
- [x] pprof profiles in docs/perf/engine/ — cpu.pprof, allocs.pprof (+ *_top.txt)

**What was done / what is left:** `services/matching/engine` (book, order index, matching for limit/market, GTC/IOC/FOK, post-only, STP cancel-newest, snapshot/restore/StateHash), 88% coverage under -race, ADRs 0002/0003, `enginebench`, `make bench`. Known limit (documented, ADR 0002): sorted-slice levels degrade to ~30 µs/op at 100k levels with random-depth inserts; Phase 3 `orders` must enforce a price band. Not yet: Kafka wiring and snapshot cadence (Phase 3). Latency timing needs Linux (Windows clock too coarse), so `make bench` runs in a container on Windows.

## Phase 3 — Identity, gateway, orders, ledger, matching services
- [ ] Two users trade via REST against seeded balances
- [ ] Invariants 1, 2, 7 tested
- [ ] Duplicate-delivery chaos test passes
- [ ] One end-to-end trace visible in Jaeger

## Phase 4 — Market data and candles
- [ ] WS client rebuilds exact book from snapshot + deltas (test vs engine state)
- [ ] Candles for all intervals match trades
- [ ] WS latency target met

## Phase 5 — Deposits
- [ ] Invariants 4 and 5 tested incl. forced 3-block reorg on regtest
- [ ] Deposit credit target met

## Phase 6 — Withdrawals and notifications
- [ ] E2E withdrawal on both chains
- [ ] Above-threshold withdrawal executes only after admin approval
- [ ] SMTP failure chaos test passes

## Phase 7 — Reconciliation, SLOs, alerts
- [ ] Fake ledger discrepancy triggers alert with runbook link
- [ ] Invariant 3 checked on every run

## Phase 8 — Kubernetes
- [ ] Full e2e suite passes against kind

## Phase 9 — Performance pass
- [ ] End-to-end order ack target met; numbers and graphs in README

## Phase 10 — Web UI and admin
- [ ] Phase 6 e2e flow doable by hand in browser; Playwright smoke in CI

## Phase 11 — Hardening and docs
- [ ] New person completes demo script in < 15 min from README
