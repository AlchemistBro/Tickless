# Progress

Legend: `[x]` done with evidence, `[ ]` open.

## Phase 1 — Skeleton and money
**Plan:** go.work + `pkg` module; `pkg/money` (Int128, decimals, fee rounding) with unit + fuzz tests vs `math/big`; ADR 0001; buf (lint/breaking/generate) with common proto; compose with Postgres, pgbouncer, Redis, Kafka (KRaft), RabbitMQ, ClickHouse, OTel collector, Jaeger, Prometheus, Alertmanager, Grafana, Mailpit — all with healthchecks; Makefile; GitHub Actions + GitLab CI.
- [x] `make up` brings everything healthy — all 13 containers Healthy + `scripts/wait-up.sh` probes (otel, jaeger, grafana, prometheus, mailpit) ok; Kafka topics created; psql via pgbouncer to `ledger` db ok; Prometheus targets up
- [x] `make test` green — `pkg/money` 89.8% cov, run with `-race` in golang:1.27 container (no gcc on host); fuzz targets run 10s each, no findings
- [x] `make lint` green — golangci-lint 0 issues, buf lint ok
- [x] Grafana and Jaeger reachable — Grafana provisioned "Overview" dashboard found via API; Jaeger :16686 ok
- [x] ADR for money (0001)
- [x] CI (GitHub Actions) green on the real remote — first real run 2026-10-17 after Phase 3.1 item 8 (workflow YAML fix); see Phase 3.1 item 8

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
**Plan:**
1. Engine: bounded deterministic dedupe window of recent order ids (in snapshot) so duplicated commands from the at-least-once outbox can never re-open a finished order (ADR 0004).
2. `pkg`: otel (traces/metrics/slog trace ids), pgx+goose helpers, outbox (insert + SKIP LOCKED relay + traceparent), kafka (franz-go producer/consumer, trace headers, `CHAOS_DUPLICATE_PUBLISH`), idempotency (processed_events in-tx), auth (EdDSA JWT, HMAC API-key signing), AES-GCM sealing.
3. Protobuf: `matching.v1` (Command/Event), `ledger.v1`, `orders.v1`, `identity.v1` gRPC + `ledger.v1`/`users.v1` event messages; docs/events.md.
4. Services (each `services/<n>/{app,cmd,internal,migrations}`): identity (argon2id, JWT, refresh, API keys, email verification token), ledger (double entry, holds, CHECK(amount>=0), settle trades/fees/releases from `matching.events` idempotently, outbox -> `ledger.events`), orders (market rules, price band, order saga PENDING_HOLD -> hold -> outbox command; consumes matching.events for fills; sweeper), matching service (engine per market, snapshot dir, offset-aware replay), gateway (chi REST, JWT + HMAC auth, Redis rate limit + idempotency keys, OpenAPI).
5. Dockerfile (distroless nonroot) + compose services; `e2e` module: trade flow over REST, invariants via SQL; chaos override duplicating every Kafka publish; trace check in Jaeger.
6. Fees charged in the received asset, rounded up (toward exchange); trade quote amount rounds down, holds round up. Partitioning of postings/orders is deferred to Phase 9 (SPEC lists it there with EXPLAIN evidence).
- [x] Two users trade via REST against seeded balances — e2e `TestTradeFlow`, `TestOrderTypesReleaseHolds`, `TestAPIKeyTrading`, `TestAdminHaltAndFreeze` (`make e2e`, 6 s from a fresh `make up`; warm flow signup→filled+settled ≈ 0.4 s)
- [x] Invariants 1, 2, 7 tested — see [docs/invariants.md](docs/invariants.md) (DB-level trigger/CHECK tests, 150-step random ledger test, SQL assertions after every e2e scenario, client_order_id idempotency)
- [x] Duplicate-delivery chaos test passes — `make chaos-dup`: all producers publish every Kafka message twice (10 duplicated `(market,seq)` pairs observed), balances and ledger invariants identical; plus `TestRestartMidStreamLosesNothingAndReplaysIdentically` (matching killed, newest checkpoint discarded, 127 replayed events identical) and `TestRelayRepublishesAfterCrashBeforeCommit`
- [x] One end-to-end trace visible in Jaeger — e2e `TestTraceSpansAllServices` finds one trace containing spans of gateway, orders, matching and ledger (async hops continued from W3C headers in the outbox/Kafka records); trace id printed in the test log, viewable at `http://localhost:16686/trace/<id>`

**What was done / what is left:** Services identity/orders/ledger/matching/gateway with unit (-race), integration (testcontainers) and e2e/chaos tests; OpenAPI spec validated against the router and real responses; ADRs 0004–0007; `docs/events.md`; Dockerfile (distroless, nonroot) + compose wiring; CI jobs for lint/proto/unit/integration/e2e/chaos in GitHub Actions and GitLab. Coverage (domain packages, unit+integration): ledger store 80.5%, identity service 81.6%, orders 77.7%, engine 88.3%, partition 94.7%, gateway api 81.2%, money 88.2%, market 80.2%. Bugs found and fixed on the way by the tests: outbox `jsonb` binding (bytea vs text), grpc-go TXT lookup stalling first calls ~8 s behind Docker DNS, `BlockRebalanceOnPoll` + `Close` hanging shutdown (now also a hard 15 s deadline in `svc.Main`). Left for later phases: postings/orders monthly partitioning (Phase 9), SLO dashboards (Phase 7), the WebSocket push step of the trace (Phase 4). Note: CI workflows are written but unverified until pushed to a remote.

## Phase 3.1 — Review fixes before Phase 4
Source: [docs/review/phase-3.1-fixes.md](docs/review/phase-3.1-fixes.md).
**Plan:** (1) per-partition consumption in `pkg/kafka` with transient/permanent error classes, stalled-partition gauge, lag from committed offsets, `settlement_failures` table, alerts + runbooks; (2) matching refuses to start on log truncation / unrestorable checkpoints / changed partition count, retention.ms=-1; (3) gateway fails closed on the frozen check and rebuilds the set from identity (`ListFrozen`); (4) API-key `POST /orders` requires `client_order_id`; (5) `email_verified` enforced for orders; (6) append-only trigger on ledger postings/journal entries; (7) ADR 0004/0005 wording; (8) push to GitHub and get Actions green.
- [x] 1. Poison events do not wedge consumers silently — `pkg/kafka/consume.go`: handlers get one partition's batch; `kafka.Permanent(err)` stalls only that partition (no commit past it, `consumer_partition_stalled{group,topic,partition}=1`, coordinates logged), transient errors retry with backoff; `kafka_consumer_lag` now comes from committed offsets via `kadm.Lag`. Ledger: one transaction per partition batch, `SettlementError` → row in `settlement_failures` (migration 0002) → `kafka.Permanent`; the ledger never skips an event. Alerts `ConsumerPartitionStalled` (for 1m) and `KafkaConsumerLagHigh` in `deploy/observability/prometheus/rules/kafka.yml` (checked by `make lint-alerts`, promtool) with runbooks `docs/runbooks/consumer-partition-stalled.md`, `kafka-consumer-lag-high.md`. Tests: `TestPermanentErrorStallsOnlyThatPartition` (real Kafka: stalled partition stops, other partitions keep settling, gauge = 1, lag grows by 10 while stalled), `TestTransientErrorIsRetriedThenCommitted`, `TestUnsettleableEventIsPermanentAndRecorded` (ledger: `consumeHold` failure → permanent, recorded, rolled back, settles once fixed), `TestUnknownMarketIsNotPermanent`.
- [x] 2. Matching recovery fails loudly instead of diverging — `retention.ms=-1` on `matching.commands`/`matching.events` (verified with `kafka-configs.sh --describe`); `verifyLogStart` refuses when resume offset < log start; checkpoint dir with files but none restorable → refuse; partition count stored in the checkpoint (format `TLC2`, old `TLCK` named explicitly) and a change refused. ADR 0002/0003 updated. Tests: `TestRefusesToStartWhenResumeOffsetIsBeforeLogStart` (real broker, `DeleteRecords`; no-snapshot and old-checkpoint cases), `TestRefusesToStartWhenPartitionCountChanged`, `TestRefusesToStartWhenCheckpointsAreUnrestorable`, unit `TestLoadCheckpoint*` (empty dir OK, torn newest falls back, none restorable, count changed), `TestRestoreNamesTheOldCheckpointFormat`. The refusal also fired for real on a stale pre-3.1 volume during this work.
- [x] 3. Gateway frozen check fails closed; frozen set rebuilt from identity — `isFrozen` returns an error → 503; new RPC `identity.ListFrozen`; `Server.SyncFrozen` replaces the Redis set at start, `/readyz` and authenticated requests wait for it (`RequireFrozenSync`). Tests: `TestFrozenCheckFailsClosedWhenRedisIsDown`, `TestFrozenSetIsRebuiltFromIdentity` (stale members removed, missing added, 503 until synced, identity-down leaves set untouched), identity `TestFreezeBlocksLoginRefreshAndKeys` (ListFrozen), e2e `TestFrozenUserStaysFrozenAfterRedisWipeAndGatewayRestart` (Redis key deleted + gateway restarted → still 403).
- [x] 4. API-key POST /orders requires client_order_id — 422 `client_order_id_required`; OpenAPI updated (and still validated against the router). Tests: `TestAPIKeyOrdersRequireClientOrderIDAndReplayIsIdempotent` (replayed signed request → 200, one order), e2e `TestSignedOrderReplayCreatesOneOrder` (replay creates exactly one open order, hold taken once).
- [x] 5. email_verified enforced for placing orders — `requireVerified` on `POST /orders` (403 `email_not_verified`; cancel and read stay allowed); API keys only resolve for verified users in identity; `IDENTITY_AUTO_VERIFY` kept for local dev. Test: `TestUnverifiedEmailCannotPlaceOrders`. Withdrawals will reuse the middleware in Phase 6.
- [x] 6. Ledger postings/journal_entries are append-only — migration 0002 triggers (BEFORE UPDATE/DELETE row-level, BEFORE TRUNCATE statement-level). Test: `TestLedgerTablesAreAppendOnly` (direct UPDATE/DELETE/TRUNCATE on both tables fail, data untouched).
- [x] 7. ADR 0004/0005 wording fixed — ADR 0005 says outbox `id` order, explains why id order ≠ commit order and why place→cancel is still safe; ADR 0004 says a duplicate beyond the window stalls the partition (permanent `ErrNoActiveHold`) instead of double-settling. `pkg/outbox` doc comment and test message corrected.
- [x] 8. GitHub remote, CI green — remote `https://github.com/AlchemistBro/stockMarket.git`, branch `main`; first run failed on invalid workflow YAML (colon in a step name, found with `actionlint`), fixed; `runbook_url` points at the real repository. CI run status: see the final report below (jobs lint x7, proto, unit, integration, e2e incl. chaos-dup).

**What was done / what is left:** all eight review items fixed with tests; `make up` now rebuilds images (`--build`: the e2e suite caught a stale gateway image); unit, integration, e2e and lint are green locally. Known remaining limits: a transient handler error still blocks the other partitions of the same consumer while it retries (only permanent errors stall one partition); orders/gateway consumers have no permanent-error classification yet (their lag is visible and alerted); `kafka_consumer_lag` is computed every 5 s.

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
