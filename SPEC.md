You are building a production-grade, self-hostable spot crypto exchange called Tickless as a personal Go project. The output must be a working product, not a demo: one command brings the whole system up, a user can sign up, deposit testnet coins, trade, see live market data and candles, and withdraw. Every claim in the README must be backed by a test, a benchmark, or a dashboard.

0. How you work on this repo
First action: save this whole prompt verbatim as SPEC.md. Create CLAUDE.md with the working rules from this section, the repo layout, and the commands to build, test and run. Create PROGRESS.md with the phase list from section 9 and a checkbox per acceptance criterion.
Work strictly phase by phase (section 9). Do not start a phase until every acceptance criterion of the previous one is checked in PROGRESS.md with evidence (test name, benchmark output, command).
Before writing code for a phase, write a short plan in PROGRESS.md under that phase. After finishing, update the checkboxes and add a "What was done / what is left" note.
No stubs, no TODO-driven shortcuts, no fake implementations presented as real. If something is deliberately simplified (e.g. AML screening uses a local denylist), say so in the code comment, in the ADR, and in the README.
Every architectural choice that is not obvious gets an ADR in docs/adr/NNNN-title.md (context, decision, alternatives, consequences). Minimum ADRs: money representation, matching engine concurrency model, event log and recovery, outbox, Kafka vs RabbitMQ split, reorg handling, deposit address derivation, why ClickHouse.
Keep make test, make lint, make up green at the end of every session. Small, focused commits with conventional-commit messages in English.
Run tests and linters yourself after every meaningful change. Never disable, skip or weaken a test to make it pass.
If a requirement is ambiguous, choose the simplest option that keeps the invariants in section 6, record it in an ADR, and continue.
Never put real private keys, mainnet RPC URLs or real money paths anywhere. Only regtest / local devnets / public testnets.
1. Product scope
Functional, from a user's point of view:

Accounts: sign up with email + password (argon2id), email verification via a local mail catcher, login returning a short-lived JWT access token + refresh token, API keys with HMAC request signing for bots, TOTP 2FA required for withdrawals.
Markets: BTC-USDT, ETH-USDT, ETH-BTC at minimum. Each market has tick size, lot size, min notional, maker/taker fees in basis points. Markets are configured in the database, admins can halt/resume a market.
Orders: limit and market orders, time-in-force GTC, IOC, FOK, post-only, cancel, cancel-all per market, client order id for idempotent submission. Self-trade prevention (cancel newest).
Balances: available and on-hold per asset, full history of ledger movements per user.
Deposits: a unique deposit address per user per chain. BTC on regtest (and optionally testnet), ETH and an ERC-20 test USDT token on a local Anvil devnet (and optionally Sepolia). Credit after N confirmations (configurable, default BTC 2, ETH 12 on devnet). Show pending deposits with confirmation count.
Withdrawals: request to an external address, daily limits per user, 2FA, address-format validation, asynchronous screening against a local denylist, automatic execution below a threshold, manual review queue above it, broadcast from a hot wallet on regtest/Anvil, confirmation tracking, final status.
Market data: public REST for tickers, L2 order book (top N), recent trades, candles (1m, 5m, 15m, 1h, 4h, 1d). Public WebSocket with channels trades.{market}, book.{market} (snapshot + incremental deltas with sequence numbers), ticker.{market}, candles.{market}.{interval}. Private WebSocket: own orders, fills, balances.
Notifications: emails for signup verification, deposit credited, withdrawal requested/sent, order filled (opt-in).
Admin back office: list users, freeze user, halt market, approve/reject withdrawals in the review queue, view reconciliation reports, view ledger of any account. Admin is a separate role in JWT.
Web UI: a thin trading terminal (React + TypeScript + Vite, lightweight-charts for candles): login, balances, deposit address, order entry, order book, trades, chart, open orders, history, withdrawals, and an admin page. The UI is not the focus: functional, clean, no design polish beyond that.
Out of scope: margin, futures, fiat rails, KYC documents, mobile apps, mainnet.

2. Stack (mandatory)
Language: Go (latest stable), modules in a single monorepo with go.work. SQL. TypeScript only for the thin UI.
Storage: PostgreSQL 16+ (pgx v5, sqlc for queries, goose for migrations, pgbouncer in front in compose and k8s), Redis 7 (go-redis v9), ClickHouse (clickhouse-go v2).
Messaging: Kafka (KRaft mode, no ZooKeeper; client franz-go) for the event log and domain events; RabbitMQ (amqp091-go) for task queues: notifications, withdrawal review tasks, delayed retries.
APIs: gRPC + Protobuf between services (buf for lint, breaking-change check and codegen), REST + WebSocket at the edge (chi, coder/websocket), OpenAPI 3 spec for the public REST API generated or hand-written and validated in CI.
Chains: btcd/btcutil + bitcoind regtest node in compose; go-ethereum + Anvil devnet; HD derivation (BIP32/BIP44) from an xpub so the deposit service never holds private keys; the hot-wallet signer is a separate service with its own key.
Infra: Docker (multi-stage, distroless, non-root), docker compose for local, Kubernetes via kind + Helm (umbrella chart with a sub-chart per service, values for local), Linux.
CI: GitHub Actions as primary and an equivalent .gitlab-ci.yml. Jobs: lint (golangci-lint, buf lint, buf breaking), unit tests with race detector, integration tests with testcontainers, build images, helm lint + template, k6 smoke test against compose.
Observability: OpenTelemetry SDK in every service, OTel Collector, Jaeger for traces, Prometheus for metrics, Grafana with dashboards provisioned from the repo, Alertmanager with SLO-based burn-rate alerts. Structured JSON logs via log/slog with trace_id and span_id.
Quality: testify, go.uber.org/mock (gomock), testcontainers-go, Go benchmarks + pprof, fuzz tests (go test -fuzz) for the matching engine and parsers, k6 for load, a property-based or model-based test for the order book.
3. Architecture
Monorepo layout:

/api/proto/            protobuf definitions (one package per service)
/api/openapi/          public REST spec
/services/<name>/      one Go module per service: cmd/, internal/, migrations/
/pkg/                  shared libs: money, outbox, kafka, otel, auth, idempotency, testkit
/deploy/compose/       docker-compose.yml + configs
/deploy/helm/          umbrella chart + sub-charts
/deploy/observability/ prometheus rules, grafana dashboards, otel-collector, alertmanager
/load/k6/              load scenarios
/web/                  React UI
/docs/adr/             ADRs
/docs/runbooks/        one runbook per alert
Services:

Service	Responsibility	Talks to
gateway	Public REST + private/public WebSocket, auth (JWT, API-key HMAC), rate limiting (Redis token bucket per user and per IP), request validation, idempotency keys	all via gRPC; subscribes to Kafka for WS fan-out of private events
identity	Users, passwords, sessions, refresh tokens, API keys, TOTP, roles	PostgreSQL; publishes user.* events via outbox
orders	Validates orders against market rules, places a hold through ledger (gRPC, idempotent by order id), persists the order, writes PlaceOrder / CancelOrder commands to Kafka via transactional outbox	PostgreSQL, ledger, Kafka
matching	In-memory order books, one single-threaded engine goroutine per market, consumes matching.commands (partitioned by market), emits matching.events (accepted, rejected, trade, cancelled, book delta) with a per-market monotonic sequence number	Kafka only (no DB on the hot path); snapshots to object storage dir / PostgreSQL
ledger	Double-entry ledger: accounts per (user, asset, kind), holds, postings in balanced journal entries, idempotent by business key, settlement of trades and fees, balance queries	PostgreSQL; consumes matching.events; publishes ledger.* via outbox
marketdata	Builds L2 books, tickers and recent trades from matching.events, keeps snapshots in Redis, serves public WS channels and REST market data	Kafka, Redis
candles	Kafka → ClickHouse ingestion of trades, materialized views for OHLCV at all intervals, REST/gRPC for historical candles; live candle updates published to Kafka for WS	Kafka, ClickHouse
chainwatcher	Per-chain block scanners for BTC and ETH/ERC-20: deposit address registry, confirmation tracking, reorg detection and rollback, emits deposit.detected / deposit.confirmed / deposit.reorged via outbox	bitcoind, Anvil, PostgreSQL, Kafka
wallet	Derives deposit addresses from xpub on request; separate signer binary holds the hot-wallet key and only signs requests from withdrawals over mTLS gRPC	PostgreSQL, chains
withdrawals	Withdrawal state machine, limits, 2FA check, async screening, manual review queue, hand-off to signer, broadcast, confirmation tracking	PostgreSQL, ledger, Kafka, RabbitMQ, signer
notifier	Consumes RabbitMQ tasks, renders templates, sends email via SMTP (MailHog/Mailpit locally), retries with backoff, DLQ	RabbitMQ
reconciler	Scheduled jobs: ledger invariants, ledger vs on-chain wallet balances, ledger vs matching trades, writes reports and raises alerts	PostgreSQL, ClickHouse, chains
admin	Back-office API (gRPC behind gateway, admin role)	other services
Key design decisions (write ADRs for each):

Money: never floats. Integer amounts in the asset's smallest unit (int64 where safe, math/big or a fixed-point 128-bit type where needed, e.g. wei). Prices as integer ticks. A shared pkg/money with exhaustive tests and fuzzing.
Matching engine: deterministic state machine. Input is an ordered stream of commands; output is an ordered stream of events. Price levels in a sorted structure (B-tree or skip list keyed by price; pick and justify), FIFO queue per level, order index by id for O(1) cancel. Zero allocations on the hot path after warm-up (verify with -benchmem and pprof). No locks inside the engine: one goroutine owns one market.
Recovery: the engine snapshots its state every N commands (state + last consumed Kafka offset + a hash of the book). On restart it loads the latest snapshot and replays commands from the stored offset. A test proves that "snapshot + replay" produces a book hash identical to an uninterrupted run.
Exactly-once effects, at-least-once delivery: every Kafka consumer is idempotent (dedup table keyed by event id or by business key, inside the same DB transaction as the effect). Every producer that changes DB state uses a transactional outbox with a relay (polling with FOR UPDATE SKIP LOCKED, or logical replication; justify).
Kafka vs RabbitMQ: Kafka for the ordered, replayable event log and domain events. RabbitMQ for work queues where per-message ack, delayed retry and DLQ matter more than ordering (notifications, review tasks, retry scheduling).
Reorg handling: the watcher stores each processed block's hash and parent hash. On a parent mismatch it walks back to the common ancestor, marks affected deposits reorged, and re-scans. Credits happen only at the confirmation threshold. A deposit is identified by (chain, txid, output index or log index) and is credited at most once.
Holds: placing an order creates a hold in the ledger before the command reaches the engine; trades settle against the hold; cancel/expire releases the remainder. Market orders buy-side use a quote-amount hold with a protective price bound.
Back-pressure and fairness: per-user rate limits at the gateway, bounded channels inside services, Kafka consumer lag exposed as a metric and alerted.
Kafka topics (document in docs/events.md with Protobuf schemas): matching.commands, matching.events, ledger.events, deposits.events, withdrawals.events, users.events, candles.live. Partition by market for matching topics, by user id elsewhere.

4. Data model (minimum)
PostgreSQL schemas per service (no cross-service joins, no shared tables):

identity: users, credentials, refresh_tokens, api_keys, totp_secrets (encrypted at rest with a key from env), roles.
orders: markets, orders (status, filled qty, client_order_id unique per user), outbox.
ledger: accounts, journal_entries, postings (amount signed, entry_id), holds, processed_events, outbox. Balances as a materialized/denormalized table updated in the same transaction, with a check that it always equals the sum of postings.
chainwatcher: chains, blocks (height, hash, parent_hash), deposit_addresses, deposits (status, confirmations), outbox.
withdrawals: withdrawals (state machine), limits, review_tasks, outbox.
Partition postings and orders by month; indexes justified by EXPLAIN ANALYZE output saved in docs/perf/.
ClickHouse: trades (MergeTree, ordered by market, time), materialized views into candles_1m (AggregatingMergeTree) and rollups for other intervals.

Redis: L2 book snapshots, tickers, rate-limit buckets, short-lived idempotency keys at the gateway, WS session presence.

5. Non-functional requirements (targets, verify each)
Area	Target	How it is proven
Engine throughput	≥ 1M add/cancel ops/s single market, in-process	Go benchmark in CI, results in README
Engine latency	p99 < 20 µs per command in-process	benchmark with histogram
Engine allocations	0 allocs/op on add, cancel and match after warm-up	-benchmem
End-to-end order ack	p99 < 50 ms at 2,000 orders/s on a laptop compose stack	k6 + Grafana screenshot
Market data	book delta visible on WS < 100 ms after trade, p99	trace span + metric
Deposit credit	≤ 10 s after the confirmation threshold on regtest	integration test
Recovery	engine restart under load loses zero commands and keeps book hash	chaos test
Availability (local SLOs)	99.9% successful order placements, 99% deposit credit within 1 min of threshold	SLO dashboard + burn-rate alerts
6. Invariants (must have automated tests)
Every journal entry sums to zero per asset.
No available balance is ever negative.
Sum of all user + fee + hot-wallet ledger accounts per asset equals deposits minus withdrawals (reconciler checks this every run).
A deposit (chain, txid, index) is credited at most once, even if the event is delivered N times or the watcher restarts mid-block.
A reorg that removes a not-yet-confirmed deposit never credits it; the system recovers automatically.
Replaying the matching command log from any snapshot yields the same events and the same final book hash.
Order submission with the same client_order_id is idempotent.
Total filled quantity of an order never exceeds its quantity; fees are computed with deterministic rounding toward the exchange.
7. Testing strategy
Unit tests for every package; the engine has table tests for every order type and TIF, a model-based test against a naive reference implementation on random command streams, and fuzz tests.
Integration tests per service with testcontainers (PostgreSQL, Kafka, Redis, RabbitMQ, ClickHouse, bitcoind, Anvil).
End-to-end suite in /e2e that runs against make up: sign up → verify email → deposit on regtest → wait for credit → trade between two users → candles appear → withdraw → confirmation.
Chaos scenarios in /e2e/chaos: kill matching mid-stream, duplicate every Kafka message, kill the outbox relay between commit and publish, force a 3-block reorg on regtest, make the SMTP server fail. Each scenario asserts the invariants in section 6.
Load: k6 scenarios smoke, steady (2k orders/s), spike (×3 for 2 minutes), ws-fanout (5k WS clients).
Coverage target 75%+ on domain packages; report in CI.
8. Observability
RED metrics for every gRPC/REST endpoint, USE metrics for queues and pools, business metrics: orders/s, trades/s, matching lag, deposit confirmation time, withdrawal queue size, reconciliation discrepancies.
One trace from REST POST /orders through orders → outbox → Kafka → matching → ledger settlement → WS push, with links across async hops.
Grafana dashboards (JSON in repo): Overview, Matching engine, Ledger, Chains, Withdrawals, Kafka/RabbitMQ, SLOs.
Alerts with runbooks: SLO burn rate (fast and slow), Kafka consumer lag, outbox backlog, reconciliation discrepancy > 0, chain watcher stalled, withdrawal queue stuck.
9. Phases and acceptance criteria
Each phase ends with a working, demoable state, a green CI, updated README and PROGRESS.md.

Phase 1. Skeleton and money. Monorepo, go.work, pkg/money with fuzz tests, Protobuf + buf, compose with PostgreSQL, Kafka, Redis, RabbitMQ, ClickHouse, observability stack, Mailpit. CI on GitHub Actions and GitLab. make up brings everything up healthy.

Accept: make up && make test && make lint green; Grafana and Jaeger reachable; ADR for money.
Phase 2. Matching engine as a library. Order book, all order types and TIF, self-trade prevention, deterministic events with sequence numbers, snapshot + replay, benchmarks, model-based and fuzz tests.

Accept: section 5 engine targets met and printed by make bench; invariant 6 and 8 tests pass; pprof profiles saved in docs/perf/engine/.
Phase 3. Identity, gateway, orders, ledger, matching as services. Signup/login/JWT/API keys, holds, outbox, Kafka wiring, settlement of trades in the ledger, cancel, REST API with OpenAPI spec.

Accept: two users can trade via REST against seeded balances; invariants 1, 2, 7 tested; duplicate-delivery chaos test passes; one end-to-end trace visible in Jaeger.
Phase 4. Market data and candles. marketdata with Redis snapshots and WS channels with sequence numbers and resync, candles with ClickHouse materialized views, private WS channels.

Accept: a WS client can rebuild an exact book from snapshot + deltas (test compares with engine state); candles for all intervals match trades; WS latency target met.
Phase 5. Deposits. bitcoind regtest + Anvil in compose with a test ERC-20 USDT, wallet xpub derivation, chainwatcher with confirmations and reorg handling, ledger credit.

Accept: invariants 4 and 5 tested, including a forced 3-block reorg on regtest; deposit credit target met.
Phase 6. Withdrawals and notifications. State machine, limits, TOTP, denylist screening, review queue via RabbitMQ, signer with mTLS, broadcast, confirmation tracking, notifier with retries and DLQ, admin approval.

Accept: e2e withdrawal on both chains; withdrawal above the threshold lands in the review queue and is executed only after admin approval; SMTP failure chaos test passes.
Phase 7. Reconciliation, SLOs, alerts. reconciler jobs, SLO definitions, burn-rate alerts, runbooks, all dashboards.

Accept: injecting a fake ledger discrepancy triggers an alert with a runbook link; invariant 3 checked on every run.
Phase 8. Kubernetes. Helm umbrella chart, kind cluster script, migrations as Jobs, secrets via Kubernetes Secrets (sealed-secrets optional), HPA for stateless services, PodDisruptionBudgets, readiness/liveness probes, make k8s-up.

Accept: the full e2e suite passes against the kind cluster.
Phase 9. Performance pass. k6 scenarios, pprof on gateway, orders, ledger; PostgreSQL tuning with EXPLAIN ANALYZE, partitioning, pgbouncer; results "before/after" in docs/perf/.

Accept: end-to-end order ack target met; numbers and graphs in README.
Phase 10. Web UI and admin. Trading terminal and admin pages over the public API and WS.

Accept: the e2e flow from Phase 6 can be done by hand in the browser; Playwright smoke test in CI.
Phase 11. Hardening and docs. Security review (authz on every endpoint, rate limits, input validation, secrets, dependency scan with govulncheck), README with architecture diagram, quickstart, demo script, ADR index, known limitations.

Accept: a new person can clone the repo, run make up, and complete the demo script in under 15 minutes following only the README.
10. Definition of done for the whole product
make up starts everything locally; make k8s-up starts it on kind; make demo runs a scripted demo (two bots trading, a deposit, a withdrawal) with dashboards showing activity.
All invariants, chaos tests, e2e and load targets pass in CI or are reproducible with one documented command.
README in English: what it is, architecture diagram, how to run, what to look at, benchmark and load results, limitations, ADR links.
No secrets in the repo, no mainnet, no floats in money paths, no skipped tests.
