# Architecture: as is (end of Phase 4) and to be (Phase 11)

Written as input for C4 diagrams. Levels: **1 Context**, **2 Containers**, **3 Components** (only for the services where it matters), plus **Deployment** and **Dynamic** (key flows). Every element below has a name, a technology and a responsibility, and every relationship has a protocol, so each table row maps to one box or one arrow. Items marked **(planned)** exist only in the to-be model (SPEC.md phases 5–11).

---

## 1. System context (C4 level 1)

### People
| Element | Description |
|---|---|
| Trader | Registers, funds the account, places/cancels orders, watches market data (browser or API bot). |
| Bot / API client | Same as a trader, authenticates with HMAC-signed API keys. |
| Administrator (planned UI, role exists) | Freezes users, halts markets, approves withdrawals, reads reconciliation reports. |

### The system
**Tickless**: a self-hosted spot crypto exchange (limit/market orders, double-entry ledger, market data, candles).

### External systems
| Element | As is | To be |
|---|---|---|
| Mail catcher (Mailpit) | running, unused (signup is auto-verified in dev) | receives verification, deposit, withdrawal, fill emails via the notifier |
| Bitcoin regtest node (bitcoind) | no | deposits and withdrawals for BTC |
| Ethereum devnet (Anvil) with test ERC-20 USDT | no | deposits and withdrawals for ETH/USDT |
| Observability backends (Prometheus, Jaeger, Grafana, Alertmanager) | running | same, plus SLO dashboards and runbook-linked alerts |

Relationships: Trader/Bot → Tickless (HTTPS REST, WebSocket); Tickless → mail catcher (SMTP, planned); Tickless ↔ chains (JSON-RPC, planned); Admin → Tickless (HTTPS, planned UI).

---

## 2. Containers (C4 level 2): as is

### 2.1 Application containers
| Container | Tech | Responsibility | Owns | Ports |
|---|---|---|---|---|
| **web** | React 18, TypeScript, Vite, nginx | Widget-based trading terminal. Static SPA; nginx proxies `/v1` and `/ws` to the gateway. Each panel (chart, book, trades, order entry, balances, open orders, activity, ticker) is one auto-discovered file. | browser `localStorage` (layout) | 8088 |
| **gateway** | Go, chi, coder/websocket | Only public entry point. REST API (OpenAPI), JWT (Ed25519) and HMAC API-key auth, per-IP/per-user rate limits, idempotency keys, frozen-user check (fail closed), email-verified check, reverse proxy for public market data, public and private WebSockets (private events fed from Kafka). | Redis keys (rate limit, idempotency, frozen set) | 8080, admin 9205 |
| **identity** | Go, gRPC, PostgreSQL | Users, argon2id passwords, sessions, refresh tokens, API keys, roles, freeze, bootstrap admin. Publishes `users.events` through an outbox. | DB `identity` | 9103, admin 9203 |
| **orders** | Go, gRPC, PostgreSQL | Order placement saga: validate against market rules, place a hold in the ledger, persist, write the engine command through an outbox, apply fill/cancel/reject events to order state. Markets config, client_order_id idempotency, price band, open-order limit, sweeper for stuck PENDING_HOLD. | DB `orders` | 9102, admin 9202 |
| **ledger** | Go, gRPC, PostgreSQL | Double-entry ledger: balanced journal entries, holds, trade settlement and fees (idempotent by business key), balances kept equal to postings by DB constraints, append-only triggers, settlement-failure table. Publishes `ledger.events` through an outbox. | DB `ledger` | 9101, admin 9201 |
| **matching** | Go (no DB) | One goroutine per partition owns the in-memory books; deterministic state machine; consumes `matching.commands`, emits `matching.events` with gap-free per-market `seq`; periodic checkpoints (offset + books + dedupe window) to a volume; refuses to start on log truncation or partition-count change. | volume `matchingdata` | admin 9204 |
| **marketdata** | Go, HTTP/WebSocket | Rebuilds books, trades, 24h ticker from `matching.events`; Redis snapshots for fast restart; REST `/v1/market/*`; public WS (`book`, `trades`, `ticker`, `candles`) with seq/prev gap detection; bounded queues, slow consumers dropped. | Redis `md:snap:*` | 8081, admin 9207 |
| **candles** | Go, gRPC, ClickHouse | Consumes trades, inserts idempotently into ClickHouse (6 materialized views for 1m…1d), serves `GetCandles`, publishes `candles.live`. | ClickHouse DB `tickless` | 9106, admin 9206 |

Every service also exposes an admin port with `/healthz`, `/readyz`, `/metrics`.

### 2.2 Data and messaging containers
| Container | Tech | Used for |
|---|---|---|
| PostgreSQL 16 | one server, one database per service | system of record for identity, orders, ledger (+ outbox, processed_events tables) |
| PgBouncer | transaction pooling | all service DB traffic goes through it (migrations connect directly) |
| Kafka (KRaft, single node) | franz-go clients | event log (`matching.commands/events` partitioned by market, infinite retention), domain events (`ledger.events`, `users.events`, `candles.live`) |
| Redis 7 | go-redis | rate-limit buckets, idempotency keys, frozen-user set, market-data snapshots |
| ClickHouse | clickhouse-go | trades + candle aggregates |
| RabbitMQ | running, unused | reserved for Phase 6 task queues |

### 2.3 Observability containers
OpenTelemetry Collector (OTLP from all services) → Jaeger (traces), Prometheus (metrics + alert rules) → Alertmanager; Grafana (dashboards provisioned from the repo). Logs: slog JSON on stdout with trace ids.

### 2.4 Relationships (as is)
Synchronous:
| From → To | Protocol | Purpose |
|---|---|---|
| web → gateway | HTTPS REST, WebSocket (via nginx) | everything the UI does |
| gateway → identity | gRPC | signup, login, refresh, API-key lookup, public key, frozen list |
| gateway → orders | gRPC | place/cancel/list orders, list markets |
| gateway → ledger | gRPC | balances, ledger entries |
| gateway → marketdata | HTTP reverse proxy (REST + WS) | public market data |
| gateway → Redis | RESP | rate limits, idempotency, frozen set |
| orders → ledger | gRPC | place/release hold |
| ledger → orders | gRPC | market specs (tick/lot, fees) via `ListMarkets` for settlement |
| marketdata → orders | gRPC | market specs (tick/lot) |
| marketdata → candles | gRPC | historical candles |
| marketdata → Redis | RESP | snapshots |
| candles → ClickHouse | native protocol | insert and query |
| every service → PostgreSQL (via PgBouncer) | pgx | own database |
| every service → OTel Collector | OTLP/gRPC | traces, metrics |

Asynchronous (Kafka), all at-least-once with idempotent consumers:
| Topic | Producer | Consumers |
|---|---|---|
| `matching.commands` | orders (outbox relay) | matching |
| `matching.events` | matching | ledger (group `ledger-settle`), orders (`orders-fills`), candles (`candles-ingest`), marketdata (own offsets), gateway private feed (live) |
| `ledger.events` | ledger (outbox) | gateway private feed |
| `users.events` | identity (outbox) | gateway (frozen set) |
| `candles.live` | candles | marketdata |

### 2.5 Component view of the services worth a C4 level 3
**matching**: Kafka consumer (per partition) → partition worker (owns engines) → Engine (book: sorted price levels, FIFO queues, order index; dedupe window; self-trade prevention) → event publisher; Checkpoint store (volume); startup verifier (log start vs checkpoint).

**orders**: gRPC server → placement saga (validator, hold client, order repository) → outbox → relay (advisory-lock single relay) → Kafka; fill consumer (applies events to orders, idempotent); sweeper; markets cache.

**ledger**: gRPC server (holds, balances, entries) → store (transactional journal, DB invariants); settlement consumer (`matching.events` → journal entries, fee booking, failure recorder); outbox relay → `ledger.events`.

**gateway**: router/middleware chain (request id, rate limit, authn JWT/HMAC, frozen check, idempotency) → handlers (gRPC clients) ; market-data proxy; public/private WebSocket endpoints; Kafka consumers (users.events, ledger+matching for private feed); OpenAPI contract.

**marketdata**: per-partition workers → pure state model (Book, trades ring, 24h ticker buckets, gap detection) → snapshot persister (Redis) → server (REST + WS hub from `pkg/wshub`); candles.live subscriber.

**web**: core (registry with `import.meta.glob`, layout store, Feed WebSocket client with reconnect/resubscribe, exact BigInt decimal math, order-book model) + widgets (one file each).

### 2.6 Shared libraries (not containers; draw as a boundary or omit)
`pkg/money` (Int128, fees, market spec), `kafka`, `outbox`, `idempotency`, `auth`, `seal`, `svc` (bootstrap, health, gRPC helpers), `otelx`, `pg`, `wshub`, `testkit`, generated protobuf.

---

## 3. Deployment view (as is)
Docker Compose project `tickless` on one host, one bridge network. Images: distroless, non-root, one Dockerfile with `SERVICE` build arg. Published ports: web 8088, gateway 8080, marketdata 8081, Postgres 5433, PgBouncer 6432, Redis 6379, Kafka 9092, ClickHouse 8123/9000, Grafana 3000, Prometheus 9090, Jaeger 16686, Mailpit 8025, RabbitMQ 15672, gRPC 9101–9106. Volumes: pgdata, kafkadata, chdata, rabbitdata, matchingdata, promdata, grafanadata. Dev-only switches are labelled in compose (`LEDGER_ALLOW_SEED`, `IDENTITY_AUTO_VERIFY`, ephemeral encryption key).

## 4. Key dynamic flows (use for C4 dynamic diagrams)
1. **Place order.** web → gateway (JWT, rate limit, idempotency) → orders.PlaceOrder → ledger.PlaceHold → orders commits order NEW + outbox row in one transaction → relay → `matching.commands` → matching emits `matching.events` (ACCEPTED, TRADE, BOOK_DELTA, …).
2. **Settlement.** `matching.events` → ledger (journal entries, fees, `ledger.events`) and → orders (fill state) in parallel; each consumer dedupes by `(market, seq)`.
3. **Market data.** `matching.events` → marketdata (state, Redis snapshot, WS deltas) and → candles (ClickHouse insert, `candles.live`) → marketdata → WS clients.
4. **Private feed.** `matching.events` + `ledger.events` → gateway → `/ws/private` of the owning user only.
5. **Restart recovery.** matching: checkpoint + replay from stored offset; marketdata: Redis snapshot + log; both yield identical state (tested).

---

## 5. To be (Phases 5–11)

### 5.1 New containers
| Container | Phase | Tech | Responsibility |
|---|---|---|---|
| **bitcoind** (regtest) | 5 | Bitcoin Core | BTC chain for deposits/withdrawals |
| **anvil** + test ERC-20 USDT | 5 | Foundry Anvil | ETH chain, token contract |
| **wallet** | 5 | Go, gRPC, PostgreSQL | Derives one deposit address per user per chain from an xpub (BIP32/44); never holds private keys |
| **chainwatcher** | 5 | Go, btcd, go-ethereum, PostgreSQL | Block scanners, stores block hash/parent hash, confirmation counting, reorg detection and rollback, emits `deposits.events` through an outbox; deposit identity `(chain, txid, index)` credited at most once |
| **withdrawals** | 6 | Go, gRPC, PostgreSQL | Withdrawal state machine, daily limits, TOTP check, address validation, denylist screening, auto-execute below threshold, review queue above, confirmation tracking, `withdrawals.events` |
| **signer** | 6 | Go, mTLS gRPC | Only holder of the hot-wallet key; signs requests from withdrawals only |
| **notifier** | 6 | Go, RabbitMQ, SMTP | Email templates, retry with backoff, DLQ |
| **reconciler** | 7 | Go, scheduled | Checks ledger invariants, ledger vs on-chain balances, ledger vs trades; writes reports, raises alerts |
| **admin** | 6–7 | Go, gRPC behind gateway | Back-office API (users, markets halt/resume, review queue, reconciliation, ledger of any account) |
| **web (extended)** | 10 | React | Login/verification flows, deposit address, withdrawals, history, admin pages |
| **k6 load generators** | 9 | k6 | steady, spike, ws-fanout scenarios |

### 5.2 Changed containers
- **ledger**: also consumes `deposits.events` (credit) and `withdrawals.events` (debit/reversal); gets hot-wallet and fee accounts for the reconciliation invariant.
- **identity**: TOTP secrets (encrypted), real email verification through notifier (removes `IDENTITY_AUTO_VERIFY`), admin role UI hooks.
- **gateway**: new routes for deposits, withdrawals, admin; proxies admin gRPC.
- **RabbitMQ**: becomes active (notification tasks, review tasks, delayed retries).
- **Postgres**: monthly partitioning of postings and orders with EXPLAIN ANALYZE evidence (Phase 9); new databases `wallet`, `chainwatcher`, `withdrawals`.
- **Observability**: dashboards Overview, Matching, Ledger, Chains, Withdrawals, Kafka/RabbitMQ, SLOs; burn-rate alerts with runbooks; outbox backlog, watcher stalled, reconciliation discrepancy, withdrawal queue stuck.

### 5.3 New topics and flows
| Topic | Producer | Consumers |
|---|---|---|
| `deposits.events` (detected, confirmed, reorged) | chainwatcher | ledger, notifier (via RabbitMQ task) |
| `withdrawals.events` | withdrawals | ledger, notifier |

- **Deposit**: wallet issues address → chainwatcher sees tx → N confirmations (BTC 2, ETH 12 on devnet) → `deposit.confirmed` → ledger credit (idempotent) → balance on `ledger.events` → UI. A reorg before the threshold never credits; after rollback the watcher rescans.
- **Withdrawal**: gateway → withdrawals (TOTP, limits) → ledger hold → screening → (auto | RabbitMQ review task → admin approval) → signer (mTLS) → broadcast → confirmations → final status → ledger settlement → notifier email.
- **Reconciliation**: reconciler queries ledger, ClickHouse trades and chain balances on a schedule; any discrepancy raises a Prometheus alert with a runbook link.

### 5.4 Deployment (to be)
Same containers on Kubernetes (kind) via a Helm umbrella chart with one sub-chart per service: migrations as Jobs, Kubernetes Secrets, HPA for stateless services (gateway, orders, marketdata, candles), PodDisruptionBudgets, readiness/liveness probes, `make k8s-up`; matching as a StatefulSet with a PVC for checkpoints. Full e2e suite runs against the cluster. CI: GitHub Actions + equivalent GitLab pipeline, adding image builds, `helm lint/template`, k6 smoke.

### 5.5 Known simplifications to show as notes on the diagrams
Single-node Kafka/Postgres/ClickHouse/Redis (no replication); candles live feed is best effort (REST history is exact); dev-only flags in compose; private WS authenticates with an access token only; RabbitMQ present but idle until Phase 6.
