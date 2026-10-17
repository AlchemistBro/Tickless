SHELL := bash
COMPOSE := docker compose -f deploy/compose/docker-compose.yml
# Every Go module in go.work (list grows with services).
MODULES := $(shell sed -n 's|^\s*\./\(.*\)$$|\1|p; s|^use \./\(.*\)$$|\1|p' go.work)
# -race needs cgo + a C compiler; fall back to a golang container when absent (e.g. Windows).
HAS_CC := $(shell command -v gcc >/dev/null 2>&1 && echo 1)
GO_IMAGE := golang:1.27

.PHONY: help test test-integration lint lint-alerts proto up down logs ps fuzz bench e2e chaos-dup seed web-install web-test web-build web-dev

help:
	@grep -E '^[a-z-]+:' Makefile | cut -d: -f1 | tr '\n' ' '; echo

test:
ifeq ($(HAS_CC),1)
	@for m in $(MODULES); do echo "== $$m"; (cd $$m && CGO_ENABLED=1 go test -race -cover ./...) || exit 1; done
else
	@echo "gcc not found: running -race inside $(GO_IMAGE)"
	@MSYS_NO_PATHCONV=1 docker run --rm -v "$(CURDIR):/src" -v tickless-gomod:/go/pkg/mod -v tickless-gobuild:/root/.cache/go-build -w /src $(GO_IMAGE) \
		bash -c 'for m in $(MODULES); do echo "== $$m"; (cd $$m && go test -race -cover ./...) || exit 1; done'
endif

# Integration tests start throwaway PostgreSQL/Kafka containers (testcontainers); they need Docker
# access, so they run on the host rather than inside the -race container.
test-integration:
	@for m in $(MODULES); do echo "== integration $$m"; (cd $$m && go test -tags integration -cover -count=1 -timeout 15m ./...) || exit 1; done

lint:
	@rc=0; for m in $(MODULES); do echo "== golangci-lint $$m"; (cd $$m && golangci-lint run ./...) || rc=1; done; 	 buf lint || rc=1; "$(MAKE)" --no-print-directory lint-alerts || rc=1; exit $$rc

# Validate Prometheus alert rules with promtool (runs in a container).
lint-alerts:
	@MSYS_NO_PATHCONV=1 docker run --rm --entrypoint promtool -v "$(CURDIR)/deploy/observability/prometheus:/p:ro" prom/prometheus:v3.1.0 check rules /p/rules/kafka.yml /p/rules/infra.yml

proto:
	buf generate

fuzz:
	cd pkg && for f in $$(go test ./money -list '^Fuzz' | grep '^Fuzz'); do go test ./money -run '^$$' -fuzz "^$$f\$$" -fuzztime 15s || exit 1; done
	cd services/matching && for f in FuzzEngineAgainstModel FuzzRestore; do go test ./engine -run '^$$' -fuzz "^$$f\$$" -fuzztime 20s || exit 1; done

# Engine benchmarks print PASS/FAIL against SPEC §5 and save pprof profiles.
# Run on Linux (container on Windows): Windows' clock is too coarse to time single commands.
BENCH_CMD := cd services/matching && go run ./cmd/enginebench -cpuprofile ../../docs/perf/engine/cpu.pprof -memprofile ../../docs/perf/engine/allocs.pprof && go test ./engine -run '^$$' -bench . -benchmem -benchtime 2s | tee ../../docs/perf/engine/bench.txt
bench:
ifeq ($(OS),Windows_NT)
	@MSYS_NO_PATHCONV=1 docker run --rm -v "$(CURDIR):/src" -v tickless-gomod:/go/pkg/mod -v tickless-gobuild:/root/.cache/go-build -w /src $(GO_IMAGE) bash -c "$(BENCH_CMD)"
else
	@$(BENCH_CMD)
endif

up:
	$(COMPOSE) up -d --build --wait --remove-orphans
	bash scripts/wait-up.sh
	@echo; echo "Grafana http://localhost:3000 (admin/admin)  Jaeger http://localhost:16686"
	@echo "Prometheus http://localhost:9090  Mailpit http://localhost:8025  RabbitMQ http://localhost:15672 (tickless/tickless)"

# Dev only: give a registered user funds (no deposits exist before Phase 5).
#   make seed EMAIL=me@example.com ASSET=USDT AMOUNT=100000
seed:
	cd e2e && go run ./cmd/seed -email "$(EMAIL)" -asset "$(or $(ASSET),USDT)" -amount "$(or $(AMOUNT),10000)"

# Web terminal (React + TypeScript + Vite). `make up` also serves a production build on :8088.
web-install:
	cd web && npm ci --no-audit --no-fund
web-test: web-install
	cd web && npm run typecheck && npm test
web-build: web-install
	cd web && npm run build
web-dev: web-install
	cd web && npm run dev

# Black-box end-to-end suite against a running `make up` stack.
e2e:
	cd e2e && go test -tags e2e -count=1 -timeout 15m -v ./...

# Chaos: every Kafka message is published twice by every producer; the same trading scenarios must
# still end with identical balances and intact ledger invariants (SPEC §7).
chaos-dup:
	$(COMPOSE) down -v
	CHAOS_DUPLICATE_PUBLISH=true "$(MAKE)" up
	cd e2e && E2E_EXPECT_DUPLICATES=true go test -tags e2e -count=1 -timeout 15m -v -run 'TestTradeFlow|TestOrderTypesReleaseHolds|TestPublicMarketDataFlowsThroughTheGateway' ./... ; rc=$$?; cd .. ; $(COMPOSE) down -v; exit $$rc

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps
