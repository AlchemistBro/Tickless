SHELL := bash
COMPOSE := docker compose -f deploy/compose/docker-compose.yml
# Every Go module in go.work (list grows with services).
MODULES := $(shell sed -n 's|^\s*\./\(.*\)$$|\1|p; s|^use \./\(.*\)$$|\1|p' go.work)
# -race needs cgo + a C compiler; fall back to a golang container when absent (e.g. Windows).
HAS_CC := $(shell command -v gcc >/dev/null 2>&1 && echo 1)
GO_IMAGE := golang:1.27

.PHONY: help test test-race lint proto up down logs ps fuzz bench

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

lint:
	@for m in $(MODULES); do echo "== golangci-lint $$m"; (cd $$m && golangci-lint run ./...) || exit 1; done
	buf lint

proto:
	buf generate

fuzz:
	cd pkg && for f in $$(go test ./money -list '^Fuzz'); do case $$f in Fuzz*) go test ./money -run '^$$' -fuzz "^$$f\$$" -fuzztime 15s || exit 1;; esac; done

bench:
	@echo "no benchmarks yet (engine arrives in phase 2)"

up:
	$(COMPOSE) up -d --wait --remove-orphans
	bash scripts/wait-up.sh
	@echo; echo "Grafana http://localhost:3000 (admin/admin)  Jaeger http://localhost:16686"
	@echo "Prometheus http://localhost:9090  Mailpit http://localhost:8025  RabbitMQ http://localhost:15672 (tickless/tickless)"

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps
