#!/usr/bin/env bash
# Waits until every HTTP endpoint that has no container healthcheck is ready.
# (Compose `--wait` already covers services with healthchecks.)
set -euo pipefail
probe() {
  local name=$1 url=$2
  for _ in $(seq 1 60); do
    if curl -fsS -o /dev/null "$url"; then echo "ok   $name"; return 0; fi
    sleep 1
  done
  echo "FAIL $name ($url)" >&2; return 1
}
probe otel-collector http://localhost:13133/
probe jaeger         http://localhost:16686/
probe grafana        http://localhost:13000/api/health
probe prometheus     http://localhost:9090/-/ready
probe mailpit        http://localhost:18025/livez
