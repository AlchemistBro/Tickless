# Runbook: TargetDown

**Alert:** `TargetDown` — a Prometheus scrape target has been unreachable for 1 minute.

1. Open Prometheus → Status → Targets (`:9090/targets`) and find the failing job/instance.
2. `docker compose -f deploy/compose/docker-compose.yml ps` — is the container running/healthy?
3. `docker compose ... logs --tail=100 <service>` for crash reasons.
4. If the service is up but the scrape fails, check the port in `deploy/observability/prometheus/prometheus.yml`.
5. Restart: `docker compose ... restart <service>`. Escalate if it flaps.
