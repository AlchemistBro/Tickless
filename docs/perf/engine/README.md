# Matching engine performance

Reproduce: `make bench` (Linux container on Windows hosts; native on Linux). Hardware: Intel Core Ultra 7 155H, 22 logical CPUs, Docker Desktop/WSL2, Go 1.27.2.

| Metric (SPEC §5) | Target | Measured | Source |
|---|---|---|---|
| Throughput, mixed add/cancel/match, ~20k resting orders | ≥ 1M cmd/s | ~6.2–6.9 M cmd/s | `enginebench` |
| Latency p99 per command | < 20 µs | ~0.45–0.5 µs (p99.9 ≈ 0.9 µs) | `enginebench` (per-command timing) |
| Allocations after warm-up | 0 | 0 mallocs in 5,000,000 commands; 0 allocs/op in all benchmarks | `enginebench`, `-benchmem` |

Files:
- `bench.txt` — `go test -bench -benchmem` output (includes the 100k-level worst case, see ADR 0002).
- `cpu.pprof`, `cpu_top.txt` — CPU profile of 5M mixed commands (`go tool pprof -top cpu.pprof`). Hot spots: order-index lookups (cache misses), event emission, level binary search.
- `allocs.pprof`, `allocs_top.txt` — allocation profile; allocations are warm-up only (slab/pool creation, index growth), none in the measured window.

Caveat: each latency sample includes ~30–50 ns of `time.Now` overhead, so real latency is slightly lower. Windows' clock is too coarse to time single commands, hence the Linux container.
