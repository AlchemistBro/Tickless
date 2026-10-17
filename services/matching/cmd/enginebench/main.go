// Command enginebench measures the matching engine against the SPEC §5
// targets and exits non-zero when one is missed.
//
//	throughput  >= 1,000,000 commands/s (single market, in-process)
//	latency     p99 < 20 µs per command
//	allocations 0 allocs/op after warm-up
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"time"

	"github.com/tickless/matching/internal/workload"
)

const (
	targetOpsPerSec = 1_000_000
	targetP99       = 20 * time.Microsecond
)

func main() {
	ops := flag.Int("ops", 5_000_000, "commands for the throughput run")
	samples := flag.Int("samples", 2_000_000, "commands timed individually for the latency run")
	resting := flag.Int("resting", 20_000, "approximate resting orders in the book")
	cpuprof := flag.String("cpuprofile", "", "write CPU profile of the throughput run")
	memprof := flag.String("memprofile", "", "write allocation profile after the run")
	flag.Parse()

	newGen := func() *workload.Gen {
		g := workload.New(workload.NewEngine(*resting, 500), *resting, 500)
		g.Fill(*resting)
		g.Warmup(10 * *resting)
		return g
	}
	runtime.GOMAXPROCS(runtime.NumCPU())

	// --- throughput + allocations ---
	g := newGen()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	for i := 0; i < *ops; i++ {
		g.Step()
	}
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	opsPerSec := float64(*ops) / elapsed.Seconds()

	// --- optional profiles, taken in a separate run so the profiler's own
	// allocations cannot pollute the allocation count above ---
	if *cpuprof != "" {
		pg := newGen()
		f, err := os.Create(*cpuprof)
		check(err)
		check(pprof.StartCPUProfile(f))
		for i := 0; i < *ops; i++ {
			pg.Step()
		}
		pprof.StopCPUProfile()
		check(f.Close())
	}

	// --- latency histogram (each command timed individually) ---
	g = newGen()
	lat := make([]int64, *samples)
	for i := range lat {
		t0 := time.Now()
		g.Step()
		lat[i] = int64(time.Since(t0))
	}
	slices.Sort(lat)
	pct := func(p float64) time.Duration { return time.Duration(lat[int(float64(len(lat)-1)*p)]) }
	p50, p99, p999, max := pct(.50), pct(.99), pct(.999), time.Duration(lat[len(lat)-1])

	if *memprof != "" {
		f, err := os.Create(*memprof)
		check(err)
		check(pprof.Lookup("allocs").WriteTo(f, 0))
		check(f.Close())
	}
	if p50 == 0 {
		fmt.Println("WARNING: clock resolution too coarse to time single commands (typical on Windows); " +
			"latency below is unreliable. Run `make bench`, which uses a Linux container.")
	}
	fail := false
	row := func(name, got, want string, ok bool) {
		status := "PASS"
		if !ok {
			status, fail = "FAIL", true
		}
		fmt.Printf("%-14s %-22s target %-16s %s\n", name, got, want, status)
	}
	fmt.Printf("engine benchmark: %d commands, ~%d resting orders, GOOS=%s GOARCH=%s CPUs=%d %s\n",
		*ops, *resting, runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	row("throughput", fmt.Sprintf("%.2f M cmd/s", opsPerSec/1e6), ">= 1.00 M cmd/s", opsPerSec >= targetOpsPerSec)
	row("latency p99", p99.String(), "< 20µs", p99 < targetP99)
	row("allocs/op", fmt.Sprintf("%d in %d cmds", after.Mallocs-before.Mallocs, *ops), "0", after.Mallocs == before.Mallocs)
	fmt.Printf("latency        p50=%v p99=%v p99.9=%v max=%v (each sample includes ~30-50ns of time.Now overhead)\n", p50, p99, p999, max)
	if fail {
		os.Exit(1)
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
