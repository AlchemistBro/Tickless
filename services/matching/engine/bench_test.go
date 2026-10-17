package engine_test

import (
	"testing"

	"github.com/tickless/matching/engine"

	"github.com/tickless/matching/internal/workload"
)

func newGen(b *testing.B, target int) *workload.Gen {
	b.Helper()
	g := workload.New(workload.NewEngine(target, 500), target, 500)
	g.Fill(target)
	g.Warmup(4 * target)
	return g
}

// BenchmarkMixed is the headline number: 55% add, 35% cancel, 10% matching IOC,
// ~20k resting orders. ns/op is per command; expect 0 allocs/op.
func BenchmarkMixed(b *testing.B) {
	g := newGen(b, 20_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Step()
	}
}

func BenchmarkAdd(b *testing.B) {
	g := newGen(b, 20_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Add()
		g.Cancel() // keeps the population bounded; ns/op covers one add+one cancel
	}
}

func BenchmarkMatch(b *testing.B) {
	g := newGen(b, 20_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Aggress()
		for g.E.OpenOrders() < 20_000 {
			g.Add() // replenish liquidity (cost included, keeps the book at steady state)
		}
	}
}

// BenchmarkSnapshot measures snapshot cost for a 20k order book.
func BenchmarkSnapshot(b *testing.B) {
	g := newGen(b, 20_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.E.Snapshot(int64(i))
	}
}

// BenchmarkDeepBookRandomInsert is the sorted-slice worst case (ADR 0002):
// 100k levels per side and inserts at uniformly random depths, which forces a
// long memmove each time. Real books are far shallower and insert near the touch.
func BenchmarkDeepBookRandomInsert(b *testing.B) {
	const levels = 100_000
	e := engine.NewSized("BTC-USDT", 4*levels, levels+16)
	var buf []engine.Event
	for i := 0; i < levels; i++ {
		buf = e.Process(engine.Command{Type: engine.CmdPlace, OrderID: uint64(i + 1), UserID: 1, Side: engine.SideBuy, OrderType: engine.Limit, TIF: engine.GTC, Price: int64(2 * (i + 1)), Qty: 1}, buf[:0])
	}
	id := uint64(levels + 1)
	rng := uint64(88172645463325252)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rng ^= rng << 13
		rng ^= rng >> 7
		rng ^= rng << 17
		price := int64(2*(rng%levels)) + 1 // odd => always a new level
		buf = e.Process(engine.Command{Type: engine.CmdPlace, OrderID: id, UserID: 2, Side: engine.SideBuy, OrderType: engine.Limit, TIF: engine.GTC, Price: price, Qty: 1}, buf[:0])
		buf = e.Process(engine.Command{Type: engine.CmdCancel, OrderID: id, UserID: 2}, buf[:0])
		id++
	}
}
