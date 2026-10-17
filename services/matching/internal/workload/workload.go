// Package workload generates a realistic, allocation-free command mix for
// benchmarks: mostly passive adds near the touch, cancels of the oldest
// orders, and a small share of aggressive IOC orders that match.
package workload

import "github.com/tickless/matching/engine"

const ringSize = 1 << 16

type live struct{ id, user uint64 }

// Gen drives one engine with a deterministic pseudo-random command stream.
type Gen struct {
	E      *engine.Engine
	Buf    []engine.Event
	target int
	levels uint64
	mid    int64
	rng    uint64
	nextID uint64
	ring   [ringSize]live
	head   int
	size   int
	now    int64
}

// NewEngine returns an engine pre-sized for a generator with these parameters.
func NewEngine(target, levels int) *engine.Engine {
	return engine.NewSized("BTC-USDT", 3*target, levels+16)
}

// New creates a generator for e keeping roughly `target` orders resting over
// `levels` price levels per side around a mid price.
func New(e *engine.Engine, target, levels int) *Gen {
	return &Gen{E: e, target: target, levels: uint64(levels), mid: 1_000_000, rng: 0x9E3779B97F4A7C15, nextID: 1, Buf: make([]engine.Event, 0, 64)}
}

func (g *Gen) rand() uint64 { // xorshift64*
	g.rng ^= g.rng >> 12
	g.rng ^= g.rng << 25
	g.rng ^= g.rng >> 27
	return g.rng * 0x2545F4914F6CDD1D
}

func (g *Gen) process(c engine.Command) {
	g.now++
	c.Time = g.now
	g.Buf = g.E.Process(c, g.Buf[:0])
}

// Add submits one passive GTC limit order (never crosses the generated touch).
func (g *Gen) Add() {
	r := g.rand()
	side := engine.SideBuy
	price := g.mid - 1 - int64(r>>8%g.levels)
	if r&1 == 1 {
		side = engine.SideSell
		price = g.mid + 1 + int64(r>>8%g.levels)
	}
	user := r>>40%1000 + 1
	id := g.nextID
	g.nextID++
	g.process(engine.Command{Type: engine.CmdPlace, OrderID: id, UserID: user, Side: side, OrderType: engine.Limit, TIF: engine.GTC, Price: price, Qty: int64(r>>32%9) + 1})
	if g.size == ringSize { // drop the oldest tracking slot
		g.head = (g.head + 1) % ringSize
		g.size--
	}
	g.ring[(g.head+g.size)%ringSize] = live{id, user}
	g.size++
}

// Cancel cancels the oldest tracked order (it may already be filled, in which
// case the engine rejects it, which is also a legitimate hot-path command).
func (g *Gen) Cancel() {
	if g.size == 0 {
		g.Add()
		return
	}
	l := g.ring[g.head]
	g.head = (g.head + 1) % ringSize
	g.size--
	g.process(engine.Command{Type: engine.CmdCancel, OrderID: l.id, UserID: l.user})
}

// Aggress submits an IOC limit order that crosses the touch and trades.
func (g *Gen) Aggress() {
	r := g.rand()
	side := engine.SideBuy
	price := g.mid + 1 + int64(r>>8%4)
	if r&1 == 1 {
		side = engine.SideSell
		price = g.mid - 1 - int64(r>>8%4)
	}
	id := g.nextID
	g.nextID++
	g.process(engine.Command{Type: engine.CmdPlace, OrderID: id, UserID: r>>40%1000 + 1, Side: side, OrderType: engine.Limit, TIF: engine.IOC, Price: price, Qty: int64(r>>32%5) + 1})
}

// Step issues one command from the mix: 55% add, 35% cancel, 10% aggressive.
// The resting population is steered toward target.
func (g *Gen) Step() {
	x := g.rand() >> 16 % 100
	open := g.E.OpenOrders()
	switch {
	case open < g.target/2, x < 55 && open < g.target*2:
		g.Add()
	case x < 90 || open > g.target*2:
		g.Cancel()
	default:
		g.Aggress()
	}
}

// Fill adds orders until n are resting.
func (g *Gen) Fill(n int) {
	for g.E.OpenOrders() < n {
		g.Add()
	}
}

// Warmup runs n mixed steps so pools and tables reach steady-state size.
func (g *Gen) Warmup(n int) {
	for i := 0; i < n; i++ {
		g.Step()
	}
}
