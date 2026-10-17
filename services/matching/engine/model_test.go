package engine_test

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/tickless/matching/engine"
)

// model is a deliberately naive reference implementation: a flat slice of
// resting orders in arrival order, linear scans everywhere. It encodes the
// matching rules independently of the optimised engine so the two can be
// compared event-for-event on random streams.
type model struct {
	orders   []*mOrder
	seq      uint64
	tradeSeq uint64
}

type mOrder struct {
	id, user      uint64
	side          Side
	price, leaves int64
	qty           int64
}

func (m *model) emit(out []Event, ev Event) []Event {
	m.seq++
	ev.Seq = m.seq
	return append(out, ev)
}

func (m *model) find(id uint64) int {
	for i, o := range m.orders {
		if o.id == id {
			return i
		}
	}
	return -1
}

func (m *model) levelQty(side Side, price int64) int64 {
	var s int64
	for _, o := range m.orders {
		if o.side == side && o.price == price {
			s += o.leaves
		}
	}
	return s
}

// best returns the best price on a side, ok=false if empty.
func (m *model) best(side Side) (int64, bool) {
	var p int64
	ok := false
	for _, o := range m.orders {
		if o.side != side {
			continue
		}
		if !ok || (side == SideBuy && o.price > p) || (side == SideSell && o.price < p) {
			p, ok = o.price, true
		}
	}
	return p, ok
}

func (m *model) process(c Command, out []Event) []Event {
	rej := func(r Reason) []Event {
		return m.emit(out, Event{Time: c.Time, Type: EvRejected, Reason: r, Side: c.Side, OrderID: c.OrderID, UserID: c.UserID})
	}
	switch c.Type {
	case CmdCancel:
		i := m.find(c.OrderID)
		if i < 0 || m.orders[i].user != c.UserID {
			return rej(RejUnknownOrder)
		}
		o := m.orders[i]
		m.orders = append(m.orders[:i], m.orders[i+1:]...)
		out = m.emit(out, Event{Time: c.Time, Type: EvCancelled, Reason: CancelUser, Side: o.side, OrderID: o.id, UserID: o.user, Price: o.price, Qty: o.leaves})
		return m.emit(out, Event{Time: c.Time, Type: EvBookDelta, Side: o.side, Price: o.price, Qty: m.levelQty(o.side, o.price)})
	case CmdPlace:
	default:
		return rej(RejUnknownCommand)
	}

	// Validation (order matters: it decides the reported reason).
	if c.Side != SideBuy && c.Side != SideSell {
		return rej(RejInvalidSide)
	}
	if c.Qty <= 0 {
		return rej(RejInvalidQty)
	}
	tif := c.TIF
	switch c.OrderType {
	case Limit:
		if c.Price <= 0 {
			return rej(RejInvalidPrice)
		}
		if tif == 0 {
			tif = GTC
		}
	case Market:
		if c.Price < 0 {
			return rej(RejInvalidPrice)
		}
		if tif == 0 {
			tif = IOC
		}
		if tif == GTC || c.PostOnly {
			return rej(RejInvalidCombo)
		}
	default:
		return rej(RejInvalidCombo)
	}
	if tif < GTC || tif > FOK || (c.PostOnly && tif != GTC) {
		return rej(RejInvalidCombo)
	}
	if m.find(c.OrderID) >= 0 {
		return rej(RejDuplicateOrder)
	}

	buy := c.Side == SideBuy
	oppSide := SideBuy
	if buy {
		oppSide = SideSell
	}
	// ok(p): may the taker trade at price p?
	ok := func(p int64) bool {
		switch {
		case c.OrderType == Market && c.Price == 0:
			return true
		case buy:
			return p <= c.Price
		default:
			return p >= c.Price
		}
	}
	if c.PostOnly {
		if bp, has := m.best(oppSide); has && ok(bp) {
			return rej(RejPostOnlyWouldCross)
		}
	}
	out = m.emit(out, Event{Time: c.Time, Type: EvAccepted, Side: c.Side, OrderID: c.OrderID, UserID: c.UserID, Price: c.Price, Qty: c.Qty})

	if tif == FOK {
		// Walk opposite orders in price-time priority; stop at first own order.
		var sum int64
		for _, o := range m.priority(oppSide) {
			if !ok(o.price) || sum >= c.Qty {
				break
			}
			if o.user == c.UserID {
				break
			}
			sum += o.leaves
		}
		if sum < c.Qty {
			return m.emit(out, Event{Time: c.Time, Type: EvCancelled, Reason: CancelFOKUnfilled, Side: c.Side, OrderID: c.OrderID, UserID: c.UserID, Qty: c.Qty})
		}
	}

	leaves := c.Qty
	stp := false
	for leaves > 0 {
		bp, has := m.best(oppSide)
		if !has || !ok(bp) {
			break
		}
		changed := false
		var done []*mOrder
		for _, o := range m.orders {
			if o.side != oppSide || o.price != bp {
				continue
			}
			if leaves == 0 {
				break
			}
			if o.user == c.UserID {
				stp = true
				break
			}
			q := min(leaves, o.leaves)
			o.leaves -= q
			leaves -= q
			changed = true
			m.tradeSeq++
			out = m.emit(out, Event{
				Time: c.Time, Type: EvTrade, Side: c.Side, OrderID: c.OrderID, UserID: c.UserID,
				MakerOrderID: o.id, MakerUserID: o.user, Price: bp, Qty: q,
				TakerLeaves: leaves, MakerLeaves: o.leaves, TradeID: m.tradeSeq,
			})
			if o.leaves == 0 {
				done = append(done, o)
			}
		}
		for _, d := range done {
			m.orders = append(m.orders[:m.find(d.id)], m.orders[m.find(d.id)+1:]...)
		}
		if changed {
			out = m.emit(out, Event{Time: c.Time, Type: EvBookDelta, Side: oppSide, Price: bp, Qty: m.levelQty(oppSide, bp)})
		}
		if stp {
			break
		}
	}
	switch {
	case stp:
		out = m.emit(out, Event{Time: c.Time, Type: EvCancelled, Reason: CancelSelfTrade, Side: c.Side, OrderID: c.OrderID, UserID: c.UserID, Qty: leaves})
	case leaves > 0 && tif == GTC:
		m.orders = append(m.orders, &mOrder{id: c.OrderID, user: c.UserID, side: c.Side, price: c.Price, qty: c.Qty, leaves: leaves})
		out = m.emit(out, Event{Time: c.Time, Type: EvBookDelta, Side: c.Side, Price: c.Price, Qty: m.levelQty(c.Side, c.Price)})
	case leaves > 0:
		out = m.emit(out, Event{Time: c.Time, Type: EvCancelled, Reason: CancelIOCRemainder, Side: c.Side, OrderID: c.OrderID, UserID: c.UserID, Qty: leaves})
	}
	return out
}

// priority returns resting orders of a side in price-time priority.
func (m *model) priority(side Side) []*mOrder {
	var res []*mOrder
	for _, o := range m.orders {
		if o.side == side {
			res = append(res, o)
		}
	}
	// stable insertion sort by price (best first); arrival order kept for ties
	for i := 1; i < len(res); i++ {
		for j := i; j > 0; j-- {
			better := res[j].price < res[j-1].price
			if side == SideBuy {
				better = res[j].price > res[j-1].price
			}
			if !better {
				break
			}
			res[j], res[j-1] = res[j-1], res[j]
		}
	}
	return res
}

func (m *model) depth(side Side) []PriceLevel {
	var out []PriceLevel
	for _, o := range m.priority(side) {
		if n := len(out); n > 0 && out[n-1].Price == o.price {
			out[n-1].Qty += o.leaves
		} else {
			out = append(out, PriceLevel{Price: o.price, Qty: o.leaves})
		}
	}
	return out
}

// genCommands builds a random but deterministic command stream that
// exercises every order type, TIF, STP, duplicates, bad cancels and bad input.
func genCommands(seed int64, n int) []Command { return genCommandsOpt(seed, n, true) }

// genCommandsOpt optionally disables re-use of already used order ids.
func genCommandsOpt(seed int64, n int, dupIDs bool) []Command {
	r := rand.New(rand.NewSource(seed))
	cmds := make([]Command, 0, n)
	var nextID uint64 = 1
	for i := 0; i < n; i++ {
		var c Command
		switch k := r.Intn(100); {
		case k < 22: // cancel (often of a plausible id, sometimes wrong user / unknown)
			c = Command{Type: CmdCancel, OrderID: uint64(1 + r.Intn(int(nextID))), UserID: uint64(1 + r.Intn(5))}
		default:
			c = Command{
				Type: CmdPlace, OrderID: nextID, UserID: uint64(1 + r.Intn(5)),
				Side: Side(1 + r.Intn(2)), Price: int64(95 + r.Intn(11)), Qty: int64(1 + r.Intn(12)),
			}
			nextID++
			switch t := r.Intn(100); {
			case t < 55:
				c.OrderType, c.TIF = Limit, GTC
			case t < 70:
				c.OrderType, c.TIF = Limit, IOC
			case t < 80:
				c.OrderType, c.TIF = Limit, FOK
			case t < 88:
				c.OrderType, c.TIF, c.PostOnly = Limit, GTC, true
			case t < 96:
				c.OrderType = Market
				c.TIF = []TIF{0, IOC, FOK}[r.Intn(3)]
				if r.Intn(2) == 0 {
					c.Price = 0 // unbounded
				}
			default: // garbage
				c.OrderType = OrderType(r.Intn(4))
				c.TIF = TIF(r.Intn(5))
				c.Qty -= int64(r.Intn(3))
				c.Price -= int64(r.Intn(100))
				c.PostOnly = r.Intn(2) == 0
			}
			if dupIDs && r.Intn(40) == 0 && nextID > 2 {
				c.OrderID = uint64(1 + r.Intn(int(nextID-1))) // duplicate id
			}
		}
		c.Time = int64(i + 1)
		cmds = append(cmds, c)
	}
	return cmds
}

// runBoth feeds the stream to the engine and the model and compares everything.
func runBoth(t testing.TB, cmds []Command) (*Engine, []Event) {
	e, m := New("M"), &model{}
	var all, buf, mbuf []Event
	for i, c := range cmds {
		buf = e.Process(c, buf[:0])
		mbuf = m.process(c, mbuf[:0])
		if !assert.Equal(t, mbuf, buf, "event mismatch at command %d: %+v", i, c) {
			t.FailNow()
		}
		if err := e.CheckInvariants(); err != nil {
			t.Fatalf("command %d %+v: %v", i, c, err)
		}
		all = append(all, buf...)
	}
	require.Equal(t, m.depth(SideBuy), e.Depth(SideBuy, 1<<20, nil))
	require.Equal(t, m.depth(SideSell), e.Depth(SideSell, 1<<20, nil))
	return e, all
}

func TestEngineMatchesNaiveModel(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		runBoth(t, genCommands(seed, 3000))
	}
}

// Invariant (SPEC §6.8): total filled quantity never exceeds order quantity.
// Stronger: for every accepted order, filled + cancelled + still resting == quantity.
func TestFilledNeverExceedsQuantityAndConservation(t *testing.T) {
	for seed := int64(100); seed < 110; seed++ {
		cmds := genCommandsOpt(seed, 4000, false)
		e, evs := runBoth(t, cmds)
		qty := map[uint64]int64{}
		filled := map[uint64]int64{}
		cancelled := map[uint64]int64{}
		for _, ev := range evs {
			switch ev.Type {
			case EvAccepted:
				qty[ev.OrderID] = ev.Qty
			case EvTrade:
				filled[ev.OrderID] += ev.Qty
				filled[ev.MakerOrderID] += ev.Qty
			case EvCancelled:
				cancelled[ev.OrderID] += ev.Qty
			}
		}
		// Per-order: filled+cancelled <= qty; the rest must be resting in the book.
		var open int64
		for id, q := range qty {
			require.LessOrEqual(t, filled[id]+cancelled[id], q, "order %d overfilled", id)
			open += q - filled[id] - cancelled[id]
		}
		var inBook int64
		for _, side := range []Side{SideBuy, SideSell} {
			for _, l := range e.Depth(side, 1<<20, nil) {
				inBook += l.Qty
			}
		}
		assert.Equal(t, open, inBook, "seed %d: quantity conserved across fills/cancels/book", seed)
	}
}

// Invariant (SPEC §6.6): replaying the command log from ANY snapshot yields the
// same events and the same final state hash as an uninterrupted run.
func TestSnapshotReplayEqualsUninterruptedRun(t *testing.T) {
	cmds := genCommands(777, 2500)

	ref := New("M")
	var refEvents []Event
	bounds := make([]int, 0, len(cmds)+1) // bounds[i] = len(refEvents) before command i
	for _, c := range cmds {
		bounds = append(bounds, len(refEvents))
		refEvents = ref.Process(c, refEvents)
	}
	bounds = append(bounds, len(refEvents))
	refHash := ref.StateHash()

	cuts := []int{0, 1, 2, len(cmds) - 1, len(cmds)}
	for k := 3; k < len(cmds); k += 41 {
		cuts = append(cuts, k)
	}
	for _, k := range cuts {
		e := New("M")
		var buf []Event
		for _, c := range cmds[:k] {
			buf = e.Process(c, buf[:0])
		}
		snap := e.Snapshot(int64(k))
		restored, off, err := Restore(snap)
		require.NoError(t, err, "cut %d", k)
		require.Equal(t, int64(k), off)
		require.Equal(t, e.StateHash(), restored.StateHash(), "cut %d: restore changed state", k)
		require.NoError(t, restored.CheckInvariants())

		var replay []Event
		for _, c := range cmds[off:] {
			replay = restored.Process(c, replay)
		}
		if want := refEvents[bounds[k]:]; len(want) != 0 || len(replay) != 0 {
			require.Equal(t, want, replay, "cut %d: events differ", k)
		}
		require.Equal(t, refHash, restored.StateHash(), "cut %d: final hash differs", k)
	}
}

func TestSnapshotDeterministic(t *testing.T) {
	a, _ := runBoth(t, genCommands(5, 1500))
	b, _ := runBoth(t, genCommands(5, 1500))
	assert.Equal(t, a.Snapshot(9), b.Snapshot(9))
	assert.Equal(t, a.StateHash(), b.StateHash())
}

func TestRestoreRejectsCorruption(t *testing.T) {
	e, _ := runBoth(t, genCommands(9, 500))
	snap := e.Snapshot(42)
	_, _, err := Restore(snap)
	require.NoError(t, err)

	for i := 0; i < len(snap); i += 7 {
		bad := append([]byte(nil), snap...)
		bad[i] ^= 0xff
		_, _, err := Restore(bad)
		assert.ErrorIs(t, err, ErrCorruptSnapshot, "flip byte %d", i)
	}
	for _, n := range []int{0, 3, 10, len(snap) - 1} {
		_, _, err := Restore(snap[:n])
		assert.ErrorIs(t, err, ErrCorruptSnapshot, "truncate to %d", n)
	}
}

// decode turns arbitrary bytes into a command stream (7 bytes per command).
func decode(data []byte) []Command {
	var cmds []Command
	for i := 0; i+7 <= len(data) && len(cmds) < 4000; i += 7 {
		b := data[i : i+7]
		c := Command{
			OrderID: uint64(b[1] % 40), UserID: uint64(b[2]%4) + 1,
			Side: Side(b[3] % 4), Price: int64(b[4]%20) - 2, Qty: int64(b[5]%10) - 1,
			Time: int64(len(cmds)),
		}
		switch b[0] % 8 {
		case 0, 1:
			c.Type = CmdCancel
		default:
			c.Type = CmdPlace
		}
		c.OrderType = OrderType(b[6]&3) % 4
		c.TIF = TIF(b[6] >> 2 & 3)
		c.PostOnly = b[6]>>4&1 == 1
		if b[0]&0x80 != 0 {
			c.Type = CmdType(b[0] >> 4 & 3) // includes invalid types
		}
		cmds = append(cmds, c)
	}
	return cmds
}

// FuzzEngineAgainstModel: any byte string is a command stream; engine and
// naive model must agree event-for-event, invariants must hold, and a
// snapshot taken midway must replay to the same state.
func FuzzEngineAgainstModel(f *testing.F) {
	f.Add([]byte{2, 1, 1, 1, 10, 5, 4, 2, 2, 2, 2, 10, 3, 4, 3, 3, 3, 1, 10, 9, 8})
	f.Add([]byte{})
	for seed := int64(1); seed <= 4; seed++ {
		var b []byte
		for _, c := range genCommands(seed, 60) {
			b = append(b, byte(c.Type), byte(c.OrderID), byte(c.UserID), byte(c.Side), byte(c.Price%18+2), byte(c.Qty+1), byte(c.OrderType)|byte(c.TIF)<<2)
		}
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		cmds := decode(data)
		e, _ := runBoth(t, cmds)

		mid := len(cmds) / 2
		a := New("M")
		var buf []Event
		for _, c := range cmds[:mid] {
			buf = a.Process(c, buf[:0])
		}
		r, _, err := Restore(a.Snapshot(int64(mid)))
		require.NoError(t, err)
		for _, c := range cmds[mid:] {
			buf = r.Process(c, buf[:0])
		}
		require.Equal(t, e.StateHash(), r.StateHash())
	})
}

// FuzzRestore: the snapshot parser must never panic and must only accept
// snapshots that satisfy all book invariants.
func FuzzRestore(f *testing.F) {
	e, _ := runBoth(f, genCommands(3, 200))
	f.Add(e.Snapshot(1))
	f.Add([]byte("TLSN"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		r, _, err := Restore(data)
		if err != nil {
			return
		}
		require.NoError(t, r.CheckInvariants())
	})
}
