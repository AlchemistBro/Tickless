package engine

import "fmt"

// Engine is the order book and matching state of one market. It is NOT safe
// for concurrent use: exactly one goroutine owns it (ADR 0002).
type Engine struct {
	market   string
	seq      uint64
	tradeSeq uint64
	bids     bookSide
	asks     bookSide
	idx      orderIndex
	pool     pools
}

// New returns an empty engine for a market.
func New(market string) *Engine {
	return &Engine{
		market: market,
		bids:   bookSide{side: SideBuy},
		asks:   bookSide{side: SideSell},
		idx:    newOrderIndex(0),
	}
}

// NewSized returns an empty engine with pools, index and level slices
// pre-allocated for maxOrders resting orders and maxLevels levels per side, so
// that the hot path performs no allocation until those bounds are exceeded
// (after which it grows, still correctly).
func NewSized(market string, maxOrders, maxLevels int) *Engine {
	e := New(market)
	e.idx = newOrderIndex(maxOrders)
	e.pool.reserve(maxOrders, 2*maxLevels)
	e.bids.levels = make([]*level, 0, maxLevels)
	e.asks.levels = make([]*level, 0, maxLevels)
	return e
}

// Market returns the market symbol.
func (e *Engine) Market() string { return e.market }

// Seq returns the sequence number of the last emitted event.
func (e *Engine) Seq() uint64 { return e.seq }

// OpenOrders returns the number of resting orders.
func (e *Engine) OpenOrders() int { return e.idx.count }

// Process applies one command and appends the resulting events to out,
// returning the extended slice. Reusing out[:0] between calls keeps the hot
// path allocation-free. Determinism contract: the events depend only on the
// engine state and the command.
func (e *Engine) Process(c Command, out []Event) []Event {
	switch c.Type {
	case CmdPlace:
		return e.place(&c, out)
	case CmdCancel:
		return e.cancel(&c, out)
	default:
		return e.reject(out, &c, RejUnknownCommand)
	}
}

func (e *Engine) emit(out []Event, ev Event) []Event {
	e.seq++
	ev.Seq = e.seq
	return append(out, ev)
}

func (e *Engine) reject(out []Event, c *Command, r Reason) []Event {
	return e.emit(out, Event{
		Time: c.Time, Type: EvRejected, Reason: r,
		Side: c.Side, OrderID: c.OrderID, UserID: c.UserID,
	})
}

// normalize validates a place command and resolves the default TIF.
func normalize(c *Command) (TIF, Reason) {
	if c.Side != SideBuy && c.Side != SideSell {
		return 0, RejInvalidSide
	}
	if c.Qty <= 0 {
		return 0, RejInvalidQty
	}
	tif := c.TIF
	switch c.OrderType {
	case Limit:
		if c.Price <= 0 {
			return 0, RejInvalidPrice
		}
		if tif == 0 {
			tif = GTC
		}
	case Market:
		if c.Price < 0 {
			return 0, RejInvalidPrice
		}
		if tif == 0 {
			tif = IOC
		}
		if tif == GTC || c.PostOnly {
			return 0, RejInvalidCombo
		}
	default:
		return 0, RejInvalidCombo
	}
	if tif < GTC || tif > FOK {
		return 0, RejInvalidCombo
	}
	if c.PostOnly && tif != GTC {
		return 0, RejInvalidCombo
	}
	return tif, ReasonNone
}

// priceOK reports whether a taker on side with the given limit may trade at levelPrice.
func priceOK(side Side, limit, levelPrice int64) bool {
	if side == SideBuy {
		return levelPrice <= limit
	}
	return levelPrice >= limit
}

func (e *Engine) place(c *Command, out []Event) []Event {
	tif, rej := normalize(c)
	if rej != ReasonNone {
		return e.reject(out, c, rej)
	}
	if e.idx.get(c.OrderID) != nil {
		return e.reject(out, c, RejDuplicateOrder)
	}
	own, opp := &e.bids, &e.asks
	limit := c.Price
	if c.Side == SideSell {
		own, opp = &e.asks, &e.bids
	} else if c.OrderType == Market && limit == 0 {
		limit = maxPrice // unbounded market buy
	}
	// A market sell with bound 0 trades at any price >= 0, i.e. any price.

	if c.PostOnly {
		if b := opp.best(); b != nil && priceOK(c.Side, limit, b.price) {
			return e.reject(out, c, RejPostOnlyWouldCross)
		}
	}

	out = e.emit(out, Event{
		Time: c.Time, Type: EvAccepted, Side: c.Side,
		OrderID: c.OrderID, UserID: c.UserID, Price: c.Price, Qty: c.Qty,
	})

	if tif == FOK && fillable(c, opp, limit) < c.Qty {
		return e.emit(out, Event{
			Time: c.Time, Type: EvCancelled, Reason: CancelFOKUnfilled, Side: c.Side,
			OrderID: c.OrderID, UserID: c.UserID, Qty: c.Qty,
		})
	}

	leaves := c.Qty
	selfTrade := false
	for leaves > 0 {
		lvl := opp.best()
		if lvl == nil || !priceOK(c.Side, limit, lvl.price) {
			break
		}
		changed := false
		for o := lvl.head; o != nil && leaves > 0; {
			if o.user == c.UserID {
				selfTrade = true // cancel newest: the incoming order stops here
				break
			}
			q := min(leaves, o.leaves)
			o.leaves -= q
			leaves -= q
			lvl.qty -= q
			changed = true
			e.tradeSeq++
			out = e.emit(out, Event{
				Time: c.Time, Type: EvTrade, Side: c.Side,
				OrderID: c.OrderID, UserID: c.UserID,
				MakerOrderID: o.id, MakerUserID: o.user,
				Price: lvl.price, Qty: q,
				TakerLeaves: leaves, MakerLeaves: o.leaves, TradeID: e.tradeSeq,
			})
			next := o.next
			if o.leaves == 0 {
				lvl.unlink(o)
				e.idx.del(o.id)
				e.pool.putOrder(o)
			}
			o = next
		}
		price, total := lvl.price, lvl.qty
		if changed {
			out = e.emit(out, Event{Time: c.Time, Type: EvBookDelta, Side: opp.side, Price: price, Qty: total})
		}
		if total == 0 {
			opp.removeAt(len(opp.levels) - 1)
			e.pool.putLevel(lvl)
		}
		if selfTrade {
			break
		}
	}

	switch {
	case selfTrade:
		out = e.emit(out, Event{
			Time: c.Time, Type: EvCancelled, Reason: CancelSelfTrade, Side: c.Side,
			OrderID: c.OrderID, UserID: c.UserID, Qty: leaves,
		})
	case leaves > 0 && tif == GTC:
		out = e.rest(c, own, leaves, out)
	case leaves > 0:
		out = e.emit(out, Event{
			Time: c.Time, Type: EvCancelled, Reason: CancelIOCRemainder, Side: c.Side,
			OrderID: c.OrderID, UserID: c.UserID, Qty: leaves,
		})
	}
	return out
}

// fillable returns how much of c could be filled immediately, stopping at the
// first self-trade conflict or when c.Qty is reached. Used for FOK.
func fillable(c *Command, opp *bookSide, limit int64) int64 {
	var sum int64
	for i := len(opp.levels) - 1; i >= 0; i-- {
		lvl := opp.levels[i]
		if !priceOK(c.Side, limit, lvl.price) {
			break
		}
		for o := lvl.head; o != nil; o = o.next {
			if o.user == c.UserID {
				return sum
			}
			sum += o.leaves
			if sum >= c.Qty {
				return sum
			}
		}
	}
	return sum
}

// rest places the unfilled remainder of a GTC limit order on its own side.
func (e *Engine) rest(c *Command, own *bookSide, leaves int64, out []Event) []Event {
	o := e.pool.getOrder()
	o.id, o.user, o.side = c.OrderID, c.UserID, c.Side
	o.price, o.qty, o.leaves, o.ts = c.Price, c.Qty, leaves, c.Time
	lvl := e.levelFor(own, c.Price)
	lvl.push(o)
	e.idx.put(o)
	return e.emit(out, Event{Time: c.Time, Type: EvBookDelta, Side: own.side, Price: c.Price, Qty: lvl.qty})
}

// levelFor finds or creates the level at price on a side.
func (e *Engine) levelFor(s *bookSide, price int64) *level {
	key := s.keyOf(price)
	i := s.search(key)
	if i < len(s.levels) && s.levels[i].key == key {
		return s.levels[i]
	}
	l := e.pool.getLevel()
	l.price, l.key = price, key
	s.insertAt(i, l)
	return l
}

func (e *Engine) cancel(c *Command, out []Event) []Event {
	o := e.idx.get(c.OrderID)
	if o == nil || o.user != c.UserID {
		return e.reject(out, c, RejUnknownOrder)
	}
	lvl := o.lvl
	side, price, leaves := o.side, o.price, o.leaves
	lvl.unlink(o)
	lvl.qty -= leaves
	e.idx.del(o.id)
	e.pool.putOrder(o)
	out = e.emit(out, Event{
		Time: c.Time, Type: EvCancelled, Reason: CancelUser, Side: side,
		OrderID: c.OrderID, UserID: c.UserID, Price: price, Qty: leaves,
	})
	s := &e.bids
	if side == SideSell {
		s = &e.asks
	}
	total := lvl.qty
	if total == 0 {
		s.removeAt(s.search(lvl.key))
		e.pool.putLevel(lvl)
	}
	return e.emit(out, Event{Time: c.Time, Type: EvBookDelta, Side: side, Price: price, Qty: total})
}

// PriceLevel is an aggregated level for depth queries.
type PriceLevel struct {
	Price int64
	Qty   int64
}

// Depth appends up to n best levels of a side (best first) to dst.
func (e *Engine) Depth(side Side, n int, dst []PriceLevel) []PriceLevel {
	s := &e.bids
	if side == SideSell {
		s = &e.asks
	}
	for i := len(s.levels) - 1; i >= 0 && n > 0; i, n = i-1, n-1 {
		dst = append(dst, PriceLevel{Price: s.levels[i].price, Qty: s.levels[i].qty})
	}
	return dst
}

// CheckInvariants verifies the structural invariants of the book. It is O(n)
// and meant for tests, fuzzing and post-restore validation.
func (e *Engine) CheckInvariants() error {
	total := 0
	for _, s := range []*bookSide{&e.bids, &e.asks} {
		for i, l := range s.levels {
			if i > 0 && s.levels[i-1].key >= l.key {
				return fmt.Errorf("side %d: levels not strictly sorted at %d", s.side, i)
			}
			if l.key != s.keyOf(l.price) || l.price <= 0 {
				return fmt.Errorf("side %d: bad level key/price %d/%d", s.side, l.key, l.price)
			}
			if l.head == nil {
				return fmt.Errorf("side %d: empty level at price %d", s.side, l.price)
			}
			var sum int64
			var prev *order
			for o := l.head; o != nil; o = o.next {
				if o.prev != prev || o.lvl != l || o.price != l.price || o.side != s.side {
					return fmt.Errorf("order %d: broken links", o.id)
				}
				if o.leaves <= 0 || o.leaves > o.qty {
					return fmt.Errorf("order %d: leaves %d outside (0,%d]", o.id, o.leaves, o.qty)
				}
				if e.idx.get(o.id) != o {
					return fmt.Errorf("order %d: missing from index", o.id)
				}
				sum += o.leaves
				prev = o
				total++
			}
			if prev != l.tail {
				return fmt.Errorf("level %d: tail mismatch", l.price)
			}
			if sum != l.qty {
				return fmt.Errorf("level %d: qty %d != sum %d", l.price, l.qty, sum)
			}
		}
	}
	if total != e.idx.count {
		return fmt.Errorf("index count %d != resting orders %d", e.idx.count, total)
	}
	if b, a := e.bids.best(), e.asks.best(); b != nil && a != nil && b.price >= a.price {
		return fmt.Errorf("crossed book: bid %d >= ask %d", b.price, a.price)
	}
	return nil
}
