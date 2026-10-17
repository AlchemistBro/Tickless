package engine_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/tickless/matching/engine"
)

type h struct {
	t   *testing.T
	e   *Engine
	buf []Event
	now int64
}

func newH(t *testing.T) *h { return &h{t: t, e: New("BTC-USDT")} }

func (x *h) do(c Command) []Event {
	x.now++
	c.Time = x.now
	x.buf = x.e.Process(c, x.buf[:0])
	require.NoError(x.t, x.e.CheckInvariants())
	// Sequence numbers are gap-free and increasing.
	for i := 1; i < len(x.buf); i++ {
		require.Equal(x.t, x.buf[i-1].Seq+1, x.buf[i].Seq)
	}
	return append([]Event(nil), x.buf...)
}

func limit(id, user uint64, side Side, price, qty int64) Command {
	return Command{Type: CmdPlace, OrderID: id, UserID: user, Side: side, OrderType: Limit, TIF: GTC, Price: price, Qty: qty}
}

func types(evs []Event) []EventType {
	out := make([]EventType, len(evs))
	for i, e := range evs {
		out[i] = e.Type
	}
	return out
}

func TestRestingLimitOrder(t *testing.T) {
	x := newH(t)
	evs := x.do(limit(1, 1, SideBuy, 100, 5))
	assert.Equal(t, []EventType{EvAccepted, EvBookDelta}, types(evs))
	assert.Equal(t, Event{Seq: 2, Time: 1, Type: EvBookDelta, Side: SideBuy, Price: 100, Qty: 5}, evs[1])
	assert.Equal(t, 1, x.e.OpenOrders())
}

func TestFullFillAtMakerPrice(t *testing.T) {
	x := newH(t)
	x.do(limit(1, 1, SideSell, 100, 5))
	evs := x.do(limit(2, 2, SideBuy, 105, 5)) // willing to pay 105, pays maker's 100
	require.Equal(t, []EventType{EvAccepted, EvTrade, EvBookDelta}, types(evs))
	tr := evs[1]
	assert.Equal(t, int64(100), tr.Price)
	assert.Equal(t, int64(5), tr.Qty)
	assert.Equal(t, uint64(1), tr.MakerOrderID)
	assert.Equal(t, uint64(2), tr.OrderID)
	assert.Equal(t, int64(0), tr.TakerLeaves)
	assert.Equal(t, int64(0), tr.MakerLeaves)
	assert.Equal(t, uint64(1), tr.TradeID)
	assert.Equal(t, SideSell, evs[2].Side)
	assert.Equal(t, int64(0), evs[2].Qty)
	assert.Zero(t, x.e.OpenOrders())
}

func TestPartialFillThenRest(t *testing.T) {
	x := newH(t)
	x.do(limit(1, 1, SideSell, 100, 3))
	evs := x.do(limit(2, 2, SideBuy, 100, 5))
	require.Equal(t, []EventType{EvAccepted, EvTrade, EvBookDelta, EvBookDelta}, types(evs))
	assert.Equal(t, int64(2), evs[1].TakerLeaves)
	assert.Equal(t, SideBuy, evs[3].Side)
	assert.Equal(t, int64(2), evs[3].Qty)
	assert.Equal(t, 1, x.e.OpenOrders())
}

func TestPriceTimePriority(t *testing.T) {
	x := newH(t)
	x.do(limit(1, 1, SideSell, 101, 1))
	x.do(limit(2, 2, SideSell, 100, 1))
	x.do(limit(3, 3, SideSell, 100, 1)) // same price, later
	evs := x.do(limit(4, 4, SideBuy, 101, 3))
	var makers []uint64
	for _, e := range evs {
		if e.Type == EvTrade {
			makers = append(makers, e.MakerOrderID)
		}
	}
	assert.Equal(t, []uint64{2, 3, 1}, makers, "best price first, FIFO within a level")
}

func TestOneDeltaPerSweptLevel(t *testing.T) {
	x := newH(t)
	for i := uint64(0); i < 4; i++ {
		x.do(limit(10+i, 1, SideSell, 100, 1))
	}
	evs := x.do(limit(1, 2, SideBuy, 100, 4))
	deltas := 0
	for _, e := range evs {
		if e.Type == EvBookDelta {
			deltas++
		}
	}
	assert.Equal(t, 1, deltas)
}

func TestMarketOrder(t *testing.T) {
	t.Run("buy sweeps and cancels remainder (IOC default)", func(t *testing.T) {
		x := newH(t)
		x.do(limit(1, 1, SideSell, 100, 2))
		x.do(limit(2, 1, SideSell, 110, 2))
		evs := x.do(Command{Type: CmdPlace, OrderID: 3, UserID: 2, Side: SideBuy, OrderType: Market, Qty: 10})
		last := evs[len(evs)-1]
		assert.Equal(t, EvCancelled, last.Type)
		assert.Equal(t, CancelIOCRemainder, last.Reason)
		assert.Equal(t, int64(6), last.Qty)
		assert.Zero(t, x.e.OpenOrders())
	})
	t.Run("protective bound stops the sweep", func(t *testing.T) {
		x := newH(t)
		x.do(limit(1, 1, SideSell, 100, 2))
		x.do(limit(2, 1, SideSell, 110, 2))
		evs := x.do(Command{Type: CmdPlace, OrderID: 3, UserID: 2, Side: SideBuy, OrderType: Market, Price: 105, Qty: 10})
		trades := 0
		for _, e := range evs {
			if e.Type == EvTrade {
				trades++
				assert.Equal(t, int64(100), e.Price)
			}
		}
		assert.Equal(t, 1, trades)
		assert.Equal(t, 1, x.e.OpenOrders()) // the 110 ask remains
	})
	t.Run("sell market never rests on empty book", func(t *testing.T) {
		x := newH(t)
		evs := x.do(Command{Type: CmdPlace, OrderID: 1, UserID: 1, Side: SideSell, OrderType: Market, Qty: 3})
		assert.Equal(t, []EventType{EvAccepted, EvCancelled}, types(evs))
		assert.Zero(t, x.e.OpenOrders())
	})
	t.Run("GTC market rejected", func(t *testing.T) {
		x := newH(t)
		evs := x.do(Command{Type: CmdPlace, OrderID: 1, UserID: 1, Side: SideBuy, OrderType: Market, TIF: GTC, Qty: 1})
		assert.Equal(t, RejInvalidCombo, evs[0].Reason)
	})
}

func TestIOC(t *testing.T) {
	x := newH(t)
	x.do(limit(1, 1, SideSell, 100, 2))
	c := limit(2, 2, SideBuy, 100, 5)
	c.TIF = IOC
	evs := x.do(c)
	require.Equal(t, []EventType{EvAccepted, EvTrade, EvBookDelta, EvCancelled}, types(evs))
	assert.Equal(t, int64(3), evs[3].Qty)
	assert.Zero(t, x.e.OpenOrders())
}

func TestFOK(t *testing.T) {
	t.Run("insufficient liquidity: no trades, cancelled", func(t *testing.T) {
		x := newH(t)
		x.do(limit(1, 1, SideSell, 100, 2))
		c := limit(2, 2, SideBuy, 100, 5)
		c.TIF = FOK
		evs := x.do(c)
		require.Equal(t, []EventType{EvAccepted, EvCancelled}, types(evs))
		assert.Equal(t, CancelFOKUnfilled, evs[1].Reason)
		assert.Equal(t, int64(5), evs[1].Qty)
		assert.Equal(t, 1, x.e.OpenOrders(), "book untouched")
	})
	t.Run("liquidity across levels fills fully", func(t *testing.T) {
		x := newH(t)
		x.do(limit(1, 1, SideSell, 100, 2))
		x.do(limit(2, 1, SideSell, 101, 3))
		c := limit(3, 2, SideBuy, 101, 5)
		c.TIF = FOK
		evs := x.do(c)
		n := 0
		for _, e := range evs {
			if e.Type == EvTrade {
				n++
			}
		}
		assert.Equal(t, 2, n)
		assert.Zero(t, x.e.OpenOrders())
	})
	t.Run("own order in the way makes FOK unfillable", func(t *testing.T) {
		x := newH(t)
		x.do(limit(1, 1, SideSell, 100, 2))
		x.do(limit(2, 2, SideSell, 100, 5)) // user 2's own
		c := limit(3, 2, SideBuy, 100, 4)
		c.TIF = FOK
		evs := x.do(c)
		assert.Equal(t, []EventType{EvAccepted, EvCancelled}, types(evs))
		assert.Equal(t, 2, x.e.OpenOrders())
	})
}

func TestPostOnly(t *testing.T) {
	x := newH(t)
	x.do(limit(1, 1, SideSell, 100, 2))
	c := limit(2, 2, SideBuy, 100, 1)
	c.PostOnly = true
	evs := x.do(c)
	require.Len(t, evs, 1)
	assert.Equal(t, EvRejected, evs[0].Type)
	assert.Equal(t, RejPostOnlyWouldCross, evs[0].Reason)

	c = limit(3, 2, SideBuy, 99, 1)
	c.PostOnly = true
	evs = x.do(c)
	assert.Equal(t, []EventType{EvAccepted, EvBookDelta}, types(evs))

	c = limit(4, 2, SideBuy, 99, 1)
	c.PostOnly, c.TIF = true, IOC
	assert.Equal(t, RejInvalidCombo, x.do(c)[0].Reason)
}

func TestSelfTradePreventionCancelsNewest(t *testing.T) {
	x := newH(t)
	x.do(limit(1, 1, SideSell, 100, 2)) // user 1 rests
	x.do(limit(2, 2, SideSell, 100, 2))
	// user 1 buys 3: own order is first in the queue -> incoming is cancelled, resting stays.
	evs := x.do(limit(3, 1, SideBuy, 100, 3))
	require.Equal(t, []EventType{EvAccepted, EvCancelled}, types(evs))
	assert.Equal(t, CancelSelfTrade, evs[1].Reason)
	assert.Equal(t, int64(3), evs[1].Qty)
	assert.Equal(t, 2, x.e.OpenOrders())

	// A fill ahead of the own order stands; the remainder is cancelled.
	x2 := newH(t)
	x2.do(limit(1, 2, SideSell, 100, 1))
	x2.do(limit(2, 1, SideSell, 100, 1))
	evs = x2.do(limit(3, 1, SideBuy, 100, 3))
	assert.Equal(t, []EventType{EvAccepted, EvTrade, EvBookDelta, EvCancelled}, types(evs))
	assert.Equal(t, int64(2), evs[3].Qty)
}

func TestCancel(t *testing.T) {
	x := newH(t)
	x.do(limit(1, 1, SideBuy, 100, 5))
	x.do(limit(2, 2, SideBuy, 100, 3))

	evs := x.do(Command{Type: CmdCancel, OrderID: 1, UserID: 1})
	require.Equal(t, []EventType{EvCancelled, EvBookDelta}, types(evs))
	assert.Equal(t, int64(5), evs[0].Qty)
	assert.Equal(t, CancelUser, evs[0].Reason)
	assert.Equal(t, int64(3), evs[1].Qty)

	evs = x.do(Command{Type: CmdCancel, OrderID: 2, UserID: 2})
	assert.Equal(t, int64(0), evs[1].Qty, "level removed")
	assert.Zero(t, x.e.OpenOrders())

	evs = x.do(Command{Type: CmdCancel, OrderID: 2, UserID: 2})
	assert.Equal(t, RejUnknownOrder, evs[0].Reason, "double cancel")
}

func TestCancelWrongUserRejected(t *testing.T) {
	x := newH(t)
	x.do(limit(1, 1, SideBuy, 100, 5))
	evs := x.do(Command{Type: CmdCancel, OrderID: 1, UserID: 2})
	assert.Equal(t, RejUnknownOrder, evs[0].Reason)
	assert.Equal(t, 1, x.e.OpenOrders())
}

func TestValidation(t *testing.T) {
	cases := map[string]struct {
		c    Command
		want Reason
	}{
		"zero qty":      {limit(1, 1, SideBuy, 100, 0), RejInvalidQty},
		"negative qty":  {limit(1, 1, SideBuy, 100, -1), RejInvalidQty},
		"zero price":    {limit(1, 1, SideBuy, 0, 1), RejInvalidPrice},
		"neg price":     {limit(1, 1, SideBuy, -5, 1), RejInvalidPrice},
		"bad side":      {limit(1, 1, 0, 100, 1), RejInvalidSide},
		"bad tif":       {Command{Type: CmdPlace, OrderID: 1, UserID: 1, Side: SideBuy, OrderType: Limit, TIF: 9, Price: 1, Qty: 1}, RejInvalidCombo},
		"bad type":      {Command{Type: CmdPlace, OrderID: 1, UserID: 1, Side: SideBuy, OrderType: 7, Price: 1, Qty: 1}, RejInvalidCombo},
		"unknown cmd":   {Command{Type: 9, OrderID: 1}, RejUnknownCommand},
		"mkt postonly":  {Command{Type: CmdPlace, OrderID: 1, UserID: 1, Side: SideBuy, OrderType: Market, PostOnly: true, Qty: 1}, RejInvalidCombo},
		"mkt neg bound": {Command{Type: CmdPlace, OrderID: 1, UserID: 1, Side: SideBuy, OrderType: Market, Price: -1, Qty: 1}, RejInvalidPrice},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			x := newH(t)
			evs := x.do(tc.c)
			require.Len(t, evs, 1)
			assert.Equal(t, EvRejected, evs[0].Type)
			assert.Equal(t, tc.want, evs[0].Reason)
			assert.Zero(t, x.e.OpenOrders())
		})
	}
}

func TestDuplicateOrderID(t *testing.T) {
	x := newH(t)
	x.do(limit(1, 1, SideBuy, 100, 5))
	evs := x.do(limit(1, 1, SideBuy, 100, 5))
	assert.Equal(t, RejDuplicateOrder, evs[0].Reason)
	assert.Equal(t, 1, x.e.OpenOrders())
}

func TestSequenceNumbersGapFree(t *testing.T) {
	x := newH(t)
	var last uint64
	for i := uint64(1); i <= 50; i++ {
		side := SideBuy
		if i%2 == 0 {
			side = SideSell
		}
		for _, ev := range x.do(limit(i, i, side, 100+int64(i%3)-1, 2)) {
			require.Equal(t, last+1, ev.Seq)
			last = ev.Seq
		}
	}
	assert.Equal(t, last, x.e.Seq())
}

func TestDepth(t *testing.T) {
	x := newH(t)
	x.do(limit(1, 1, SideBuy, 99, 1))
	x.do(limit(2, 1, SideBuy, 100, 2))
	x.do(limit(3, 1, SideBuy, 100, 3))
	x.do(limit(4, 1, SideSell, 101, 4))
	assert.Equal(t, []PriceLevel{{100, 5}, {99, 1}}, x.e.Depth(SideBuy, 10, nil))
	assert.Equal(t, []PriceLevel{{100, 5}}, x.e.Depth(SideBuy, 1, nil))
	assert.Equal(t, []PriceLevel{{101, 4}}, x.e.Depth(SideSell, 10, nil))
}

func TestManyLevelsOutOfOrder(t *testing.T) {
	x := newH(t)
	prices := []int64{50, 10, 90, 30, 70, 20, 80, 40, 60, 100}
	for i, p := range prices {
		x.do(limit(uint64(i+1), 1, SideBuy, p, 1))
	}
	d := x.e.Depth(SideBuy, 20, nil)
	require.Len(t, d, len(prices))
	for i := 1; i < len(d); i++ {
		require.Greater(t, d[i-1].Price, d[i].Price)
	}
	// Cancel from the middle and from the top.
	x.do(Command{Type: CmdCancel, OrderID: 4, UserID: 1}) // price 30
	x.do(Command{Type: CmdCancel, OrderID: 10, UserID: 1})
	assert.Equal(t, int64(90), x.e.Depth(SideBuy, 1, nil)[0].Price)
}
