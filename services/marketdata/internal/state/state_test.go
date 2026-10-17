package state_test

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/marketdata/internal/state"
	"github.com/tickless/matching/engine"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
)

func toProto(market string, e engine.Event) *matchingv1.Event {
	return &matchingv1.Event{
		Market: market, Seq: e.Seq, TimeUnixNano: e.Time, Type: matchingv1.EventType(e.Type), Reason: matchingv1.Reason(e.Reason),
		Side: commonv1.Side(e.Side), OrderId: e.OrderID, UserId: e.UserID, MakerOrderId: e.MakerOrderID, MakerUserId: e.MakerUserID,
		Price: e.Price, Qty: e.Qty, TakerLeaves: e.TakerLeaves, MakerLeaves: e.MakerLeaves, TradeId: e.TradeID,
	}
}

func engineLevels(e *engine.Engine, side engine.Side) []state.Level {
	out := []state.Level{}
	for _, l := range e.Depth(side, 1<<30, nil) {
		out = append(out, state.Level{Price: l.Price, Qty: l.Qty})
	}
	return out
}

func randomCommands(seed int64, n int) []engine.Command {
	r := rand.New(rand.NewSource(seed))
	var cmds []engine.Command
	var id uint64
	for i := 0; i < n; i++ {
		if r.Intn(5) == 0 && id > 0 {
			cmds = append(cmds, engine.Command{Type: engine.CmdCancel, OrderID: uint64(1 + r.Intn(int(id))), UserID: uint64(1 + r.Intn(5)), Time: int64(i)})
			continue
		}
		id++
		c := engine.Command{Type: engine.CmdPlace, OrderID: id, UserID: uint64(1 + r.Intn(5)), Side: engine.Side(1 + r.Intn(2)),
			OrderType: engine.Limit, TIF: engine.TIF(1 + r.Intn(3)), Price: int64(90 + r.Intn(21)), Qty: int64(1 + r.Intn(15)),
			Time: 1_700_000_000_000_000_000 + int64(i)*1e9}
		if r.Intn(10) == 0 {
			c.OrderType, c.TIF = engine.Market, engine.IOC
		}
		cmds = append(cmds, c)
	}
	return cmds
}

// The acceptance property of the market-data model: fed only with the engine
// events, its book is EXACTLY the engine book after every single command.
func TestBookEqualsEngineStateAfterEveryCommand(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		e := engine.New("BTC-USDT")
		m := state.New("BTC-USDT")
		var buf []engine.Event
		for i, c := range randomCommands(seed, 1500) {
			buf = e.Process(c, buf[:0])
			for _, ev := range buf {
				_, err := m.Apply(toProto("BTC-USDT", ev))
				require.NoError(t, err)
			}
			require.Equal(t, engineLevels(e, engine.SideBuy), m.Book.Best(true, 0), "seed %d command %d bids", seed, i)
			require.Equal(t, engineLevels(e, engine.SideSell), m.Book.Best(false, 0), "seed %d command %d asks", seed, i)
			require.Equal(t, e.Seq(), m.Seq)
		}
	}
}

// A client that applies the broadcast deltas to the snapshot it was handed
// rebuilds the same book, and the Prev chain lets it detect any missed message.
func TestDeltaStreamRebuildsBookAndPrevChainIsContiguous(t *testing.T) {
	e := engine.New("M")
	m := state.New("M")
	var buf []engine.Event
	cmds := randomCommands(99, 2000)
	var client state.Book
	var last uint64
	snapshotAt := 700
	for i, c := range cmds {
		buf = e.Process(c, buf[:0])
		for _, ev := range buf {
			ups, err := m.Apply(toProto("M", ev))
			require.NoError(t, err)
			if i < snapshotAt {
				continue
			}
			if i == snapshotAt && last == 0 { // client subscribes here: takes the snapshot
				client, last = m.Book.Clone(), m.BookSeq
				continue
			}
			for _, u := range ups {
				if u.Delta == nil {
					continue
				}
				require.Equal(t, last, u.Delta.Prev, "no delta may be missed or reordered")
				client.Set(u.Delta.Buy, u.Delta.Price, u.Delta.Qty)
				last = u.Delta.Seq
			}
		}
	}
	require.Equal(t, m.Book, client)
}

func TestDuplicatesAreIgnoredAndGapsDetected(t *testing.T) {
	m := state.New("M")
	delta := func(seq uint64, price, qty int64) *matchingv1.Event {
		return &matchingv1.Event{Market: "M", Seq: seq, Type: matchingv1.EventType_EVENT_TYPE_BOOK_DELTA, Side: commonv1.Side_SIDE_BUY, Price: price, Qty: qty}
	}
	_, err := m.Apply(delta(1, 100, 5))
	require.NoError(t, err)
	ups, err := m.Apply(delta(1, 100, 99)) // replayed duplicate
	require.NoError(t, err)
	assert.Empty(t, ups)
	assert.Equal(t, int64(5), m.Book.Best(true, 0)[0].Qty, "duplicate did not change the book")

	_, err = m.Apply(delta(3, 101, 1)) // seq 2 is missing
	require.ErrorIs(t, err, state.ErrGap)
	assert.Len(t, m.Book.Best(true, 0), 1, "a gap changes nothing")

	_, err = m.Apply(delta(2, 101, 1))
	require.NoError(t, err)
	assert.Equal(t, []state.Level{{Price: 101, Qty: 1}, {Price: 100, Qty: 5}}, m.Book.Best(true, 0), "bids best first")
}

func TestTradesTickerAndBuckets(t *testing.T) {
	m := state.New("M")
	trade := func(seq uint64, id uint64, price, qty int64, tsSec int64, buy bool) {
		side := commonv1.Side_SIDE_SELL
		if buy {
			side = commonv1.Side_SIDE_BUY
		}
		_, err := m.Apply(&matchingv1.Event{Market: "M", Seq: seq, Type: matchingv1.EventType_EVENT_TYPE_TRADE, TradeId: id, Price: price, Qty: qty, Side: side, TimeUnixNano: tsSec * 1e9})
		require.NoError(t, err)
	}
	base := int64(1_700_000_040) // start of a minute
	trade(1, 1, 100, 2, base+1, true)
	trade(2, 2, 110, 1, base+30, false)
	trade(3, 3, 90, 3, base+61, true) // next minute
	require.Len(t, m.Buckets, 2)
	assert.Equal(t, state.Bucket{Minute: base, Open: 100, High: 110, Low: 100, Close: 110, Lots: 3, Turnover: "310", Trades: 2}, m.Buckets[0])

	tk := m.Ticker(base + 120)
	assert.Equal(t, int64(100), tk.Open)
	assert.Equal(t, int64(110), tk.High)
	assert.Equal(t, int64(90), tk.Low)
	assert.Equal(t, int64(90), tk.Last)
	assert.Equal(t, int64(6), tk.Lots)
	assert.Equal(t, "580", tk.Turnover.String()) // 100*2 + 110*1 + 90*3
	assert.Equal(t, int64(90), m.LastPrice())

	// Everything older than 24 h falls out of the ticker.
	old := m.Ticker(base + 25*3600)
	assert.False(t, old.HasTrade)

	for i := 0; i < state.MaxTrades+50; i++ {
		trade(uint64(4+i), uint64(4+i), 100, 1, base+200, true)
	}
	assert.Len(t, m.Trades, state.MaxTrades, "recent-trade ring is bounded")
	assert.Equal(t, uint64(4+state.MaxTrades+49), m.Trades[len(m.Trades)-1].ID)
}

func TestSnapshotRoundTripAndValidation(t *testing.T) {
	e := engine.New("M")
	m := state.New("M")
	var buf []engine.Event
	for _, c := range randomCommands(5, 800) {
		buf = e.Process(c, buf[:0])
		for _, ev := range buf {
			_, _ = m.Apply(toProto("M", ev))
		}
	}
	data, err := m.Marshal()
	require.NoError(t, err)
	back, err := state.Unmarshal(data)
	require.NoError(t, err)
	assert.Equal(t, m, back)

	_, err = state.Unmarshal([]byte(`{"symbol":"M","book":{"bids":[{"p":5,"q":1},{"p":5,"q":1}]}}`))
	assert.Error(t, err, "unsorted/duplicate levels are rejected")
	_, err = state.Unmarshal([]byte(`not json`))
	assert.Error(t, err)
	_, err = state.Unmarshal([]byte(`{}`))
	assert.Error(t, err)
}
