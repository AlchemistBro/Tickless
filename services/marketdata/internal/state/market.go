package state

import (
	"encoding/json"
	"errors"
	"fmt"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/money"
)

const (
	// MaxTrades is how many recent trades are kept per market.
	MaxTrades = 500
	window    = 24 * 60 // minutes of ticker history
)

// ErrGap means an event was missing: Seq jumped by more than one. The model is
// then stale and must be rebuilt from a snapshot or the log; applying on top of
// a gap would produce a wrong book.
var ErrGap = errors.New("state: gap in matching event sequence")

// Trade is one execution (ticks / lots), TakerSide is the aggressor side.
type Trade struct {
	ID        uint64 `json:"id"`
	Seq       uint64 `json:"seq"`
	Price     int64  `json:"p"`
	Qty       int64  `json:"q"`
	TakerBuy  bool   `json:"b"`
	TimeNanos int64  `json:"t"`
}

// Bucket aggregates one UTC minute of trades for the 24 h ticker.
type Bucket struct {
	Minute   int64  `json:"m"` // unix seconds, start of the minute
	Open     int64  `json:"o"`
	High     int64  `json:"h"`
	Low      int64  `json:"l"`
	Close    int64  `json:"c"`
	Lots     int64  `json:"v"`
	Turnover string `json:"x"` // sum(price*qty) in tick-lots, decimal Int128
	Trades   int64  `json:"n"`
}

// BookDelta tells subscribers a level changed. Prev is the Seq of the previous
// book delta of this market, so a client detects a missed message
// (Prev != last seen) without needing every engine sequence number.
type BookDelta struct {
	Seq, Prev uint64
	Buy       bool
	Price     int64
	Qty       int64
}

// Update is what applying one event produced for broadcasting.
type Update struct {
	Delta *BookDelta
	Trade *Trade
}

// Market is the model of one market. It is not safe for concurrent use.
type Market struct {
	Symbol  string
	Seq     uint64 // last engine event sequence applied
	BookSeq uint64 // sequence of the last book delta applied (0 = none yet)
	Book    Book
	Trades  []Trade  // oldest first, at most MaxTrades
	Buckets []Bucket // ascending minute, at most 24 h
}

// New returns an empty market model.
func New(symbol string) *Market { return &Market{Symbol: symbol} }

// Apply folds one engine event into the model. Duplicates (Seq <= m.Seq, e.g.
// after a replay) are ignored; a gap returns ErrGap and changes nothing.
func (m *Market) Apply(ev *matchingv1.Event) ([]Update, error) {
	if ev.Seq <= m.Seq {
		return nil, nil
	}
	if m.Seq != 0 && ev.Seq != m.Seq+1 {
		return nil, fmt.Errorf("%w: %s has seq %d after %d", ErrGap, m.Symbol, ev.Seq, m.Seq)
	}
	m.Seq = ev.Seq
	switch ev.Type {
	case matchingv1.EventType_EVENT_TYPE_BOOK_DELTA:
		m.Book.Set(ev.Side == commonv1.Side_SIDE_BUY, ev.Price, ev.Qty)
		d := &BookDelta{Seq: ev.Seq, Prev: m.BookSeq, Buy: ev.Side == commonv1.Side_SIDE_BUY, Price: ev.Price, Qty: ev.Qty}
		m.BookSeq = ev.Seq
		return []Update{{Delta: d}}, nil
	case matchingv1.EventType_EVENT_TYPE_TRADE:
		t := Trade{ID: ev.TradeId, Seq: ev.Seq, Price: ev.Price, Qty: ev.Qty,
			TakerBuy: ev.Side == commonv1.Side_SIDE_BUY, TimeNanos: ev.TimeUnixNano}
		m.Trades = append(m.Trades, t)
		if len(m.Trades) > MaxTrades {
			m.Trades = append(m.Trades[:0], m.Trades[len(m.Trades)-MaxTrades:]...)
		}
		m.addToBucket(t)
		return []Update{{Trade: &t}}, nil
	}
	return nil, nil
}

func (m *Market) addToBucket(t Trade) {
	minute := t.TimeNanos / 1e9 / 60 * 60
	n := len(m.Buckets)
	if n == 0 || m.Buckets[n-1].Minute < minute {
		m.Buckets = append(m.Buckets, Bucket{Minute: minute, Open: t.Price, High: t.Price, Low: t.Price, Turnover: "0"})
		n++
	}
	// An out-of-order timestamp is attributed to the newest bucket (all
	// timestamps come from the same producer clock, so this is at most a skew).
	b := &m.Buckets[n-1]
	b.High, b.Low, b.Close = max(b.High, t.Price), min(b.Low, t.Price), t.Price
	b.Lots += t.Qty
	b.Trades++
	prev, _ := money.ParseInt128(b.Turnover)
	tl, _ := money.FromInt64(t.Price).Mul(money.FromInt64(t.Qty))
	sum, err := prev.Add(tl)
	if err == nil {
		b.Turnover = sum.String()
	}
	// Keep 24 h of buckets.
	if len(m.Buckets) > window {
		m.Buckets = append(m.Buckets[:0], m.Buckets[len(m.Buckets)-window:]...)
	}
}

// Ticker is the rolling 24 h summary (ticks / lots).
type Ticker struct {
	Last     int64
	Open     int64
	High     int64
	Low      int64
	Lots     int64
	Turnover money.Int128 // sum(price*qty) in tick-lots
	Trades   int64
	HasTrade bool
}

// Ticker summarises the buckets within 24 h before nowUnix.
func (m *Market) Ticker(nowUnix int64) Ticker {
	var t Ticker
	cutoff := nowUnix - 24*3600
	for _, b := range m.Buckets {
		if b.Minute+60 <= cutoff {
			continue
		}
		if !t.HasTrade {
			t.HasTrade, t.Open, t.High, t.Low = true, b.Open, b.High, b.Low
		}
		t.High, t.Low, t.Last = max(t.High, b.High), min(t.Low, b.Low), b.Close
		t.Lots += b.Lots
		t.Trades += b.Trades
		x, _ := money.ParseInt128(b.Turnover)
		t.Turnover, _ = t.Turnover.Add(x)
	}
	return t
}

// LastPrice is the price of the most recent trade (0 if none).
func (m *Market) LastPrice() int64 {
	if n := len(m.Trades); n > 0 {
		return m.Trades[n-1].Price
	}
	return 0
}

type snapshot struct {
	Symbol  string   `json:"symbol"`
	Seq     uint64   `json:"seq"`
	BookSeq uint64   `json:"book_seq"`
	Book    Book     `json:"book"`
	Trades  []Trade  `json:"trades"`
	Buckets []Bucket `json:"buckets"`
}

// Marshal serialises the model for the Redis snapshot.
func (m *Market) Marshal() ([]byte, error) {
	return json.Marshal(snapshot{Symbol: m.Symbol, Seq: m.Seq, BookSeq: m.BookSeq, Book: m.Book, Trades: m.Trades, Buckets: m.Buckets})
}

// Unmarshal restores a model written by Marshal.
func Unmarshal(data []byte) (*Market, error) {
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	if s.Symbol == "" {
		return nil, errors.New("state: snapshot without symbol")
	}
	for _, side := range [][]Level{s.Book.Bids, s.Book.Asks} {
		for i, l := range side {
			if l.Price <= 0 || l.Qty <= 0 || (i > 0 && side[i-1].Price >= l.Price) {
				return nil, errors.New("state: invalid book in snapshot")
			}
		}
	}
	return &Market{Symbol: s.Symbol, Seq: s.Seq, BookSeq: s.BookSeq, Book: s.Book, Trades: s.Trades, Buckets: s.Buckets}, nil
}
