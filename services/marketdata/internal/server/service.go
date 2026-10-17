// Package server holds the market-data service: per-market models fed from
// matching events, the public REST API and the public WebSocket protocol.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tickless/marketdata/internal/state"
	candlesv1 "github.com/tickless/pkg/gen/tickless/candles/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/market"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/wshub"
)

// Specs resolves market configuration (price/quantity formatting).
type Specs interface {
	Spec(symbol string) (market.Spec, bool)
	All() []market.Spec
}

// Intervals are the supported candle intervals.
var Intervals = []string{"1m", "5m", "15m", "1h", "4h", "1d"}

type mkt struct {
	mu          sync.Mutex
	st          *state.Market
	dirtyTicker bool
}

// Service owns the models and the hub.
type Service struct {
	Hub     *wshub.Hub
	specs   Specs
	candles candlesv1.CandlesServiceClient
	now     func() time.Time

	mu   sync.RWMutex
	mkts map[string]*mkt
}

// NewService creates a service; candles may be nil (history endpoints then answer 503).
func NewService(specs Specs, candles candlesv1.CandlesServiceClient) *Service {
	return &Service{Hub: wshub.New("public"), specs: specs, candles: candles, now: time.Now, mkts: map[string]*mkt{}}
}

func (s *Service) get(symbol string) *mkt {
	s.mu.RLock()
	m := s.mkts[symbol]
	s.mu.RUnlock()
	if m != nil {
		return m
	}
	if _, ok := s.specs.Spec(symbol); !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if m = s.mkts[symbol]; m == nil {
		m = &mkt{st: state.New(symbol)}
		s.mkts[symbol] = m
	}
	return m
}

// Load installs a model restored from a snapshot.
func (s *Service) Load(m *state.Market) {
	s.mu.Lock()
	s.mkts[m.Symbol] = &mkt{st: m}
	s.mu.Unlock()
}

// Snapshot serialises one market (nil if unknown).
func (s *Service) Snapshot(symbol string) ([]byte, error) {
	s.mu.RLock()
	m := s.mkts[symbol]
	s.mu.RUnlock()
	if m == nil {
		return nil, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.st.Marshal()
}

// ---- wire formats (all money as decimal strings) ----

type levelMsg [2]string

func levels(spec market.Spec, ls []state.Level) []levelMsg {
	out := make([]levelMsg, len(ls))
	for i, l := range ls {
		out[i] = levelMsg{spec.FormatPrice(l.Price), spec.FormatQty(l.Qty)}
	}
	return out
}

func side(buy bool) string {
	if buy {
		return "buy"
	}
	return "sell"
}

type bookSnapshotMsg struct {
	Type    string     `json:"type"`
	Channel string     `json:"channel"`
	Seq     uint64     `json:"seq"`
	Bids    []levelMsg `json:"bids"`
	Asks    []levelMsg `json:"asks"`
}

type bookDeltaMsg struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	Seq     uint64 `json:"seq"`
	Prev    uint64 `json:"prev"`
	Side    string `json:"side"`
	Price   string `json:"price"`
	Qty     string `json:"qty"`
}

// TradeMsg is a public trade.
type TradeMsg struct {
	Type    string `json:"type,omitempty"`
	Channel string `json:"channel,omitempty"`
	ID      string `json:"id"`
	Price   string `json:"price"`
	Qty     string `json:"qty"`
	Side    string `json:"side"` // taker side
	Time    string `json:"time"`
}

// TickerMsg is the rolling 24 h summary.
type TickerMsg struct {
	Type        string `json:"type,omitempty"`
	Channel     string `json:"channel,omitempty"`
	Market      string `json:"market"`
	Last        string `json:"last"`
	Open        string `json:"open"`
	High        string `json:"high"`
	Low         string `json:"low"`
	Volume      string `json:"volume"`       // base
	QuoteVolume string `json:"quote_volume"` // quote
	Trades      int64  `json:"trades"`
	Change      string `json:"change_pct"`
}

// CandleMsg is one candle.
type CandleMsg struct {
	Type     string `json:"type,omitempty"`
	Channel  string `json:"channel,omitempty"`
	Time     int64  `json:"t"`
	Open     string `json:"o"`
	High     string `json:"h"`
	Low      string `json:"l"`
	Close    string `json:"c"`
	Volume   string `json:"v"`
	Turnover string `json:"quote_volume"`
	Trades   int64  `json:"n"`
}

func tradeMsg(spec market.Spec, t state.Trade) TradeMsg {
	return TradeMsg{ID: fmt.Sprint(t.ID), Price: spec.FormatPrice(t.Price), Qty: spec.FormatQty(t.Qty),
		Side: side(t.TakerBuy), Time: time.Unix(0, t.TimeNanos).UTC().Format(time.RFC3339Nano)}
}

// quoteAmount converts sum(price*qty) in tick-lots into a quote-asset amount:
// tickLots * tickSize * lotSize / 10^baseDecimals, rounded down.
func quoteAmount(spec market.Spec, tickLots money.Int128) string {
	unit, err := money.FromInt64(spec.TickSize).Mul(money.FromInt64(spec.LotSize))
	if err != nil {
		return "0"
	}
	den := money.FromInt64(1)
	for i := uint8(0); i < spec.BaseDecimals; i++ {
		if den, err = den.Mul(money.FromInt64(10)); err != nil {
			return "0"
		}
	}
	v, err := money.MulDiv(tickLots, unit, den, money.RoundDown)
	if err != nil {
		return "0"
	}
	return money.Asset{Decimals: spec.QuoteDecimals}.Format(v)
}

func tickerMsg(spec market.Spec, m *state.Market, now time.Time) TickerMsg {
	t := m.Ticker(now.Unix())
	out := TickerMsg{Market: spec.Symbol, Last: "0", Open: "0", High: "0", Low: "0", Volume: "0", QuoteVolume: "0", Change: "0"}
	if !t.HasTrade {
		return out
	}
	out.Last, out.Open = spec.FormatPrice(t.Last), spec.FormatPrice(t.Open)
	out.High, out.Low = spec.FormatPrice(t.High), spec.FormatPrice(t.Low)
	out.Volume = spec.FormatQty(t.Lots)
	out.QuoteVolume = quoteAmount(spec, t.Turnover)
	out.Trades = t.Trades
	if t.Open > 0 { // basis points -> percent with two decimals, integer math only
		bp := (t.Last - t.Open) * 10_000 / t.Open
		out.Change = fmt.Sprintf("%d.%02d", bp/100, abs64(bp%100))
		if bp < 0 && bp > -100 {
			out.Change = "-" + out.Change
		}
	}
	return out
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func candleMsg(spec market.Spec, c *candlesv1.Candle) CandleMsg {
	turnover, _ := money.ParseInt128(c.Turnover)
	return CandleMsg{Time: c.OpenTimeUnix, Open: spec.FormatPrice(c.Open), High: spec.FormatPrice(c.High), Low: spec.FormatPrice(c.Low),
		Close: spec.FormatPrice(c.Close), Volume: spec.FormatQty(c.VolumeLots), Turnover: quoteAmount(spec, turnover), Trades: c.Trades}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // only plain structs of strings/numbers are marshalled
	}
	return b
}

// SetClock replaces the wall clock used for the 24 h ticker window (tests).
func (s *Service) SetClock(now func() time.Time) { s.now = now }

// ---- feeding ----

// Apply folds a matching event into the model and publishes the changes.
// ts is when the event was produced (for delivery latency).
func (s *Service) Apply(ev *matchingv1.Event, ts time.Time) error {
	m := s.get(ev.Market)
	if m == nil {
		return fmt.Errorf("unknown market %q", ev.Market)
	}
	spec, _ := s.specs.Spec(ev.Market)
	m.mu.Lock()
	defer m.mu.Unlock()
	ups, err := m.st.Apply(ev)
	if err != nil {
		return err
	}
	for _, u := range ups {
		switch {
		case u.Delta != nil:
			ch := "book." + ev.Market
			s.Hub.Publish(ch, mustJSON(bookDeltaMsg{Type: "delta", Channel: ch, Seq: u.Delta.Seq, Prev: u.Delta.Prev,
				Side: side(u.Delta.Buy), Price: spec.FormatPrice(u.Delta.Price), Qty: spec.FormatQty(u.Delta.Qty)}), ts)
		case u.Trade != nil:
			ch := "trades." + ev.Market
			tm := tradeMsg(spec, *u.Trade)
			tm.Type, tm.Channel = "trade", ch
			s.Hub.Publish(ch, mustJSON(tm), ts)
			m.dirtyTicker = true
		}
	}
	return nil
}

// ApplyCandle publishes a live candle update.
func (s *Service) ApplyCandle(u *candlesv1.CandleUpdate, ts time.Time) {
	spec, ok := s.specs.Spec(u.Market)
	if !ok || u.Candle == nil {
		return
	}
	ch := "candles." + u.Market + "." + u.Interval
	cm := candleMsg(spec, u.Candle)
	cm.Type, cm.Channel = "candle", ch
	s.Hub.Publish(ch, mustJSON(cm), ts)
}

// RunTickerFlusher publishes ticker updates for markets with new trades at most
// every interval, until ctx ends.
func (s *Service) RunTickerFlusher(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s.mu.RLock()
		list := make([]*mkt, 0, len(s.mkts))
		for _, m := range s.mkts {
			list = append(list, m)
		}
		s.mu.RUnlock()
		for _, m := range list {
			m.mu.Lock()
			if m.dirtyTicker {
				m.dirtyTicker = false
				if spec, ok := s.specs.Spec(m.st.Symbol); ok {
					tm := tickerMsg(spec, m.st, s.now())
					tm.Type, tm.Channel = "ticker", "ticker."+m.st.Symbol
					s.Hub.Publish(tm.Channel, mustJSON(tm), time.Time{})
				}
			}
			m.mu.Unlock()
		}
	}
}

// ---- WebSocket protocol ----

type clientOp struct {
	Op       string   `json:"op"`
	Channels []string `json:"channels"`
}

const maxSubscriptions = 50

func errMsg(msg string) []byte {
	return mustJSON(map[string]string{"type": "error", "message": msg})
}

// HandleMessage processes one client message.
func (s *Service) HandleMessage(c *wshub.Conn, data []byte) {
	var op clientOp
	if err := json.Unmarshal(data, &op); err != nil {
		c.Send(errMsg("malformed message"), time.Time{})
		return
	}
	switch op.Op {
	case "ping":
		c.Send(mustJSON(map[string]string{"type": "pong"}), time.Time{})
	case "subscribe":
		for _, ch := range op.Channels {
			if s.Hub.Subscriptions(c) >= maxSubscriptions {
				c.Send(errMsg("too many subscriptions"), time.Time{})
				return
			}
			if err := s.subscribe(c, ch); err != nil {
				c.Send(errMsg(err.Error()), time.Time{})
			}
		}
	case "unsubscribe":
		for _, ch := range op.Channels {
			s.Hub.Unsubscribe(c, ch)
			c.Send(mustJSON(map[string]string{"type": "unsubscribed", "channel": ch}), time.Time{})
		}
	default:
		c.Send(errMsg("unknown op"), time.Time{})
	}
}

// subscribe validates the channel, sends the initial state and registers the
// connection. For book channels the snapshot is taken and the subscription
// registered under the market lock, so no delta can be missed or duplicated.
func (s *Service) subscribe(c *wshub.Conn, channel string) error {
	kind, symbol, interval, ok := parseChannel(channel)
	if !ok {
		return fmt.Errorf("unknown channel %q", channel)
	}
	m := s.get(symbol)
	if m == nil {
		return fmt.Errorf("unknown market %q", symbol)
	}
	spec, _ := s.specs.Spec(symbol)
	if kind == "candles" {
		if !validInterval(interval) {
			return fmt.Errorf("unknown interval %q", interval)
		}
		s.Hub.Subscribe(c, channel)
		c.Send(mustJSON(map[string]string{"type": "subscribed", "channel": channel}), time.Time{})
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s.Hub.Subscribe(c, channel)
	c.Send(mustJSON(map[string]string{"type": "subscribed", "channel": channel}), time.Time{})
	switch kind {
	case "book":
		c.Send(mustJSON(bookSnapshotMsg{Type: "snapshot", Channel: channel, Seq: m.st.BookSeq,
			Bids: levels(spec, m.st.Book.Best(true, 0)), Asks: levels(spec, m.st.Book.Best(false, 0))}), time.Time{})
	case "ticker":
		tm := tickerMsg(spec, m.st, s.now())
		tm.Type, tm.Channel = "ticker", channel
		c.Send(mustJSON(tm), time.Time{})
	case "trades":
		trades := m.st.Trades
		if len(trades) > 50 {
			trades = trades[len(trades)-50:]
		}
		list := make([]TradeMsg, len(trades))
		for i, t := range trades {
			list[i] = tradeMsg(spec, t)
		}
		c.Send(mustJSON(map[string]any{"type": "trades_snapshot", "channel": channel, "trades": list}), time.Time{})
	}
	return nil
}

func validInterval(iv string) bool {
	for _, v := range Intervals {
		if v == iv {
			return true
		}
	}
	return false
}

// parseChannel splits "book.BTC-USDT" or "candles.BTC-USDT.1m".
func parseChannel(ch string) (kind, symbol, interval string, ok bool) {
	parts := strings.Split(ch, ".")
	switch {
	case len(parts) == 2 && (parts[0] == "book" || parts[0] == "trades" || parts[0] == "ticker"):
		return parts[0], parts[1], "", parts[1] != ""
	case len(parts) == 3 && parts[0] == "candles":
		return "candles", parts[1], parts[2], parts[1] != ""
	}
	return "", "", "", false
}
