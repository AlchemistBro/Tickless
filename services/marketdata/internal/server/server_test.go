package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tickless/marketdata/internal/server"
	"github.com/tickless/matching/engine"
	candlesv1 "github.com/tickless/pkg/gen/tickless/candles/v1"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/market"
	"github.com/tickless/pkg/money"
)

var btc = market.Spec{Symbol: "BTC-USDT", Base: "BTC", Quote: "USDT", BaseDecimals: 8, QuoteDecimals: 6,
	TickSize: 10_000, LotSize: 1_000, MinNotional: money.FromInt64(5_000_000)}

type specs map[string]market.Spec

func (s specs) Spec(sym string) (market.Spec, bool) { v, ok := s[sym]; return v, ok }
func (s specs) All() []market.Spec {
	var out []market.Spec
	for _, v := range s {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out
}

type fakeCandles struct {
	candlesv1.CandlesServiceClient
	req  *candlesv1.GetCandlesRequest
	resp *candlesv1.GetCandlesResponse
	err  error
}

func (f *fakeCandles) GetCandles(_ context.Context, r *candlesv1.GetCandlesRequest, _ ...grpc.CallOption) (*candlesv1.GetCandlesResponse, error) {
	f.req = r
	return f.resp, f.err
}

type env struct {
	svc *server.Service
	srv *httptest.Server
	fc  *fakeCandles
	eng *engine.Engine
	buf []engine.Event
	now int64
}

func newEnv(t *testing.T) *env {
	fc := &fakeCandles{}
	s := server.NewService(specs{"BTC-USDT": btc}, fc)
	e := &env{svc: s, fc: fc, eng: engine.New("BTC-USDT"), now: 1_700_000_000_000_000_000}
	s.SetClock(func() time.Time { return time.Unix(0, e.now) }) // events carry synthetic times
	e.srv = httptest.NewServer(s.Handler(server.HTTPOptions{MaxConnsPerIP: 3}))
	t.Cleanup(e.srv.Close)
	return e
}

func toProto(e engine.Event) *matchingv1.Event {
	return &matchingv1.Event{Market: "BTC-USDT", Seq: e.Seq, TimeUnixNano: e.Time, Type: matchingv1.EventType(e.Type), Reason: matchingv1.Reason(e.Reason),
		Side: commonv1.Side(e.Side), OrderId: e.OrderID, UserId: e.UserID, MakerOrderId: e.MakerOrderID, MakerUserId: e.MakerUserID,
		Price: e.Price, Qty: e.Qty, TakerLeaves: e.TakerLeaves, MakerLeaves: e.MakerLeaves, TradeId: e.TradeID}
}

// feed runs one engine command and applies its events to the service.
func (e *env) feed(t testing.TB, c engine.Command) {
	e.now += int64(time.Second)
	c.Time = e.now
	e.buf = e.eng.Process(c, e.buf[:0])
	for _, ev := range e.buf {
		require.NoError(t, e.svc.Apply(toProto(ev), time.Now()))
	}
}

func commands(seed int64, n int) []engine.Command {
	r := rand.New(rand.NewSource(seed))
	var out []engine.Command
	var id uint64
	for i := 0; i < n; i++ {
		if r.Intn(5) == 0 && id > 0 {
			out = append(out, engine.Command{Type: engine.CmdCancel, OrderID: uint64(1 + r.Intn(int(id))), UserID: uint64(1 + r.Intn(5))})
			continue
		}
		id++
		out = append(out, engine.Command{Type: engine.CmdPlace, OrderID: id, UserID: uint64(1 + r.Intn(5)), Side: engine.Side(1 + r.Intn(2)),
			OrderType: engine.Limit, TIF: engine.TIF(1 + r.Intn(3)), Price: int64(4_990_000 + r.Intn(41)), Qty: int64(1 + r.Intn(20))})
	}
	return out
}

// ---- WebSocket test client ----

type wsClient struct {
	t    *testing.T
	c    *websocket.Conn
	book map[bool]map[int64]int64 // buy? -> price ticks -> lots
	last uint64
}

func (e *env) dial(t *testing.T) *wsClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(e.srv.URL, "http")+"/ws/public", nil)
	closeBody(resp)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.CloseNow() })
	return &wsClient{t: t, c: c, book: map[bool]map[int64]int64{true: {}, false: {}}}
}

func (w *wsClient) send(v any) {
	b, _ := json.Marshal(v)
	require.NoError(w.t, w.c.Write(context.Background(), websocket.MessageText, b))
}

func (w *wsClient) read() map[string]any {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, b, err := w.c.Read(ctx)
	require.NoError(w.t, err)
	var m map[string]any
	require.NoError(w.t, json.Unmarshal(b, &m), string(b))
	return m
}

func ticks(t testing.TB, s string) int64 {
	v, err := btc.ParsePrice(s)
	require.NoError(t, err)
	return v
}

func lots(t testing.TB, s string) int64 {
	if s == "0" {
		return 0
	}
	v, err := btc.ParseQty(s)
	require.NoError(t, err)
	return v
}

func (w *wsClient) loadSnapshot(m map[string]any) {
	w.book = map[bool]map[int64]int64{true: {}, false: {}}
	for _, side := range []struct {
		key string
		buy bool
	}{{"bids", true}, {"asks", false}} {
		for _, l := range m[side.key].([]any) {
			lv := l.([]any)
			w.book[side.buy][ticks(w.t, lv[0].(string))] = lots(w.t, lv[1].(string))
		}
	}
	w.last = uint64(m["seq"].(float64))
}

// apply handles a book message; it reports false when a gap is detected.
func (w *wsClient) apply(m map[string]any) bool {
	switch m["type"] {
	case "snapshot":
		w.loadSnapshot(m)
	case "delta":
		if uint64(m["prev"].(float64)) != w.last {
			return false // a message was missed: resync
		}
		buy := m["side"] == "buy"
		p, q := ticks(w.t, m["price"].(string)), lots(w.t, m["qty"].(string))
		if q == 0 {
			delete(w.book[buy], p)
		} else {
			w.book[buy][p] = q
		}
		w.last = uint64(m["seq"].(float64))
	}
	return true
}

func engineBook(e *engine.Engine) map[bool]map[int64]int64 {
	out := map[bool]map[int64]int64{true: {}, false: {}}
	for _, l := range e.Depth(engine.SideBuy, 1<<30, nil) {
		out[true][l.Price] = l.Qty
	}
	for _, l := range e.Depth(engine.SideSell, 1<<30, nil) {
		out[false][l.Price] = l.Qty
	}
	return out
}

// pingBarrier sends a ping and applies everything up to the pong. Since a
// connection's queue is ordered, all book messages published before the ping
// have been processed by then.
func (w *wsClient) pingBarrier() {
	w.send(map[string]string{"op": "ping"})
	for {
		m := w.read()
		if m["type"] == "pong" {
			return
		}
		require.True(w.t, w.apply(m), "gap while draining")
	}
}

// Acceptance (SPEC phase 4): a WS client rebuilds the EXACT book from a
// snapshot plus deltas while the market keeps moving, and the result equals
// the matching engine state.
func TestWSClientRebuildsExactBookFromSnapshotAndDeltas(t *testing.T) {
	e := newEnv(t)
	cmds := commands(1, 3000)
	for _, c := range cmds[:800] { // the market already has history when the client connects
		e.feed(t, c)
	}
	w := e.dial(t)
	w.send(map[string]any{"op": "subscribe", "channels": []string{"book.BTC-USDT"}})
	require.Equal(t, "subscribed", w.read()["type"])
	snap := w.read()
	require.Equal(t, "snapshot", snap["type"])
	w.apply(snap)
	require.Equal(t, engineBook(e.eng), w.book, "snapshot equals the engine book at subscription time")

	// A realistic client reads continuously on its own goroutine while the market keeps moving.
	pong := make(chan struct{})
	go func() {
		for {
			m := w.read()
			if m["type"] == "pong" {
				close(pong)
				return
			}
			if !w.apply(m) {
				t.Error("gap detected while streaming")
				close(pong)
				return
			}
		}
	}()
	for _, c := range cmds[800:] {
		e.feed(t, c)
	}
	w.send(map[string]string{"op": "ping"}) // queue order: everything published before it arrives before the pong
	select {
	case <-pong:
	case <-time.After(10 * time.Second):
		t.Fatal("no pong")
	}
	assert.Equal(t, engineBook(e.eng), w.book, "snapshot + deltas == engine state")
}

func TestClientDetectsMissedDeltaAndResyncsByResubscribing(t *testing.T) {
	e := newEnv(t)
	w := e.dial(t)
	w.send(map[string]any{"op": "subscribe", "channels": []string{"book.BTC-USDT"}})
	w.read()
	w.apply(w.read())

	cmds := commands(2, 400)
	for _, c := range cmds[:200] {
		e.feed(t, c)
	}
	// Simulate a lost message: read all deltas but drop one before applying.
	w.send(map[string]string{"op": "ping"})
	var deltas []map[string]any
	for {
		m := w.read()
		if m["type"] == "pong" {
			break
		}
		deltas = append(deltas, m)
	}
	require.Greater(t, len(deltas), 10)
	gap := false
	for i, m := range deltas {
		if i == 5 {
			continue // lost
		}
		if !w.apply(m) {
			gap = true
			break
		}
	}
	require.True(t, gap, "the prev chain exposes the lost message")

	// Resync: unsubscribe + subscribe again returns a fresh snapshot.
	w.send(map[string]any{"op": "unsubscribe", "channels": []string{"book.BTC-USDT"}})
	w.send(map[string]any{"op": "subscribe", "channels": []string{"book.BTC-USDT"}})
	for {
		m := w.read()
		if m["type"] == "snapshot" {
			w.apply(m)
			break
		}
	}
	for _, c := range cmds[200:] {
		e.feed(t, c)
	}
	w.pingBarrier()
	assert.Equal(t, engineBook(e.eng), w.book, "after the resync the client is exact again")
}

// Latency (SPEC §5: book delta visible on WS < 100 ms, p99): measured here
// in-process from Apply() to the client receiving the frame.
func TestDeltaDeliveryLatencyP99(t *testing.T) {
	e := newEnv(t)
	w := e.dial(t)
	w.send(map[string]any{"op": "subscribe", "channels": []string{"book.BTC-USDT"}})
	w.read()
	w.apply(w.read())

	var mu sync.Mutex
	var lat []time.Duration
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			m := w.read()
			if m["type"] == "pong" {
				return
			}
			if m["type"] == "delta" {
				mu.Lock()
				lat = append(lat, time.Since(sent(m)))
				mu.Unlock()
			}
		}
	}()
	// Each command stamps its wall-clock time into the engine event time via ts below.
	for _, c := range commands(3, 1500) {
		e.now += int64(time.Second)
		c.Time = e.now
		e.buf = e.eng.Process(c, e.buf[:0])
		for _, ev := range e.buf {
			stampMu.Lock()
			stamps[ev.Seq] = time.Now()
			stampMu.Unlock()
			require.NoError(t, e.svc.Apply(toProto(ev), time.Now()))
		}
	}
	w.send(map[string]string{"op": "ping"})
	<-done
	mu.Lock()
	defer mu.Unlock()
	require.Greater(t, len(lat), 500)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p99 := lat[len(lat)*99/100]
	t.Logf("%d deltas, p50=%v p99=%v max=%v", len(lat), lat[len(lat)/2], p99, lat[len(lat)-1])
	assert.Less(t, p99, 100*time.Millisecond)
}

var (
	stampMu sync.Mutex
	stamps  = map[uint64]time.Time{}
)

func sent(m map[string]any) time.Time {
	stampMu.Lock()
	defer stampMu.Unlock()
	return stamps[uint64(m["seq"].(float64))]
}

func TestTradesAndTickerChannels(t *testing.T) {
	e := newEnv(t)
	go e.svc.RunTickerFlusher(context.Background(), 20*time.Millisecond)
	w := e.dial(t)
	w.send(map[string]any{"op": "subscribe", "channels": []string{"trades.BTC-USDT", "ticker.BTC-USDT"}})
	for i := 0; i < 4; i++ { // 2x subscribed + trades_snapshot + ticker snapshot
		w.read()
	}
	e.feed(t, engine.Command{Type: engine.CmdPlace, OrderID: 1, UserID: 1, Side: engine.SideSell, OrderType: engine.Limit, TIF: engine.GTC, Price: 5_000_000, Qty: 50_000})
	e.feed(t, engine.Command{Type: engine.CmdPlace, OrderID: 2, UserID: 2, Side: engine.SideBuy, OrderType: engine.Limit, TIF: engine.GTC, Price: 5_000_000, Qty: 20_000})

	var trade, ticker map[string]any
	for trade == nil || ticker == nil {
		m := w.read()
		switch m["type"] {
		case "trade":
			trade = m
		case "ticker":
			ticker = m
		}
	}
	assert.Equal(t, "50000", trade["price"])
	assert.Equal(t, "0.2", trade["qty"])
	assert.Equal(t, "buy", trade["side"], "taker side")
	assert.Equal(t, "50000", ticker["last"])
	assert.Equal(t, "0.2", ticker["volume"])
	assert.Equal(t, "10000", ticker["quote_volume"])
}

func TestProtocolErrors(t *testing.T) {
	e := newEnv(t)
	w := e.dial(t)
	for _, tc := range []struct {
		send any
		want string
	}{
		{map[string]any{"op": "subscribe", "channels": []string{"book.NOPE-USDT"}}, "unknown market"},
		{map[string]any{"op": "subscribe", "channels": []string{"orders.BTC-USDT"}}, "unknown channel"},
		{map[string]any{"op": "subscribe", "channels": []string{"candles.BTC-USDT.7m"}}, "unknown interval"},
		{map[string]any{"op": "fly"}, "unknown op"},
	} {
		w.send(tc.send)
		m := w.read()
		assert.Equal(t, "error", m["type"])
		assert.Contains(t, m["message"], tc.want)
	}
	require.NoError(t, w.c.Write(context.Background(), websocket.MessageText, []byte("{not json")))
	assert.Contains(t, w.read()["message"], "malformed")

	var many []string
	for i := 0; i < 60; i++ {
		many = append(many, fmt.Sprintf("candles.BTC-USDT.1m#%d", i))
	}
	w.send(map[string]any{"op": "subscribe", "channels": many})
	assert.Equal(t, "error", w.read()["type"])
}

func TestPerIPConnectionLimit(t *testing.T) {
	e := newEnv(t)
	var open []*wsClient
	for i := 0; i < 3; i++ {
		open = append(open, e.dial(t))
	}
	_, resp, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(e.srv.URL, "http")+"/ws/public", nil)
	defer closeBody(resp)
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	require.NoError(t, open[0].c.Close(websocket.StatusNormalClosure, "bye"))
	require.Eventually(t, func() bool {
		c, resp, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(e.srv.URL, "http")+"/ws/public", nil)
		closeBody(resp)
		if err == nil {
			_ = c.CloseNow()
		}
		return err == nil
	}, 5*time.Second, 50*time.Millisecond, "a slot is free again after a client disconnects")
}

func get(t *testing.T, e *env, path string) (int, map[string]any) {
	t.Helper()
	res, err := http.Get(e.srv.URL + path)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	var m map[string]any
	_ = json.NewDecoder(res.Body).Decode(&m)
	return res.StatusCode, m
}

func TestRESTBookTradesTicker(t *testing.T) {
	e := newEnv(t)
	for i, p := range []int64{5_000_000, 5_000_100, 5_000_200} {
		e.feed(t, engine.Command{Type: engine.CmdPlace, OrderID: uint64(1 + i), UserID: 1, Side: engine.SideSell, OrderType: engine.Limit, TIF: engine.GTC, Price: p, Qty: 10_000})
	}
	e.feed(t, engine.Command{Type: engine.CmdPlace, OrderID: 10, UserID: 2, Side: engine.SideBuy, OrderType: engine.Limit, TIF: engine.GTC, Price: 4_999_000, Qty: 30_000})
	e.feed(t, engine.Command{Type: engine.CmdPlace, OrderID: 11, UserID: 3, Side: engine.SideBuy, OrderType: engine.Limit, TIF: engine.IOC, Price: 5_000_100, Qty: 15_000})

	code, book := get(t, e, "/v1/market/BTC-USDT/book?depth=1")
	assert.Equal(t, 200, code)
	asks := book["asks"].([]any)
	require.Len(t, asks, 1, "depth limits the levels")
	assert.Equal(t, []any{"50001", "0.05"}, asks[0], "best ask first, decimals")
	assert.Equal(t, []any{[]any{"49990", "0.3"}}, book["bids"])

	code, tr := get(t, e, "/v1/market/BTC-USDT/trades?limit=10")
	assert.Equal(t, 200, code)
	trades := tr["trades"].([]any)
	require.Len(t, trades, 2)
	assert.Equal(t, "50001", trades[0].(map[string]any)["price"], "newest first")
	assert.Equal(t, "50000", trades[1].(map[string]any)["price"])

	code, tk := get(t, e, "/v1/market/BTC-USDT/ticker")
	assert.Equal(t, 200, code)
	assert.Equal(t, "50001", tk["last"])
	assert.Equal(t, "50000", tk["open"])
	assert.Equal(t, "0.15", tk["volume"])

	code, all := get(t, e, "/v1/market/tickers")
	assert.Equal(t, 200, code)
	assert.Len(t, all["tickers"], 1)

	code, _ = get(t, e, "/v1/market/NOPE/book")
	assert.Equal(t, 404, code)
}

func TestRESTCandlesProxyOrderingAndErrors(t *testing.T) {
	e := newEnv(t)
	e.fc.resp = &candlesv1.GetCandlesResponse{Candles: []*candlesv1.Candle{ // backend answers newest first
		{OpenTimeUnix: 120, Open: 5_000_100, High: 5_000_200, Low: 5_000_000, Close: 5_000_150, VolumeLots: 30_000, Turnover: "150004500000", Trades: 3},
		{OpenTimeUnix: 60, Open: 5_000_000, High: 5_000_000, Low: 5_000_000, Close: 5_000_000, VolumeLots: 10_000, Turnover: "50000000000", Trades: 1},
	}}
	code, out := get(t, e, "/v1/market/BTC-USDT/candles?interval=5m&from=10&to=999&limit=7")
	assert.Equal(t, 200, code)
	assert.Equal(t, "BTC-USDT", e.fc.req.Market)
	assert.Equal(t, "5m", e.fc.req.Interval)
	assert.EqualValues(t, 10, e.fc.req.FromUnix)
	assert.EqualValues(t, 7, e.fc.req.Limit)
	cs := out["candles"].([]any)
	require.Len(t, cs, 2)
	assert.EqualValues(t, 60, cs[0].(map[string]any)["t"], "REST returns oldest first for charting")
	assert.Equal(t, "50000", cs[0].(map[string]any)["o"])
	assert.Equal(t, "0.1", cs[0].(map[string]any)["v"])
	assert.Equal(t, "5000", cs[0].(map[string]any)["quote_volume"])

	code, _ = get(t, e, "/v1/market/BTC-USDT/candles?interval=7m")
	assert.Equal(t, 400, code)
	code, _ = get(t, e, "/v1/market/BTC-USDT/candles?from=50&to=10")
	assert.Equal(t, 400, code)
	e.fc.err = status.Error(codes.Unavailable, "down")
	code, _ = get(t, e, "/v1/market/BTC-USDT/candles")
	assert.Equal(t, 503, code)
}

func TestApplyCandlePublishesToSubscribers(t *testing.T) {
	e := newEnv(t)
	w := e.dial(t)
	w.send(map[string]any{"op": "subscribe", "channels": []string{"candles.BTC-USDT.1m"}})
	require.Equal(t, "subscribed", w.read()["type"])
	e.svc.ApplyCandle(&candlesv1.CandleUpdate{Market: "BTC-USDT", Interval: "1m", Candle: &candlesv1.Candle{
		OpenTimeUnix: 60, Open: 5_000_000, High: 5_000_100, Low: 5_000_000, Close: 5_000_100, VolumeLots: 20_000, Turnover: "100002000000", Trades: 2}}, time.Now())
	m := w.read()
	assert.Equal(t, "candle", m["type"])
	assert.Equal(t, "candles.BTC-USDT.1m", m["channel"])
	assert.Equal(t, "50001", m["c"])
	assert.Equal(t, "0.2", m["v"])
}

// closeBody releases the handshake response (Dial already consumed a 101 body).
func closeBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}
