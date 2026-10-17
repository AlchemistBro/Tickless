//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ETH-USDT is used for the market-data scenarios so they do not interfere with
// the BTC-USDT book of the trading scenarios. Prices are unique per run.
const ethMarket = "ETH-USDT"

func uniquePrice() string {
	return fmt.Sprintf("%d.%02d", 2500+rand.Intn(1000), rand.Intn(100))
}

// wsConn collects every message in the background so assertions never depend
// on the order in which independent channels deliver (a candle can arrive
// before or after a ticker for the same trade).
type wsConn struct {
	t    *testing.T
	c    *websocket.Conn
	mu   sync.Mutex
	msgs []map[string]any
}

func dialWS(t *testing.T, path string) *wsConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(gatewayURL, "http")+path, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.CloseNow() })
	w := &wsConn{t: t, c: c}
	go func() {
		for {
			_, b, err := c.Read(context.Background())
			if err != nil {
				return
			}
			var m map[string]any
			if json.Unmarshal(b, &m) == nil {
				w.mu.Lock()
				w.msgs = append(w.msgs, m)
				w.mu.Unlock()
			}
		}
	}()
	return w
}

func (w *wsConn) send(v any) {
	b, _ := json.Marshal(v)
	require.NoError(w.t, w.c.Write(context.Background(), websocket.MessageText, b))
}

// waitFor returns the first collected message satisfying match, waiting up to timeout.
func (w *wsConn) waitFor(timeout time.Duration, what string, match func(map[string]any) bool) map[string]any {
	w.t.Helper()
	var found map[string]any
	require.Eventually(w.t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		for _, m := range w.msgs {
			if match(m) {
				found = m
				return true
			}
		}
		return false
	}, timeout, 20*time.Millisecond, "never received: %s", what)
	return found
}

// count returns how many collected messages satisfy match.
func (w *wsConn) count(match func(map[string]any) bool) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, m := range w.msgs {
		if match(m) {
			n++
		}
	}
	return n
}

func ofType(typ string) func(map[string]any) bool {
	return func(m map[string]any) bool { return m["type"] == typ }
}

func getJSON(t *testing.T, path string, v any) int {
	t.Helper()
	res, err := http.Get(gatewayURL + path)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	_ = json.NewDecoder(res.Body).Decode(v)
	return res.StatusCode
}

func tradersWithFunds(t *testing.T) (seller, buyer *user) {
	seller, buyer = newUser(t, "md-seller"), newUser(t, "md-buyer")
	seed(t, seller, "ETH", "10000000000000000000") // 10 ETH
	seed(t, buyer, "USDT", "100000000000")         // 100,000 USDT
	seller.waitBalance("ETH", "10", "0")
	buyer.waitBalance("USDT", "100000", "0")
	return
}

// TestPublicMarketDataFlowsThroughTheGateway: a trade shows up on every public
// WebSocket channel and on the REST endpoints, and the candle history served
// from ClickHouse is consistent with the trades.
func TestPublicMarketDataFlowsThroughTheGateway(t *testing.T) {
	seller, buyer := tradersWithFunds(t)
	price := uniquePrice()

	ws := dialWS(t, "/ws/public")
	ws.send(map[string]any{"op": "subscribe", "channels": []string{"book." + ethMarket, "trades." + ethMarket, "ticker." + ethMarket, "candles." + ethMarket + ".1m"}})
	ws.waitFor(10*time.Second, "book snapshot", ofType("snapshot"))

	r, sell := seller.place(limit(ethMarket, "sell", price, "1", ""))
	require.Equal(t, 201, r.Status, string(r.Body))
	ws.waitFor(10*time.Second, "ask delta for the new level", func(m map[string]any) bool {
		return m["type"] == "delta" && m["side"] == "sell" && samePrice(m["price"].(string), price)
	})

	r, buy := buyer.place(limit(ethMarket, "buy", price, "0.4", ""))
	require.Equal(t, 201, r.Status, string(r.Body))
	buyer.waitOrder(buy.ID, func(o order) bool { return o.Status == "filled" }, "filled")

	trade := ws.waitFor(10*time.Second, "trade message", ofType("trade"))
	assert.Equal(t, "0.4", trade["qty"])
	assert.Equal(t, "buy", trade["side"], "taker side")
	ws.waitFor(10*time.Second, "ticker update", ofType("ticker"))
	candle := ws.waitFor(15*time.Second, "live candle update (ClickHouse -> candles.live -> WS)", ofType("candle"))
	assert.Equal(t, "candles."+ethMarket+".1m", candle["channel"])

	// REST agrees.
	var book struct {
		Seq  float64
		Asks [][]string
	}
	require.Equal(t, 200, getJSON(t, "/v1/market/"+ethMarket+"/book?depth=500", &book))
	found := false
	for _, a := range book.Asks {
		if samePrice(a[0], price) {
			found = true
			assert.Equal(t, "0.6", a[1], "1 ETH resting minus the 0.4 traded")
		}
	}
	assert.True(t, found, "our ask level is in the REST book")
	var trades struct {
		Trades []struct{ Price, Qty, Side string }
	}
	require.Equal(t, 200, getJSON(t, "/v1/market/"+ethMarket+"/trades?limit=5", &trades))
	require.NotEmpty(t, trades.Trades)
	assert.True(t, samePrice(trades.Trades[0].Price, price))
	assert.Equal(t, "0.4", trades.Trades[0].Qty)
	var tk struct{ Last, Volume string }
	require.Equal(t, 200, getJSON(t, "/v1/market/"+ethMarket+"/ticker", &tk))
	assert.True(t, samePrice(tk.Last, price))

	// Clean up the resting ask so reruns start from a clean book.
	require.Equal(t, 202, call(t, "DELETE", "/v1/orders/"+sell.ID, seller.Access, nil, nil).Status)

	assertCandlesMatchTrades(t)
}

func samePrice(a, b string) bool {
	x, e1 := strconv.ParseFloat(a, 64) // tests may use floats; the product never does
	y, e2 := strconv.ParseFloat(b, 64)
	return e1 == nil && e2 == nil && x == y
}

type restTrade struct {
	Price, Qty, Time string
}

type restCandle struct {
	T             int64
	O, H, L, C, V string
	Trades        int64  `json:"n"`
	QuoteVolume   string `json:"quote_volume"`
}

// assertCandlesMatchTrades recomputes OHLCV for several intervals from the
// public trade list and compares them with the candles from ClickHouse. The
// oldest bucket of the trade window can be partial, so it is skipped when the
// window is full.
func assertCandlesMatchTrades(t *testing.T) {
	t.Helper()
	var tr struct{ Trades []restTrade }
	require.Equal(t, 200, getJSON(t, "/v1/market/"+ethMarket+"/trades?limit=500", &tr))
	require.NotEmpty(t, tr.Trades)
	sort.Slice(tr.Trades, func(i, j int) bool { return tr.Trades[i].Time < tr.Trades[j].Time }) // oldest first (RFC3339Nano sorts lexically)

	for _, iv := range []struct {
		name string
		secs int64
	}{{"1m", 60}, {"5m", 300}, {"1h", 3600}, {"1d", 86400}} {
		type agg struct{ o, h, l, c, v float64 }
		want := map[int64]*agg{}
		var order []int64
		for _, x := range tr.Trades {
			ts, err := time.Parse(time.RFC3339Nano, x.Time)
			require.NoError(t, err)
			b := ts.Unix() / iv.secs * iv.secs
			p, _ := strconv.ParseFloat(x.Price, 64)
			q, _ := strconv.ParseFloat(x.Qty, 64)
			a := want[b]
			if a == nil {
				a = &agg{o: p, h: p, l: p}
				want[b] = a
				order = append(order, b)
			}
			a.h, a.l, a.c = max(a.h, p), min(a.l, p), p
			a.v += q
		}
		if len(tr.Trades) >= 500 {
			delete(want, order[0]) // possibly partial
		}
		require.Eventually(t, func() bool {
			var cs struct{ Candles []restCandle }
			q := url.Values{"interval": {iv.name}, "limit": {"2000"}}
			if getJSON(t, "/v1/market/"+ethMarket+"/candles?"+q.Encode(), &cs) != 200 {
				return false
			}
			got := map[int64]restCandle{}
			for _, c := range cs.Candles {
				got[c.T] = c
			}
			for b, w := range want {
				c, ok := got[b]
				if !ok {
					return false
				}
				o, _ := strconv.ParseFloat(c.O, 64)
				h, _ := strconv.ParseFloat(c.H, 64)
				l, _ := strconv.ParseFloat(c.L, 64)
				cl, _ := strconv.ParseFloat(c.C, 64)
				v, _ := strconv.ParseFloat(c.V, 64)
				if o != w.o || h != w.h || l != w.l || cl != w.c || fmt.Sprintf("%.6f", v) != fmt.Sprintf("%.6f", w.v) {
					return false
				}
			}
			return true
		}, 30*time.Second, 500*time.Millisecond, "candles of interval %s must equal the aggregation of the trades", iv.name)
	}
}

// TestPrivateWebSocketDeliversOnlyOwnEvents: orders, fills and balances arrive
// on /ws/private for the authenticated user and for nobody else.
func TestPrivateWebSocketDeliversOnlyOwnEvents(t *testing.T) {
	seller, buyer := tradersWithFunds(t)
	price := uniquePrice()

	sellerWS := dialWS(t, "/ws/private")
	sellerWS.send(map[string]string{"op": "auth", "token": seller.Access})
	sellerWS.waitFor(10*time.Second, "authenticated", ofType("authenticated"))
	buyerWS := dialWS(t, "/ws/private")
	buyerWS.send(map[string]string{"op": "auth", "token": buyer.Access})
	buyerWS.waitFor(10*time.Second, "authenticated", ofType("authenticated"))

	bystander := newUser(t, "md-bystander")
	byWS := dialWS(t, "/ws/private")
	byWS.send(map[string]string{"op": "auth", "token": bystander.Access})
	byWS.waitFor(10*time.Second, "authenticated", ofType("authenticated"))

	r, sell := seller.place(limit(ethMarket, "sell", price, "1", ""))
	require.Equal(t, 201, r.Status, string(r.Body))
	acc := sellerWS.waitFor(10*time.Second, "order accepted", func(m map[string]any) bool { return m["type"] == "order" && m["event"] == "accepted" })
	assert.Equal(t, sell.ID, acc["order_id"])
	bal := sellerWS.waitFor(10*time.Second, "balance update for the hold", ofType("balance"))
	assert.Equal(t, "ETH", bal["asset"])

	r, buy := buyer.place(limit(ethMarket, "buy", price, "0.25", ""))
	require.Equal(t, 201, r.Status, string(r.Body))
	fill := buyerWS.waitFor(10*time.Second, "buyer fill", ofType("fill"))
	assert.Equal(t, "taker", fill["role"])
	assert.Equal(t, "0.25", fill["qty"])
	assert.Equal(t, buy.ID, fill["order_id"])
	mfill := sellerWS.waitFor(10*time.Second, "seller fill", ofType("fill"))
	assert.Equal(t, "maker", mfill["role"])
	assert.Equal(t, sell.ID, mfill["order_id"])
	buyerWS.waitFor(10*time.Second, "buyer balance", func(m map[string]any) bool { return m["type"] == "balance" && m["asset"] == "ETH" })

	// The bystander saw none of it.
	byWS.send(map[string]string{"op": "ping"})
	byWS.waitFor(10*time.Second, "pong", ofType("pong"))
	assert.Zero(t, byWS.count(func(m map[string]any) bool {
		return m["type"] == "order" || m["type"] == "fill" || m["type"] == "balance"
	}),
		"no foreign events were delivered to an unrelated user")

	// Authentication is mandatory.
	anon := dialWS(t, "/ws/private")
	anon.send(map[string]string{"op": "auth", "token": "garbage"})
	e := anon.waitFor(10*time.Second, "error", ofType("error"))
	assert.Equal(t, "invalid token", e["message"])

	require.Equal(t, 202, call(t, "DELETE", "/v1/orders/"+sell.ID, seller.Access, nil, nil).Status)
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", append([]string{"compose", "-f", "../deploy/compose/docker-compose.yml"}, args...)...).CombinedOutput()
	require.NoError(t, err, string(out))
}

// TestMarketDataSurvivesRestart: after marketdata restarts it resumes from its
// Redis snapshot plus the log and serves exactly the same book (the exact
// state, not "similar").
func TestMarketDataSurvivesRestart(t *testing.T) {
	seller, buyer := tradersWithFunds(t)
	price := uniquePrice()
	_, sell := seller.place(limit(ethMarket, "sell", price, "2", ""))
	_, _ = buyer.place(limit(ethMarket, "buy", price, "0.5", ""))
	require.Eventually(t, func() bool {
		var b struct{ Asks [][]string }
		getJSON(t, "/v1/market/"+ethMarket+"/book?depth=500", &b)
		for _, a := range b.Asks {
			if samePrice(a[0], price) && a[1] == "1.5" {
				return true
			}
		}
		return false
	}, 20*time.Second, 200*time.Millisecond)

	var before struct {
		Seq        float64
		Bids, Asks [][]string
	}
	getJSON(t, "/v1/market/"+ethMarket+"/book?depth=500", &before)

	compose(t, "restart", "marketdata")
	require.Eventually(t, func() bool { return call2(gatewayURL+"/v1/market/"+ethMarket+"/book") == 200 }, 60*time.Second, 500*time.Millisecond)

	var after struct {
		Seq        float64
		Bids, Asks [][]string
	}
	require.Equal(t, 200, getJSON(t, "/v1/market/"+ethMarket+"/book?depth=500", &after))
	assert.Equal(t, before, after, "identical book and sequence after the restart")

	// And it keeps following the market.
	_, buy2 := buyer.place(limit(ethMarket, "buy", price, "0.5", ""))
	buyer.waitOrder(buy2.ID, func(o order) bool { return o.Status == "filled" }, "filled")
	require.Eventually(t, func() bool {
		var b struct{ Asks [][]string }
		getJSON(t, "/v1/market/"+ethMarket+"/book?depth=500", &b)
		for _, a := range b.Asks {
			if samePrice(a[0], price) && a[1] == "1" {
				return true
			}
		}
		return false
	}, 20*time.Second, 200*time.Millisecond, "new trades after the restart are applied")
	require.Equal(t, 202, call(t, "DELETE", "/v1/orders/"+sell.ID, seller.Access, nil, nil).Status)
}

// TestWebSocketDeliveryLatencyP99 reads the server-side histogram
// ws_delivery_seconds (Kafka record timestamp -> WebSocket write) from
// Prometheus after a burst of trades. SPEC §5: p99 < 100 ms.
func TestWebSocketDeliveryLatencyP99(t *testing.T) {
	seller, buyer := tradersWithFunds(t)
	price := uniquePrice()
	ws := dialWS(t, "/ws/public")
	ws.send(map[string]any{"op": "subscribe", "channels": []string{"book." + ethMarket, "trades." + ethMarket}})
	_, sell := seller.place(limit(ethMarket, "sell", price, "5", ""))
	for i := 0; i < 60; i++ {
		_, _ = buyer.place(limit(ethMarket, "buy", price, "0.01", ""))
	}
	time.Sleep(12 * time.Second) // metrics are pushed every 5 s and scraped every 5 s

	q := `histogram_quantile(0.99, sum by (le) (increase(ws_delivery_seconds_bucket{hub="public"}[5m])))`
	var res struct {
		Data struct{ Result []struct{ Value []any } }
	}
	require.Eventually(t, func() bool {
		r, err := http.Get("http://localhost:9090/api/v1/query?query=" + url.QueryEscape(q))
		if err != nil {
			return false
		}
		defer func() { _ = r.Body.Close() }()
		res.Data.Result = nil
		_ = json.NewDecoder(r.Body).Decode(&res)
		return len(res.Data.Result) > 0
	}, 40*time.Second, time.Second, "Prometheus has the ws_delivery_seconds histogram")
	p99, err := strconv.ParseFloat(res.Data.Result[0].Value[1].(string), 64)
	require.NoError(t, err)
	t.Logf("public WebSocket delivery p99 = %.1f ms (Kafka timestamp -> socket write)", p99*1000)
	assert.Less(t, p99, 0.1, "SPEC §5: book delta visible on WS in < 100 ms (p99)")

	require.Equal(t, 202, call(t, "DELETE", "/v1/orders/"+sell.ID, seller.Access, nil, nil).Status)
}
