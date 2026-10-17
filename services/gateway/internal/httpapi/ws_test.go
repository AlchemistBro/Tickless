package httpapi_test

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/gateway/internal/httpapi"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/market"
	"github.com/tickless/pkg/wshub"
)

type oneSpec struct{}

func (oneSpec) Spec(sym string) (market.Spec, bool) {
	return market.Spec{Symbol: "BTC-USDT", Base: "BTC", Quote: "USDT", BaseDecimals: 8, QuoteDecimals: 6, TickSize: 10_000, LotSize: 1_000}, sym == "BTC-USDT"
}

// gatewayServer serves the gateway routes over a real listener (WebSocket needs one).
func gatewayServer(t *testing.T, e *testEnv, mutate func(*httpapi.Deps)) (*httptest.Server, *httpapi.Server) {
	t.Helper()
	d := httpapi.Deps{Identity: e.identity, Ledger: fakeLedger{}, Orders: e.orders, Redis: e.rdb, Limits: generousLimits,
		PublicKey: func(context.Context) (ed25519.PublicKey, error) { return e.identity.pub(), nil },
		Now:       func() time.Time { return time.Unix(0, e.now.Load()) }, Markets: oneSpec{}, WSAuthTimeout: 5 * time.Second}
	if mutate != nil {
		mutate(&d)
	}
	api := httpapi.New(d)
	srv := httptest.NewServer(api.Routes())
	t.Cleanup(srv.Close)
	return srv, api
}

func dialWS(t *testing.T, base, path string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(base, "http")+path, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

func wsSend(t *testing.T, c *websocket.Conn, v any) {
	b, _ := json.Marshal(v)
	require.NoError(t, c.Write(context.Background(), websocket.MessageText, b))
}

func wsRead(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, b, err := c.Read(ctx)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(b, &m), string(b))
	return m
}

func wsExpectClosed(t *testing.T, c *websocket.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		if _, _, err := c.Read(ctx); err != nil {
			require.NotErrorIs(t, err, context.DeadlineExceeded, "the server should have closed the connection")
			return
		}
	}
}

func TestPrivateWSRequiresAuthAndIsolatesUsers(t *testing.T) {
	e := newEnv(t, generousLimits)
	srv, api := gatewayServer(t, e, nil)

	// Before authentication nothing is served.
	anon := dialWS(t, srv.URL, "/ws/private")
	wsSend(t, anon, map[string]string{"op": "subscribe"})
	assert.Equal(t, "authenticate first", wsRead(t, anon)["message"])

	bad := dialWS(t, srv.URL, "/ws/private")
	wsSend(t, bad, map[string]string{"op": "auth", "token": "not-a-token"})
	assert.Equal(t, "invalid token", wsRead(t, bad)["message"])
	wsExpectClosed(t, bad)

	alice := dialWS(t, srv.URL, "/ws/private")
	wsSend(t, alice, map[string]string{"op": "auth", "token": e.token(7, "user")})
	auth := wsRead(t, alice)
	assert.Equal(t, "authenticated", auth["type"])
	assert.Equal(t, "7", auth["user_id"])
	bob := dialWS(t, srv.URL, "/ws/private")
	wsSend(t, bob, map[string]string{"op": "auth", "token": e.token(8, "user")})
	assert.Equal(t, "authenticated", wsRead(t, bob)["type"])

	ts := time.Now()
	// Alice has a resting buy; Bob sells into it; her balance changes; a duplicate rejection and her cancel follow.
	api.PublishMatching(&matchingv1.Event{Market: "BTC-USDT", Type: matchingv1.EventType_EVENT_TYPE_ACCEPTED, OrderId: 100, UserId: 7,
		Side: commonv1.Side_SIDE_BUY, Price: 5_000_000, Qty: 50_000}, ts)
	api.PublishMatching(&matchingv1.Event{Market: "BTC-USDT", Type: matchingv1.EventType_EVENT_TYPE_TRADE, OrderId: 101, UserId: 8,
		Side: commonv1.Side_SIDE_SELL, MakerOrderId: 100, MakerUserId: 7, Price: 5_000_000, Qty: 20_000, TakerLeaves: 0, MakerLeaves: 30_000, TradeId: 9}, ts)
	api.PublishBalance(&ledgerv1.BalanceChanged{UserId: 7, Asset: "USDT", Available: &commonv1.Amount{Units: "80000000000"}, Held: &commonv1.Amount{Units: "0"}}, ts)
	api.PublishMatching(&matchingv1.Event{Market: "BTC-USDT", Type: matchingv1.EventType_EVENT_TYPE_REJECTED, Reason: matchingv1.Reason_REASON_DUPLICATE_ORDER, OrderId: 100, UserId: 7}, ts)
	api.PublishMatching(&matchingv1.Event{Market: "BTC-USDT", Type: matchingv1.EventType_EVENT_TYPE_CANCELLED, Reason: matchingv1.Reason_REASON_CANCEL_USER, OrderId: 100, UserId: 7, Qty: 30_000}, ts)

	m := wsRead(t, alice)
	assert.Equal(t, "order", m["type"])
	assert.Equal(t, "accepted", m["event"])
	assert.Equal(t, "100", m["order_id"], "ids are strings")
	assert.Equal(t, "50000", m["price"])
	assert.Equal(t, "0.5", m["qty"])
	m = wsRead(t, alice)
	assert.Equal(t, "fill", m["type"])
	assert.Equal(t, "maker", m["role"])
	assert.Equal(t, "buy", m["side"], "the maker side is the opposite of the taker side")
	assert.Equal(t, "0.2", m["qty"])
	assert.Equal(t, "0.3", m["leaves"])
	m = wsRead(t, alice)
	assert.Equal(t, "balance", m["type"])
	assert.Equal(t, "80000", m["available"], "units are formatted with the asset decimals")
	m = wsRead(t, alice)
	assert.Equal(t, "cancelled", m["event"], "the duplicate rejection is not delivered")

	m = wsRead(t, bob)
	assert.Equal(t, "fill", m["type"])
	assert.Equal(t, "taker", m["role"])
	assert.Equal(t, "sell", m["side"])
	wsSend(t, bob, map[string]string{"op": "ping"})
	assert.Equal(t, "pong", wsRead(t, bob)["type"], "Bob received nothing of Alice events: his next message is his own pong")
}

func TestPrivateWSRefusesFrozenUsersAndUnauthenticatedTimeout(t *testing.T) {
	e := newEnv(t, generousLimits)
	srv, _ := gatewayServer(t, e, nil)
	_, err := e.redis.SAdd(httpapi.FrozenSet, "9")
	require.NoError(t, err)

	c := dialWS(t, srv.URL, "/ws/private")
	wsSend(t, c, map[string]string{"op": "auth", "token": e.token(9, "user")})
	assert.Equal(t, "not allowed", wsRead(t, c)["message"])
	wsExpectClosed(t, c)

	// Fail closed: with Redis down the frozen check errors, and the user is refused too.
	ok := dialWS(t, srv.URL, "/ws/private")
	e.redis.Close()
	wsSend(t, ok, map[string]string{"op": "auth", "token": e.token(10, "user")})
	assert.Equal(t, "not allowed", wsRead(t, ok)["message"])
}

func TestMarketDataIsProxiedWithoutCredentials(t *testing.T) {
	var gotAuth, gotPath, gotQuery string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath, gotQuery = r.Header.Get("Authorization")+r.Header.Get("X-API-KEY"), r.URL.Path, r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"market":"BTC-USDT","seq":5,"bids":[],"asks":[]}`)
	}))
	defer backend.Close()
	e := newEnv(t, generousLimits)
	srv, _ := gatewayServer(t, e, func(d *httpapi.Deps) { d.MarketData = backend.URL })

	req, _ := http.NewRequest("GET", srv.URL+"/v1/market/BTC-USDT/book?depth=5", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("X-API-KEY", "tk_1")
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	assert.Equal(t, 200, res.StatusCode)
	assert.Contains(t, string(body), `"seq":5`)
	assert.Equal(t, "/v1/market/BTC-USDT/book", gotPath)
	assert.Equal(t, "depth=5", gotQuery)
	assert.Empty(t, gotAuth, "credentials are never forwarded to marketdata")
	assert.Equal(t, "no-store", res.Header.Get("Cache-Control"))

	backend.Close()
	res, err = http.Get(srv.URL + "/v1/market/tickers")
	require.NoError(t, err)
	body, _ = io.ReadAll(res.Body)
	_ = res.Body.Close()
	assert.Equal(t, 502, res.StatusCode)
	assert.Contains(t, string(body), "market data temporarily unavailable")
	assert.NotContains(t, string(body), "127.0.0.1", "no internal addresses leak")
}

func TestPublicWebSocketIsProxiedToMarketData(t *testing.T) {
	hub := wshub.New("upstream")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.Serve(w, r, wshub.Handler{OnMessage: func(c *wshub.Conn, data []byte) { c.Send(append([]byte("echo:"), data...), time.Time{}) }})
	}))
	defer upstream.Close()
	e := newEnv(t, generousLimits)
	srv, _ := gatewayServer(t, e, func(d *httpapi.Deps) { d.MarketData = upstream.URL })

	c := dialWS(t, srv.URL, "/ws/public")
	require.NoError(t, c.Write(context.Background(), websocket.MessageText, []byte("hello")))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, b, err := c.Read(ctx)
	require.NoError(t, err)
	assert.Equal(t, "echo:hello", string(b))
}

func TestPrivateWSDropsConnectionsThatNeverAuthenticate(t *testing.T) {
	e := newEnv(t, generousLimits)
	srv, _ := gatewayServer(t, e, func(d *httpapi.Deps) { d.WSAuthTimeout = 300 * time.Millisecond })
	idle := dialWS(t, srv.URL, "/ws/private")
	start := time.Now()
	wsExpectClosed(t, idle)
	assert.Less(t, time.Since(start), 3*time.Second)
}
