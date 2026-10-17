//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/pkg/auth"
)

func feeBalance(t *testing.T, asset string) string {
	t.Helper()
	var amt string
	require.NoError(t, ledgerDB(t).QueryRow(context.Background(),
		`SELECT COALESCE(sum(b.amount), 0)::text FROM balances b JOIN accounts a ON a.id = b.account_id WHERE a.kind = 'FEE' AND a.asset = $1`, asset).Scan(&amt))
	return amt
}

func limit(market, side, price, qty, cid string) map[string]any {
	m := map[string]any{"market": market, "side": side, "type": "limit", "price": price, "quantity": qty}
	if cid != "" {
		m["client_order_id"] = cid
	}
	return m
}

// TestTradeFlow is the Phase 3 acceptance scenario: two users trade over REST
// against seeded balances; fills, fees, holds and cancellation are all
// reflected in balances and the ledger invariants hold.
func TestTradeFlow(t *testing.T) {
	start := time.Now()
	lap := func(what string) { t.Logf("%6.2fs  %s", time.Since(start).Seconds(), what) }
	a, b := newUser(t, "buyer"), newUser(t, "seller")
	lap("two users signed up and logged in")
	seed(t, a, "USDT", "100000000000") // 100,000 USDT (6 decimals)
	seed(t, b, "BTC", "1000000000")    // 10 BTC (8 decimals)
	a.waitBalance("USDT", "100000", "0")
	b.waitBalance("BTC", "10", "0")
	lap("balances seeded and visible")
	feeBTC0, feeUSDT0 := feeBalance(t, "BTC"), feeBalance(t, "USDT")

	// Seller rests 1 BTC @ 50,000.
	r, sell := b.place(limit("BTC-USDT", "sell", "50000.00", "1", "sell-1"))
	require.Equal(t, 201, r.Status, string(r.Body))
	b.waitOrder(sell.ID, func(o order) bool { return o.Status == "new" }, "new")
	b.waitBalance("BTC", "9", "1")
	lap("sell order resting, hold placed")

	// Client-order-id idempotency: resubmitting returns the same order with 200.
	r, again := b.place(limit("BTC-USDT", "sell", "50000.00", "1", "sell-1"))
	require.Equal(t, 200, r.Status)
	assert.Equal(t, sell.ID, again.ID)
	b.waitBalance("BTC", "9", "1") // still one hold

	// Buyer takes 0.4 BTC.
	r, buy := a.place(limit("BTC-USDT", "buy", "50000.00", "0.4", "buy-1"))
	require.Equal(t, 201, r.Status, string(r.Body))
	a.waitOrder(buy.ID, func(o order) bool { return o.Status == "filled" }, "filled")
	lap("buy order filled")
	b.waitOrder(sell.ID, func(o order) bool { return o.Status == "partially_filled" && o.FilledQuantity == "0.4" }, "partially_filled 0.4")

	// Settlement: 0.4 BTC for 20,000 USDT; fees in the received asset (taker 20 bps base, maker 10 bps quote).
	a.waitBalance("USDT", "80000", "0")
	a.waitBalance("BTC", "0.3992", "0")
	lap("trade settled in the ledger")
	b.waitBalance("USDT", "19980", "0")
	b.waitBalance("BTC", "9", "0.6")

	// Cancel the remainder.
	r = call(t, "DELETE", "/v1/orders/"+sell.ID, b.Access, nil, nil)
	require.Equal(t, 202, r.Status, string(r.Body))
	b.waitOrder(sell.ID, func(o order) bool { return o.Status == "cancelled" }, "cancelled")
	b.waitBalance("BTC", "9.6", "0")

	// Fees went to the exchange: 0.0008 BTC = 80,000 sat and 20 USDT = 20,000,000 micro.
	require.Eventually(t, func() bool {
		return feeBalance(t, "BTC") == add(feeBTC0, "80000") && feeBalance(t, "USDT") == add(feeUSDT0, "20000000")
	}, 20*time.Second, 100*time.Millisecond, "fee income")

	// Ledger history is visible to the user.
	r = call(t, "GET", "/v1/ledger?asset=USDT", a.Access, nil, nil)
	require.Equal(t, 200, r.Status)
	assert.Contains(t, string(r.Body), `"kind":"trade"`)

	assertLedgerInvariants(t)

	if os.Getenv("E2E_EXPECT_DUPLICATES") == "true" {
		total, dups := kafkaDuplicates(t)
		t.Logf("matching.events: %d records, %d duplicated (market,seq) pairs", total, dups)
		assert.Positive(t, dups, "chaos mode must really duplicate Kafka messages")
	}
}

func add(a, b string) string {
	x, _ := strconv.ParseInt(a, 10, 64)
	y, _ := strconv.ParseInt(b, 10, 64)
	return strconv.FormatInt(x+y, 10)
}

// TestOrderTypesReleaseHolds exercises FOK, post-only and IOC through the full
// stack and checks that every hold is returned.
func TestOrderTypesReleaseHolds(t *testing.T) {
	maker, taker := newUser(t, "maker"), newUser(t, "taker")
	seed(t, maker, "BTC", "10000000")      // 0.1 BTC
	seed(t, taker, "USDT", "100000000000") // 100,000 USDT: enough to hold a 1 BTC order at 60,000
	taker.waitBalance("USDT", "100000", "0")

	r, rest := maker.place(limit("BTC-USDT", "sell", "60000.00", "0.1", ""))
	require.Equal(t, 201, r.Status, string(r.Body))
	maker.waitOrder(rest.ID, func(o order) bool { return o.Status == "new" }, "new")

	// FOK for more than is available: cancelled, nothing traded, funds returned.
	fok := limit("BTC-USDT", "buy", "60000.00", "1", "")
	fok["time_in_force"] = "FOK"
	r, o := taker.place(fok)
	require.Equal(t, 201, r.Status, string(r.Body))
	taker.waitOrder(o.ID, func(o order) bool { return o.Status == "cancelled" }, "cancelled (FOK)")
	taker.waitBalance("USDT", "100000", "0")

	// Post-only that would cross: rejected, funds returned.
	po := limit("BTC-USDT", "buy", "60000.00", "0.1", "")
	po["post_only"] = true
	r, o = taker.place(po)
	require.Equal(t, 201, r.Status, string(r.Body))
	got := taker.waitOrder(o.ID, func(o order) bool { return o.Status == "rejected" }, "rejected (post-only)")
	assert.Contains(t, got.RejectReason, "post only")
	taker.waitBalance("USDT", "100000", "0")

	// IOC for 1 BTC: fills the 0.1 available, remainder cancelled, excess hold released.
	ioc := limit("BTC-USDT", "buy", "60000.00", "1", "")
	ioc["time_in_force"] = "IOC"
	r, o = taker.place(ioc)
	require.Equal(t, 201, r.Status, string(r.Body))
	taker.waitOrder(o.ID, func(o order) bool { return o.Status == "cancelled" && o.FilledQuantity == "0.1" }, "cancelled with 0.1 filled (IOC)")
	taker.waitBalance("USDT", "94000", "0")
	taker.waitBalance("BTC", "0.0998", "0")
	maker.waitBalance("USDT", "5994", "0") // 6000 minus 10 bps maker fee
	maker.waitBalance("BTC", "0", "0")

	// Market buy with a protective bound.
	m2 := newUser(t, "marketmaker")
	seed(t, m2, "BTC", "10000000")
	_, rest2 := m2.place(limit("BTC-USDT", "sell", "59000.00", "0.1", ""))
	m2.waitOrder(rest2.ID, func(o order) bool { return o.Status == "new" }, "new")
	mk := newUser(t, "marketbuyer")
	seed(t, mk, "USDT", "10000000000") // 10,000 USDT
	mk.waitBalance("USDT", "10000", "0")
	mkt := map[string]any{"market": "BTC-USDT", "side": "buy", "type": "market", "price": "61000.00", "quantity": "0.1"}
	r, mo := mk.place(mkt)
	require.Equal(t, 201, r.Status, string(r.Body))
	mk.waitOrder(mo.ID, func(o order) bool { return o.Status == "filled" }, "filled (market)")
	// Held 6,100 at the bound, paid 5,900 at the maker price; the 200 excess is released.
	mk.waitBalance("USDT", "4100", "0")
	mk.waitBalance("BTC", "0.0998", "0")
	m2.waitBalance("USDT", "5894.1", "0") // 5,900 minus 10 bps maker fee
	assertLedgerInvariants(t)
}

func TestInsufficientFundsAndValidation(t *testing.T) {
	u := newUser(t, "poor")
	r, _ := u.place(limit("BTC-USDT", "buy", "50000.00", "1", ""))
	assert.Equal(t, 422, r.Status, string(r.Body))
	assert.Contains(t, string(r.Body), "insufficient funds")

	for name, body := range map[string]map[string]any{
		"off tick":      limit("BTC-USDT", "buy", "50000.001", "1", ""),
		"unknown":       limit("DOGE-USDT", "buy", "1", "1", ""),
		"market no cap": {"market": "BTC-USDT", "side": "buy", "type": "market", "quantity": "1"},
	} {
		r, _ := u.place(body)
		assert.GreaterOrEqual(t, r.Status, 400, name)
		assert.Less(t, r.Status, 500, name)
	}
	assert.Equal(t, 401, call(t, "GET", "/v1/balances", "", nil, nil).Status)
}

func TestAPIKeyTrading(t *testing.T) {
	u := newUser(t, "bot")
	seed(t, u, "USDT", "100000000")
	u.waitBalance("USDT", "100", "0")
	r := call(t, "POST", "/v1/api-keys", u.Access, map[string]any{"label": "bot", "can_trade": true}, nil)
	require.Equal(t, 201, r.Status, string(r.Body))
	var k struct {
		KeyID  string `json:"key_id"`
		Secret string `json:"secret"`
	}
	r.json(t, &k)

	signed := func(method, path string, body []byte) resp {
		ts := time.Now().Unix()
		return callRaw(t, method, path, body, map[string]string{
			"X-API-KEY": k.KeyID, "X-API-TIMESTAMP": strconv.FormatInt(ts, 10),
			"X-API-SIGNATURE": auth.SignRequest(k.Secret, ts, method, path, body)})
	}
	assert.Equal(t, 200, signed("GET", "/v1/balances", nil).Status)
	body, _ := json.Marshal(limit("BTC-USDT", "buy", "50000.00", "0.001", "bot-1"))
	assert.Equal(t, 201, signed("POST", "/v1/orders", body).Status)
	assert.Equal(t, 401, callRaw(t, "GET", "/v1/balances", nil, map[string]string{"X-API-KEY": k.KeyID, "X-API-TIMESTAMP": "1", "X-API-SIGNATURE": "bad"}).Status)

	require.Equal(t, 204, call(t, "DELETE", "/v1/api-keys/"+k.KeyID, u.Access, nil, nil).Status)
	// Revocation is cached at the gateway for up to 30 s.
	require.Eventually(t, func() bool { return signed("GET", "/v1/balances", nil).Status == 401 }, 45*time.Second, time.Second)
}

func callRaw(t *testing.T, method, path string, body []byte, hdr map[string]string) resp {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, gatewayURL+path, rd)
	require.NoError(t, err)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return resp{Status: res.StatusCode, Body: b, Header: res.Header}
}

func contains(b []byte, sub string) bool { return strings.Contains(string(b), sub) }

func TestAdminHaltAndFreeze(t *testing.T) {
	adminPw := env("TICKLESS_ADMIN_PASSWORD", "dev-admin-password")
	r := call(t, "POST", "/v1/auth/login", "", map[string]string{"email": "admin@tickless.local", "password": adminPw}, nil)
	require.Equal(t, 200, r.Status, string(r.Body))
	var tk struct {
		Access string `json:"access_token"`
	}
	r.json(t, &tk)

	u := newUser(t, "victim")
	seed(t, u, "USDT", "100000000")
	u.waitBalance("USDT", "100", "0")
	assert.Equal(t, 403, call(t, "GET", "/v1/admin/users", u.Access, nil, nil).Status, "plain users cannot use admin endpoints")

	// Halt: new orders are refused; resume restores trading.
	require.Equal(t, 200, call(t, "POST", "/v1/admin/markets/ETH-USDT/halt", tk.Access, nil, nil).Status)
	require.Eventually(t, func() bool {
		r, _ := u.place(limit("ETH-USDT", "buy", "3000.00", "0.01", ""))
		return r.Status == 422 && contains(r.Body, "halted")
	}, 10*time.Second, 200*time.Millisecond, "halt takes effect")
	require.Equal(t, 200, call(t, "POST", "/v1/admin/markets/ETH-USDT/resume", tk.Access, nil, nil).Status)

	// Freeze: the user is locked out immediately and cannot log in again.
	require.Equal(t, 204, call(t, "POST", "/v1/admin/users/"+u.ID+"/freeze", tk.Access, nil, nil).Status)
	assert.Equal(t, 403, call(t, "GET", "/v1/balances", u.Access, nil, nil).Status)
	r = call(t, "POST", "/v1/auth/login", "", map[string]string{"email": u.Email, "password": "correct horse battery staple"}, nil)
	assert.Equal(t, 403, r.Status)
	require.Equal(t, 204, call(t, "POST", "/v1/admin/users/"+u.ID+"/unfreeze", tk.Access, nil, nil).Status)
	assert.Equal(t, 200, call(t, "GET", "/v1/balances", u.Access, nil, nil).Status)

	// The admin can inspect any ledger.
	r = call(t, "GET", "/v1/admin/users/"+u.ID+"/ledger", tk.Access, nil, nil)
	require.Equal(t, 200, r.Status)
	assert.Contains(t, string(r.Body), "credit")
}

// TestTraceSpansAllServices checks the Jaeger trace of a taker order: one trace
// id from the REST call through orders, the outbox and Kafka to matching and
// the ledger settlement (async hops are continued from W3C headers).
func TestTraceSpansAllServices(t *testing.T) {
	a, b := newUser(t, "trace-buyer"), newUser(t, "trace-seller")
	seed(t, a, "USDT", "100000000000")
	seed(t, b, "BTC", "1000000000")
	a.waitBalance("USDT", "100000", "0")
	b.waitBalance("BTC", "10", "0")
	r, sell := b.place(limit("BTC-USDT", "sell", "50000.00", "0.1", ""))
	require.Equal(t, 201, r.Status)
	b.waitOrder(sell.ID, func(o order) bool { return o.Status == "new" }, "new")
	r, buy := a.place(limit("BTC-USDT", "buy", "50000.00", "0.1", ""))
	require.Equal(t, 201, r.Status)
	a.waitOrder(buy.ID, func(o order) bool { return o.Status == "filled" }, "filled")
	a.waitBalance("BTC", "0.0998", "0")

	want := []string{"gateway", "orders", "matching", "ledger"}
	var found string
	require.Eventually(t, func() bool {
		q := url.Values{"service": {"gateway"}, "operation": {"POST /v1/orders"}, "limit": {"50"}, "lookback": {"1h"}}
		res, err := http.Get(jaegerURL + "/api/traces?" + q.Encode())
		if err != nil || res.StatusCode != 200 {
			return false
		}
		defer res.Body.Close()
		var out struct {
			Data []struct {
				TraceID   string `json:"traceID"`
				Processes map[string]struct {
					ServiceName string `json:"serviceName"`
				} `json:"processes"`
			} `json:"data"`
		}
		if json.NewDecoder(res.Body).Decode(&out) != nil {
			return false
		}
		for _, tr := range out.Data {
			svcs := map[string]bool{}
			for _, p := range tr.Processes {
				svcs[p.ServiceName] = true
			}
			ok := true
			for _, w := range want {
				ok = ok && svcs[w]
			}
			if ok {
				found = tr.TraceID
				return true
			}
		}
		return false
	}, 60*time.Second, time.Second, "no Jaeger trace contains spans of %v", want)
	t.Logf("end-to-end trace: %s/trace/%s", jaegerURL, found)
}
