//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/pg"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var (
	gatewayURL = env("E2E_GATEWAY", "http://localhost:8080")
	ledgerAddr = env("E2E_LEDGER_GRPC", "127.0.0.1:9101")
	ledgerDSN  = env("E2E_LEDGER_DSN", "postgres://tickless:tickless@127.0.0.1:5433/ledger?sslmode=disable")
	jaegerURL  = env("E2E_JAEGER", "http://localhost:16686")
	kafkaAddr  = env("E2E_KAFKA", "127.0.0.1:9092")
)

// user is a registered, logged-in account.
type user struct {
	t      *testing.T
	ID     string
	Email  string
	Access string
}

type resp struct {
	Status int
	Body   []byte
	Header http.Header
}

func (r resp) json(t *testing.T, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(r.Body, v), string(r.Body))
}

func call(t *testing.T, method, path, token string, body any, hdr map[string]string) resp {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, gatewayURL+path, rd)
	require.NoError(t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return resp{Status: res.StatusCode, Body: b, Header: res.Header}
}

func newUser(t *testing.T, name string) *user {
	t.Helper()
	email := fmt.Sprintf("%s-%d@example.com", name, time.Now().UnixNano())
	pw := "correct horse battery staple"
	r := call(t, "POST", "/v1/auth/signup", "", map[string]string{"email": email, "password": pw}, nil)
	require.Equal(t, 201, r.Status, string(r.Body))
	var su struct {
		UserID string `json:"user_id"`
	}
	r.json(t, &su)
	r = call(t, "POST", "/v1/auth/login", "", map[string]string{"email": email, "password": pw}, nil)
	require.Equal(t, 200, r.Status, string(r.Body))
	var tk struct {
		Access string `json:"access_token"`
	}
	r.json(t, &tk)
	u := &user{t: t, ID: su.UserID, Email: email, Access: tk.Access}
	// Leave the shared book clean for the next test/run: cancel whatever is still resting.
	t.Cleanup(func() {
		req, err := http.NewRequest("DELETE", gatewayURL+"/v1/orders", nil)
		if err != nil {
			return
		}
		req.Header.Set("Authorization", "Bearer "+u.Access)
		if res, err := http.DefaultClient.Do(req); err == nil {
			res.Body.Close()
			time.Sleep(300 * time.Millisecond) // let the cancels reach the engine
		}
	})
	return u
}

type order struct {
	ID             string `json:"id"`
	Status         string `json:"status"`
	FilledQuantity string `json:"filled_quantity"`
	Quantity       string `json:"quantity"`
	Price          string `json:"price"`
	RejectReason   string `json:"reject_reason"`
}

func (u *user) place(body map[string]any) (resp, order) {
	r := call(u.t, "POST", "/v1/orders", u.Access, body, nil)
	var o order
	if r.Status == 200 || r.Status == 201 {
		r.json(u.t, &o)
	}
	return r, o
}

// tryOrder fetches an order without failing the test (safe inside Eventually).
func (u *user) tryOrder(id string) (order, int) {
	req, err := http.NewRequest("GET", gatewayURL+"/v1/orders/"+id, nil)
	if err != nil {
		return order{}, 0
	}
	req.Header.Set("Authorization", "Bearer "+u.Access)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return order{}, 0
	}
	defer res.Body.Close()
	var o order
	_ = json.NewDecoder(res.Body).Decode(&o)
	return o, res.StatusCode
}

// waitOrder polls until the order satisfies cond.
func (u *user) waitOrder(id string, cond func(order) bool, what string) order {
	u.t.Helper()
	require.NotEmpty(u.t, id, "order id (the order was not accepted)")
	var o order
	var status int
	ok := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if o, status = u.tryOrder(id); status == 200 && cond(o) {
			ok = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(u.t, ok, "order %s never became %s (last http %d: %+v)", id, what, status, o)
	return o
}

type balance struct{ Asset, Available, Held string }

func (u *user) balances() map[string]balance {
	m := map[string]balance{}
	req, err := http.NewRequest("GET", gatewayURL+"/v1/balances", nil)
	if err != nil {
		return m
	}
	req.Header.Set("Authorization", "Bearer "+u.Access)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return m
	}
	defer res.Body.Close()
	var out struct{ Balances []balance }
	_ = json.NewDecoder(res.Body).Decode(&out)
	for _, b := range out.Balances {
		m[b.Asset] = b
	}
	return m
}

// waitBalance polls until the asset's balance equals available/held.
func (u *user) waitBalance(asset, available, held string) {
	u.t.Helper()
	var got balance
	ok := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if got = u.balances()[asset]; got.Available == available && got.Held == held {
			ok = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(u.t, ok, "%s balance of user %s: want %s/%s, last %+v", asset, u.ID, available, held, got)
}

func ledgerClient(t *testing.T) ledgerv1.LedgerServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(ledgerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return ledgerv1.NewLedgerServiceClient(conn)
}

// seed credits a user via the dev-only AdminCredit RPC.
func seed(t *testing.T, u *user, asset, unitsAmount string) {
	t.Helper()
	var uid int64
	_, err := fmt.Sscan(u.ID, &uid)
	require.NoError(t, err)
	_, err = ledgerClient(t).AdminCredit(context.Background(), &ledgerv1.AdminCreditRequest{
		BusinessKey: fmt.Sprintf("e2e-seed-%s-%s-%d", u.ID, asset, time.Now().UnixNano()), UserId: uid, Asset: asset,
		Amount: &commonv1.Amount{Units: unitsAmount}})
	require.NoError(t, err)
}

func ledgerDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pg.Connect(context.Background(), ledgerDSN, 3)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// assertLedgerInvariants checks SPEC §6 invariants 1 and 2 (and the
// balance == sum(postings) rule) directly in the ledger database.
func assertLedgerInvariants(t *testing.T) {
	t.Helper()
	db := ledgerDB(t)
	ctx := context.Background()
	var bad int
	require.NoError(t, db.QueryRow(ctx, `SELECT count(*) FROM ledger_verify()`).Scan(&bad))
	require.Zero(t, bad, "balances must equal the sum of their postings")

	rows, err := db.Query(ctx, `SELECT asset, sum(amount)::text FROM balances b JOIN accounts a ON a.id=b.account_id GROUP BY asset HAVING sum(amount) <> 0`)
	require.NoError(t, err)
	defer rows.Close()
	require.False(t, rows.Next(), "every asset must sum to zero across the ledger")

	var neg int
	require.NoError(t, db.QueryRow(ctx, `SELECT count(*) FROM balances WHERE kind <> 'EXTERNAL' AND amount < 0`).Scan(&neg))
	require.Zero(t, neg, "no available/held/fee balance may be negative")

	var unbalanced int
	require.NoError(t, db.QueryRow(ctx, `SELECT count(*) FROM (SELECT entry_id, asset FROM postings GROUP BY entry_id, asset HAVING sum(amount) <> 0) x`).Scan(&unbalanced))
	require.Zero(t, unbalanced, "every journal entry must sum to zero per asset")
}

// kafkaDuplicates counts (market, seq) pairs that appear more than once on matching.events.
func kafkaDuplicates(t *testing.T) (total, dups int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaAddr), kgo.ConsumeTopics("matching.events"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	seen := map[string]int{}
	quiet := time.Now()
	for time.Since(quiet) < 2*time.Second {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		n := total
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			var e matchingv1.Event
			if proto.Unmarshal(r.Value, &e) == nil {
				seen[fmt.Sprintf("%s:%d", e.Market, e.Seq)]++
				total++
			}
		})
		cancel()
		if total != n {
			quiet = time.Now()
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if seen[k] > 1 {
			dups++
		}
	}
	return total, dups
}

// call2 returns the HTTP status of a GET (0 on connection errors); safe inside polling loops.
func call2(url string) int {
	res, err := http.Get(url)
	if err != nil {
		return 0
	}
	defer res.Body.Close()
	return res.StatusCode
}
