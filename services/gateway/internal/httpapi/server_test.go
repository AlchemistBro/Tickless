package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tickless/gateway/internal/httpapi"
	"github.com/tickless/pkg/auth"
	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
)

func (e *testEnv) do(method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = "192.0.2.1:1234"
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	e.srv.Routes().ServeHTTP(rec, req)
	return rec
}

func bearer(tok string) map[string]string { return map[string]string{"Authorization": "Bearer " + tok} }

func errCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var b struct {
		Error struct{ Code, Message string } `json:"error"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &b), rec.Body.String())
	return b.Error.Code
}

const orderBody = `{"market":"BTC-USDT","side":"buy","type":"limit","price":"50000.00","quantity":"0.5"}`

// API-key requests must carry a client_order_id (a signed request can be replayed).
const keyOrderBody = `{"market":"BTC-USDT","side":"buy","type":"limit","price":"50000.00","quantity":"0.5","client_order_id":"bot-1"}`

func TestPublicEndpointsNeedNoAuth(t *testing.T) {
	e := newEnv(t, generousLimits)
	rec := e.do("GET", "/v1/markets", "", nil)
	require.Equal(t, 200, rec.Code)
	var out struct{ Markets []map[string]any }
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Markets, 1)
	assert.Equal(t, "BTC-USDT", out.Markets[0]["symbol"])
	assert.Equal(t, "0.01", out.Markets[0]["tick_size"])
	assert.Equal(t, "0.00001", out.Markets[0]["lot_size"])
	assert.Equal(t, "5", out.Markets[0]["min_notional"])

	rec = e.do("POST", "/v1/auth/login", `{"email":"a@b.co","password":"correct horse battery"}`, nil)
	require.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), `"access_token":"a"`)
}

func TestAuthRequiredAndGenericFailure(t *testing.T) {
	e := newEnv(t, generousLimits)
	for name, hdr := range map[string]map[string]string{
		"none":         nil,
		"garbage":      bearer("not.a.jwt"),
		"wrong scheme": {"Authorization": "Basic Zm9vOmJhcg=="},
		"empty bearer": {"Authorization": "Bearer "},
		"tampered":     bearer(e.token(1, "user")[:50] + "x" + e.token(1, "user")[51:]),
	} {
		rec := e.do("GET", "/v1/balances", "", hdr)
		assert.Equal(t, 401, rec.Code, name)
		assert.Equal(t, "unauthenticated", errCode(t, rec), name)
		assert.NotEmpty(t, rec.Header().Get("WWW-Authenticate"))
	}
	rec := e.do("GET", "/v1/balances", "", bearer(e.token(1, "user")))
	require.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), `"available":"5500"`, "balance units formatted with asset decimals")
}

func TestExpiredTokenRejected(t *testing.T) {
	e := newEnv(t, generousLimits)
	tok := e.token(1, "user")
	e.now.Add(int64(11 * time.Minute))
	assert.Equal(t, 401, e.do("GET", "/v1/balances", "", bearer(tok)).Code)
}

func TestKeyRotationOfIdentityIsPickedUp(t *testing.T) {
	e := newEnv(t, generousLimits)
	require.Equal(t, 200, e.do("GET", "/v1/balances", "", bearer(e.token(1, "user"))).Code)
	// identity restarts with a new ephemeral key
	newSigner, _ := auth.NewSigner(nil, 10*time.Minute)
	e.signer = newSigner
	e.now.Add(int64(time.Minute)) // beyond the refetch rate limit gap
	e.now.Add(int64(20 * time.Second))
	assert.Equal(t, 200, e.do("GET", "/v1/balances", "", bearer(e.token(1, "user"))).Code, "gateway refetched the public key")
}

func TestFrozenUserIsBlocked(t *testing.T) {
	e := newEnv(t, generousLimits)
	tok := e.token(5, "user")
	require.Equal(t, 200, e.do("GET", "/v1/balances", "", bearer(tok)).Code)
	_, err := e.redis.SAdd(httpapi.FrozenSet, "5")
	require.NoError(t, err)
	rec := e.do("GET", "/v1/balances", "", bearer(tok))
	assert.Equal(t, 403, rec.Code)
	assert.Equal(t, "account_frozen", errCode(t, rec))
}

func signedHeaders(secret, keyID, method, path, body string, ts int64) map[string]string {
	return map[string]string{
		"X-API-KEY": keyID, "X-API-TIMESTAMP": strconv.FormatInt(ts, 10),
		"X-API-SIGNATURE": auth.SignRequest(secret, ts, method, path, []byte(body)),
	}
}

func TestAPIKeyAuthentication(t *testing.T) {
	e := newEnv(t, generousLimits)
	e.identity.keys["tk_trade"] = &identityv1.LookupAPIKeyResponse{UserId: 9, Secret: "shh", CanTrade: true, Role: "user"}
	e.identity.keys["tk_ro"] = &identityv1.LookupAPIKeyResponse{UserId: 9, Secret: "ro-secret", CanTrade: false, Role: "user"}
	e.identity.keys["tk_admin"] = &identityv1.LookupAPIKeyResponse{UserId: 1, Secret: "adm", CanTrade: true, Role: "admin"}
	ts := time.Unix(0, e.now.Load()).Unix()

	// valid signed GET and POST
	assert.Equal(t, 200, e.do("GET", "/v1/balances", "", signedHeaders("shh", "tk_trade", "GET", "/v1/balances", "", ts)).Code)
	assert.Equal(t, 201, e.do("POST", "/v1/orders", keyOrderBody, signedHeaders("shh", "tk_trade", "POST", "/v1/orders", keyOrderBody, ts)).Code)
	assert.Equal(t, int64(9), e.orders.last.UserId, "order is placed for the key's owner")

	// query string is part of the signature
	assert.Equal(t, 200, e.do("GET", "/v1/orders?open=true", "", signedHeaders("shh", "tk_trade", "GET", "/v1/orders?open=true", "", ts)).Code)
	assert.Equal(t, 401, e.do("GET", "/v1/orders?open=false", "", signedHeaders("shh", "tk_trade", "GET", "/v1/orders?open=true", "", ts)).Code)

	// tampered body, wrong secret, unknown key, stale timestamp, missing signature
	assert.Equal(t, 401, e.do("POST", "/v1/orders", `{"market":"BTC-USDT","side":"sell","type":"limit","price":"1","quantity":"1","client_order_id":"x"}`,
		signedHeaders("shh", "tk_trade", "POST", "/v1/orders", keyOrderBody, ts)).Code)
	assert.Equal(t, 401, e.do("GET", "/v1/balances", "", signedHeaders("nope", "tk_trade", "GET", "/v1/balances", "", ts)).Code)
	assert.Equal(t, 401, e.do("GET", "/v1/balances", "", signedHeaders("shh", "tk_unknown", "GET", "/v1/balances", "", ts)).Code)
	assert.Equal(t, 401, e.do("GET", "/v1/balances", "", signedHeaders("shh", "tk_trade", "GET", "/v1/balances", "", ts-120)).Code, "replay window")
	h := signedHeaders("shh", "tk_trade", "GET", "/v1/balances", "", ts)
	delete(h, "X-API-SIGNATURE")
	assert.Equal(t, 401, e.do("GET", "/v1/balances", "", h).Code)

	// scopes: read-only key cannot trade; keys cannot manage keys or call admin
	assert.Equal(t, 200, e.do("GET", "/v1/balances", "", signedHeaders("ro-secret", "tk_ro", "GET", "/v1/balances", "", ts)).Code)
	rec := e.do("POST", "/v1/orders", keyOrderBody, signedHeaders("ro-secret", "tk_ro", "POST", "/v1/orders", keyOrderBody, ts))
	assert.Equal(t, 403, rec.Code)
	assert.Equal(t, 403, e.do("GET", "/v1/api-keys", "", signedHeaders("shh", "tk_trade", "GET", "/v1/api-keys", "", ts)).Code)
	assert.Equal(t, 403, e.do("GET", "/v1/admin/users", "", signedHeaders("adm", "tk_admin", "GET", "/v1/admin/users", "", ts)).Code,
		"admin endpoints require a session, even for admin-owned keys")
}

func TestAPIKeyLookupIsCached(t *testing.T) {
	e := newEnv(t, generousLimits)
	e.identity.keys["tk"] = &identityv1.LookupAPIKeyResponse{UserId: 9, Secret: "shh", CanTrade: true}
	ts := time.Unix(0, e.now.Load()).Unix()
	for i := 0; i < 5; i++ {
		require.Equal(t, 200, e.do("GET", "/v1/balances", "", signedHeaders("shh", "tk", "GET", "/v1/balances", "", ts)).Code)
	}
	assert.Equal(t, int32(1), e.identity.lookups.Load())
}

func TestAdminAuthorization(t *testing.T) {
	e := newEnv(t, generousLimits)
	user, admin := e.token(2, auth.RoleUser), e.token(1, auth.RoleAdmin)
	for _, ep := range []struct{ method, path string }{
		{"GET", "/v1/admin/users"}, {"POST", "/v1/admin/users/2/freeze"}, {"POST", "/v1/admin/users/2/unfreeze"},
		{"GET", "/v1/admin/users/2/balances"}, {"GET", "/v1/admin/users/2/ledger"},
		{"POST", "/v1/admin/markets/BTC-USDT/halt"}, {"POST", "/v1/admin/markets/BTC-USDT/resume"},
	} {
		assert.Equal(t, 401, e.do(ep.method, ep.path, "", nil).Code, "anonymous "+ep.path)
		assert.Equal(t, 403, e.do(ep.method, ep.path, "", bearer(user)).Code, "plain user "+ep.path)
		assert.Less(t, e.do(ep.method, ep.path, "", bearer(admin)).Code, 300, "admin "+ep.path)
	}
	assert.Equal(t, []int64{2, 2}, e.identity.frozenSet[:2])
	rec := e.do("POST", "/v1/admin/markets/NOPE/halt", "", bearer(admin))
	assert.Equal(t, 404, rec.Code)
	// freeze takes effect immediately in the shared set
	assert.False(t, e.redis.Exists(httpapi.FrozenSet), "unfreeze removed the member again")
}

func TestRateLimiting(t *testing.T) {
	l := generousLimits
	l.UserRate, l.UserBurst = 1, 3
	e := newEnv(t, l)
	a, b := e.token(1, "user"), e.token(2, "user")
	for i := 0; i < 3; i++ {
		require.Equal(t, 200, e.do("GET", "/v1/balances", "", bearer(a)).Code, "burst %d", i)
	}
	rec := e.do("GET", "/v1/balances", "", bearer(a))
	assert.Equal(t, 429, rec.Code)
	assert.Equal(t, "rate_limited", errCode(t, rec))
	assert.NotEmpty(t, rec.Header().Get("Retry-After"))
	assert.Equal(t, 200, e.do("GET", "/v1/balances", "", bearer(b)).Code, "other users are unaffected")
}

func TestAuthEndpointsHaveStricterIPLimit(t *testing.T) {
	l := generousLimits
	l.AuthRate, l.AuthBurst = 0.1, 2
	e := newEnv(t, l)
	body := `{"email":"a@b.co","password":"correct horse battery"}`
	assert.Equal(t, 200, e.do("POST", "/v1/auth/login", body, nil).Code)
	assert.Equal(t, 200, e.do("POST", "/v1/auth/login", body, nil).Code)
	assert.Equal(t, 429, e.do("POST", "/v1/auth/login", body, nil).Code, "credential stuffing is throttled per IP")
	assert.Equal(t, 200, e.do("GET", "/v1/markets", "", nil).Code, "other endpoints unaffected")
}

func TestIPRateLimitFailsOpenWhenRedisIsDown(t *testing.T) {
	l := generousLimits
	l.IPRate, l.IPBurst = 1, 1
	e := newEnv(t, l)
	e.redis.Close()
	for i := 0; i < 3; i++ {
		assert.Equal(t, 200, e.do("GET", "/v1/markets", "", nil).Code)
	}
}

func TestIdempotencyKey(t *testing.T) {
	e := newEnv(t, generousLimits)
	tok := bearer(e.token(1, "user"))
	withKey := func(k string) map[string]string {
		h := bearer(e.token(1, "user"))
		h["Idempotency-Key"] = k
		_ = tok
		return h
	}

	first := e.do("POST", "/v1/orders", orderBody, withKey("k1"))
	require.Equal(t, 201, first.Code)
	second := e.do("POST", "/v1/orders", orderBody, withKey("k1"))
	assert.Equal(t, 201, second.Code)
	assert.Equal(t, "true", second.Header().Get("Idempotent-Replayed"))
	assert.Equal(t, first.Body.String(), second.Body.String(), "identical stored response")
	assert.Equal(t, int32(1), e.orders.placed.Load(), "backend executed once")

	rec := e.do("POST", "/v1/orders", `{"market":"BTC-USDT","side":"sell","type":"limit","price":"50000","quantity":"1"}`, withKey("k1"))
	assert.Equal(t, 422, rec.Code)
	assert.Equal(t, "idempotency_key_reused", errCode(t, rec))

	// another user can use the same key value independently
	h := bearer(e.token(2, "user"))
	h["Idempotency-Key"] = "k1"
	assert.Equal(t, 201, e.do("POST", "/v1/orders", orderBody, h).Code)
	assert.Equal(t, int32(2), e.orders.placed.Load())

	// 5xx responses are not stored: the retry executes again
	e.orders.err = status.Error(codes.Internal, "secret internal detail")
	assert.Equal(t, 500, e.do("POST", "/v1/orders", orderBody, withKey("k2")).Code)
	e.orders.err = nil
	assert.Equal(t, 201, e.do("POST", "/v1/orders", orderBody, withKey("k2")).Code)

	// too long key
	assert.Equal(t, 400, e.do("POST", "/v1/orders", orderBody, withKey(strings.Repeat("x", 129))).Code)
}

func TestRequestValidation(t *testing.T) {
	e := newEnv(t, generousLimits)
	h := bearer(e.token(1, "user"))
	cases := map[string]struct {
		body string
		want int
	}{
		"unknown field":   {`{"market":"BTC-USDT","side":"buy","type":"limit","price":"1","quantity":"1","evil":true}`, 400},
		"trailing data":   {orderBody + `{"x":1}`, 400},
		"not json":        {`market=BTC`, 400},
		"empty":           {``, 400},
		"bad side":        {`{"market":"BTC-USDT","side":"hold","type":"limit","price":"1","quantity":"1"}`, 400},
		"number as price": {`{"market":"BTC-USDT","side":"buy","type":"limit","price":50000,"quantity":"1"}`, 400},
		"too large":       {`{"market":"` + strings.Repeat("A", 2<<20) + `"}`, 413},
	}
	for name, tc := range cases {
		rec := e.do("POST", "/v1/orders", tc.body, h)
		assert.Equal(t, tc.want, rec.Code, name)
	}
	assert.Zero(t, e.orders.placed.Load(), "nothing invalid reaches the orders service")

	assert.Equal(t, 400, e.do("GET", "/v1/orders/abc", "", h).Code)
	assert.Equal(t, 400, e.do("GET", "/v1/orders/0", "", h).Code)
	assert.Equal(t, 404, e.do("GET", "/v1/orders/404", "", h).Code)
	assert.Equal(t, 400, e.do("POST", "/v1/admin/users/x/freeze", "", bearer(e.token(1, "admin"))).Code)
}

func TestErrorMappingHidesInternals(t *testing.T) {
	e := newEnv(t, generousLimits)
	h := bearer(e.token(1, "user"))
	for _, tc := range []struct {
		err  error
		want int
	}{
		{status.Error(codes.InvalidArgument, "price: not a multiple of tick"), 400},
		{status.Error(codes.NotFound, "orders: unknown market"), 404},
		{status.Error(codes.FailedPrecondition, "orders: insufficient funds"), 422},
		{status.Error(codes.ResourceExhausted, "orders: too many open orders"), 429},
		{status.Error(codes.Unavailable, "dial tcp 10.0.0.5: connection refused"), 503},
		{status.Error(codes.Internal, "pq: password authentication failed for user ledger"), 500},
		{status.Error(codes.DeadlineExceeded, "ctx"), 504},
	} {
		e.orders.err = tc.err
		rec := e.do("POST", "/v1/orders", orderBody, h)
		assert.Equal(t, tc.want, rec.Code, tc.err.Error())
		if tc.want >= 500 {
			assert.NotContains(t, rec.Body.String(), "10.0.0.5")
			assert.NotContains(t, rec.Body.String(), "password")
		}
	}
	rec := e.do("POST", "/v1/auth/signup", `{"email":"taken@example.com","password":"correct horse battery"}`, nil)
	assert.Equal(t, 409, rec.Code)
}

func TestOrderJSONShape(t *testing.T) {
	e := newEnv(t, generousLimits)
	h := bearer(e.token(1, "user"))
	rec := e.do("POST", "/v1/orders", orderBody, h)
	require.Equal(t, 201, rec.Code)
	var o map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &o))
	assert.IsType(t, "", o["id"], "ids are strings (uint64 safe for JavaScript)")
	assert.Equal(t, "new", o["status"])
	assert.Equal(t, "buy", o["side"])
	assert.Equal(t, "0.5", o["quantity"])
	assert.IsType(t, "", o["price"], "money is never a JSON number")
	assert.Equal(t, "BTC-USDT", e.orders.last.Market)
	assert.Equal(t, int64(1), e.orders.last.UserId, "user id comes from the token, never from the body")

	dupBody := `{"market":"BTC-USDT","side":"buy","type":"limit","price":"50000","quantity":"1","client_order_id":"dup"}`
	assert.Equal(t, 201, e.do("POST", "/v1/orders", dupBody, h).Code, "first submission creates the order")
	assert.Equal(t, 200, e.do("POST", "/v1/orders", dupBody, h).Code, "duplicate client_order_id returns 200")
}

func TestSecurityHeadersAndCORS(t *testing.T) {
	e := newEnv(t, generousLimits)
	rec := e.do("GET", "/v1/markets", "", map[string]string{"Origin": "http://localhost:5173"})
	assert.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	assert.Equal(t, "http://localhost:5173", rec.Header().Get("Access-Control-Allow-Origin"))
	rec = e.do("GET", "/v1/markets", "", map[string]string{"Origin": "https://evil.example"})
	assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"), "unlisted origins get no CORS headers")
	pre := e.do("OPTIONS", "/v1/orders", "", map[string]string{"Origin": "http://localhost:5173"})
	assert.Equal(t, 204, pre.Code)
}

func TestCreateAndUseAPIKeyFlow(t *testing.T) {
	e := newEnv(t, generousLimits)
	h := bearer(e.token(1, "user"))
	rec := e.do("POST", "/v1/api-keys", `{"label":"bot","can_trade":true}`, h)
	require.Equal(t, 201, rec.Code)
	assert.Contains(t, rec.Body.String(), `"secret":"s3cret"`)
	rec = e.do("GET", "/v1/ledger?asset=usdt&limit=10", "", h)
	require.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), `"amount":"-25000"`)
}

func TestUnmatchedRoutesAre404(t *testing.T) {
	e := newEnv(t, generousLimits)
	assert.Equal(t, 404, e.do("GET", "/v1/nothing", "", nil).Code)
	assert.Equal(t, 405, e.do("PUT", "/v1/markets", "", nil).Code)
}

// Phase 3.1 item 4: a signed request can be replayed inside its validity window.
// The order service is idempotent by client_order_id, so API-key orders must have one.
func TestAPIKeyOrdersRequireClientOrderIDAndReplayIsIdempotent(t *testing.T) {
	e := newEnv(t, generousLimits)
	e.identity.keys["tk"] = &identityv1.LookupAPIKeyResponse{UserId: 9, Secret: "shh", CanTrade: true, Role: "user"}
	ts := time.Unix(0, e.now.Load()).Unix()

	rec := e.do("POST", "/v1/orders", orderBody, signedHeaders("shh", "tk", "POST", "/v1/orders", orderBody, ts))
	assert.Equal(t, 422, rec.Code)
	assert.Equal(t, "client_order_id_required", errCode(t, rec))
	assert.Zero(t, e.orders.placed.Load(), "rejected before reaching the order service")

	hdr := signedHeaders("shh", "tk", "POST", "/v1/orders", keyOrderBody, ts)
	first := e.do("POST", "/v1/orders", keyOrderBody, hdr)
	require.Equal(t, 201, first.Code)
	replay := e.do("POST", "/v1/orders", keyOrderBody, hdr) // identical signed request, replayed
	assert.Equal(t, 200, replay.Code, "the replay is answered as a duplicate")
	assert.Equal(t, int32(1), e.orders.placed.Load(), "exactly one order exists after the replay")

	// Session (JWT) requests keep working without a client_order_id.
	assert.Equal(t, 201, e.do("POST", "/v1/orders", orderBody, bearer(e.token(1, "user"))).Code)
}

// Phase 3.1 item 5: email_verified is enforced for placing orders.
func TestUnverifiedEmailCannotPlaceOrders(t *testing.T) {
	e := newEnv(t, generousLimits)
	unverified := bearer(e.tokenV(4, "user", false))
	rec := e.do("POST", "/v1/orders", orderBody, unverified)
	assert.Equal(t, 403, rec.Code)
	assert.Equal(t, "email_not_verified", errCode(t, rec))
	assert.Zero(t, e.orders.placed.Load())
	assert.Equal(t, 200, e.do("GET", "/v1/balances", "", unverified).Code, "read access is unaffected")
	assert.Equal(t, 202, e.do("DELETE", "/v1/orders/5", "", unverified).Code, "cancelling is always allowed")
	assert.Equal(t, 201, e.do("POST", "/v1/orders", orderBody, bearer(e.tokenV(4, "user", true))).Code)
}

// Phase 3.1 item 3: if Redis cannot answer the frozen check, authenticated
// requests are refused (503) instead of served.
func TestFrozenCheckFailsClosedWhenRedisIsDown(t *testing.T) {
	e := newEnv(t, generousLimits)
	tok := bearer(e.token(1, "user"))
	require.Equal(t, 200, e.do("GET", "/v1/balances", "", tok).Code)
	e.redis.Close()
	rec := e.do("GET", "/v1/balances", "", tok)
	assert.Equal(t, 503, rec.Code)
	assert.Equal(t, "unavailable", errCode(t, rec))
	assert.Equal(t, 503, e.do("POST", "/v1/orders", orderBody, tok).Code)
	assert.Zero(t, e.orders.placed.Load())
	assert.Equal(t, 200, e.do("GET", "/v1/markets", "", nil).Code, "public endpoints do not depend on the frozen set")
}

func TestFrozenSetIsRebuiltFromIdentity(t *testing.T) {
	e := newEnvSync(t, generousLimits, true)
	tok3, tok5 := bearer(e.token(3, "user")), bearer(e.token(5, "user"))
	_, err := e.redis.SAdd(httpapi.FrozenSet, "5", "9") // stale members
	require.NoError(t, err)
	e.identity.frozenOnServer = []int64{3, 4}

	// Until the first rebuild completes, authenticated requests are refused.
	assert.False(t, e.srv.FrozenSynced())
	assert.Equal(t, 503, e.do("GET", "/v1/balances", "", tok5).Code)

	require.NoError(t, e.srv.SyncFrozen(context.Background()))
	assert.True(t, e.srv.FrozenSynced())
	members, err := e.redis.SMembers(httpapi.FrozenSet)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"3", "4"}, members, "the set equals the list from identity: stale members removed, missing ones added")
	assert.Equal(t, 403, e.do("GET", "/v1/balances", "", tok3).Code)
	assert.Equal(t, 200, e.do("GET", "/v1/balances", "", tok5).Code)

	// Everyone unfrozen: the key disappears.
	e.identity.frozenOnServer = nil
	require.NoError(t, e.srv.SyncFrozen(context.Background()))
	assert.False(t, e.redis.Exists(httpapi.FrozenSet))

	// identity down: the sync fails and leaves the set untouched.
	_, err = e.redis.SAdd(httpapi.FrozenSet, "7")
	require.NoError(t, err)
	e.identity.listFrozenErr = status.Error(codes.Unavailable, "down")
	assert.Error(t, e.srv.SyncFrozen(context.Background()))
	assert.True(t, e.redis.Exists(httpapi.FrozenSet))
}
