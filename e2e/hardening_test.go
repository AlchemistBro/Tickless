//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/pkg/auth"
)

func adminToken(t *testing.T) string {
	t.Helper()
	r := call(t, "POST", "/v1/auth/login", "", map[string]string{"email": "admin@tickless.local", "password": env("TICKLESS_ADMIN_PASSWORD", "dev-admin-password")}, nil)
	require.Equal(t, 200, r.Status, string(r.Body))
	var tk struct {
		Access string `json:"access_token"`
	}
	r.json(t, &tk)
	return tk.Access
}

// Phase 3.1 item 4, end to end: a signed order request replayed inside its
// validity window creates exactly one order; without client_order_id it is refused.
func TestSignedOrderReplayCreatesOneOrder(t *testing.T) {
	u := newUser(t, "replay")
	seed(t, u, "USDT", "100000000000")
	u.waitBalance("USDT", "100000", "0")
	r := call(t, "POST", "/v1/api-keys", u.Access, map[string]any{"label": "bot", "can_trade": true}, nil)
	require.Equal(t, 201, r.Status, string(r.Body))
	var k struct {
		KeyID  string `json:"key_id"`
		Secret string `json:"secret"`
	}
	r.json(t, &k)

	send := func(body []byte, ts int64) resp {
		return callRaw(t, "POST", "/v1/orders", body, map[string]string{
			"X-API-KEY": k.KeyID, "X-API-TIMESTAMP": strconv.FormatInt(ts, 10),
			"X-API-SIGNATURE": auth.SignRequest(k.Secret, ts, "POST", "/v1/orders", body)})
	}
	noID, _ := json.Marshal(limit("BTC-USDT", "buy", "40000.00", "0.01", ""))
	assert.Equal(t, 422, send(noID, time.Now().Unix()).Status, "API-key orders need a client_order_id")

	body, _ := json.Marshal(limit("BTC-USDT", "buy", "40000.00", "0.01", "replay-1"))
	ts := time.Now().Unix()
	first := send(body, ts)
	require.Equal(t, 201, first.Status, string(first.Body))
	again := send(body, ts) // byte-identical signed request replayed by an attacker
	assert.Equal(t, 200, again.Status, string(again.Body))
	var o1, o2 order
	first.json(t, &o1)
	again.json(t, &o2)
	assert.Equal(t, o1.ID, o2.ID, "the replay returned the original order")

	r = call(t, "GET", "/v1/orders?open=true", u.Access, nil, nil)
	var list struct{ Orders []order }
	r.json(t, &list)
	assert.Len(t, list.Orders, 1, "exactly one open order exists")
	u.waitBalance("USDT", "99600", "400")
}

func restartGateway(t *testing.T) {
	t.Helper()
	out, err := exec.Command("docker", "compose", "-f", "../deploy/compose/docker-compose.yml", "restart", "gateway").CombinedOutput()
	require.NoError(t, err, string(out))
	require.Eventually(t, func() bool {
		return call2(gatewayURL+"/readyz") == 200
	}, 60*time.Second, 500*time.Millisecond, "gateway ready again (it only becomes ready after rebuilding the frozen set)")
}

// Phase 3.1 item 3, end to end: the frozen set is rebuilt from identity, so a
// wiped Redis plus a gateway restart cannot unfreeze anybody.
func TestFrozenUserStaysFrozenAfterRedisWipeAndGatewayRestart(t *testing.T) {
	admin := adminToken(t)
	u := newUser(t, "frozen")
	require.Equal(t, 204, call(t, "POST", "/v1/admin/users/"+u.ID+"/freeze", admin, nil, nil).Status)
	require.Equal(t, 403, call(t, "GET", "/v1/balances", u.Access, nil, nil).Status)

	rdb := redis.NewClient(&redis.Options{Addr: env("E2E_REDIS", "127.0.0.1:6379")})
	defer func() { _ = rdb.Close() }()
	require.NoError(t, rdb.Del(context.Background(), "frozen_users").Err(), "simulate a Redis that lost its data")

	restartGateway(t)
	assert.Equal(t, 403, call(t, "GET", "/v1/balances", u.Access, nil, nil).Status, "still frozen after the rebuild from identity")
	n, err := rdb.SIsMember(context.Background(), "frozen_users", u.ID).Result()
	require.NoError(t, err)
	assert.True(t, n)

	// Unfreeze through the API and verify it is reversible.
	admin = adminToken(t)
	require.Equal(t, 204, call(t, "POST", "/v1/admin/users/"+u.ID+"/unfreeze", admin, nil, nil).Status)
	assert.Equal(t, 200, call(t, "GET", "/v1/balances", u.Access, nil, nil).Status)
}
