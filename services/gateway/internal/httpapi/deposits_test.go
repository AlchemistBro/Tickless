package httpapi_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDepositAddressIsTheCallersOwn(t *testing.T) {
	e := newEnv(t, generousLimits)
	rec := e.do("GET", "/v1/deposit-address?chain=ETH", "", bearer(e.token(42, "user")))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var body struct {
		Chain, Address string
		Assets         []string
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, "eth", body.Chain, "chain names are case-insensitive")
	require.Equal(t, "eth-addr-of-42", body.Address)
	require.Equal(t, []string{"ETH", "USDT"}, body.Assets)
	require.Equal(t, []int64{42}, e.wallet.users, "the user id comes from the token, never from the request")

	require.Equal(t, http.StatusUnauthorized, e.do("GET", "/v1/deposit-address?chain=btc", "", nil).Code)
	require.Equal(t, http.StatusUnprocessableEntity, e.do("GET", "/v1/deposit-address", "", bearer(e.token(42, "user"))).Code)
}

func TestDepositsAreFormattedPerAsset(t *testing.T) {
	e := newEnv(t, generousLimits)
	rec := e.do("GET", "/v1/deposits?limit=10", "", bearer(e.token(7, "user")))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var body struct {
		Deposits []map[string]any `json:"deposits"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Deposits, 2)
	btc, eth := body.Deposits[0], body.Deposits[1]
	require.Equal(t, "0.5", btc["amount"])
	require.Equal(t, "pending", btc["status"])
	require.EqualValues(t, 1, btc["confirmations"])
	require.EqualValues(t, 2, btc["required_confirmations"])
	require.NotContains(t, btc, "confirmed_at")
	require.Equal(t, "1.5", eth["amount"])
	require.Equal(t, "confirmed", eth["status"])
	require.EqualValues(t, -1, eth["index"])
	require.Contains(t, eth, "confirmed_at")
	require.Equal(t, []int64{7}, e.cw.users)
}
