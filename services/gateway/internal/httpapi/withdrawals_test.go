package httpapi_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
)

func withTOTP(h map[string]string, code string) map[string]string {
	out := map[string]string{"X-TOTP": code}
	for k, v := range h {
		out[k] = v
	}
	return out
}

func TestWithdrawalRequestConvertsTheAmountAndPassesTheCode(t *testing.T) {
	e := newEnv(t, generousLimits)
	body := `{"chain":"btc","asset":"btc","amount":"0.05","destination":"bcrt1qdest"}`
	rec := e.do("POST", "/v1/withdrawals", body, withTOTP(bearer(e.token(42, "user")), "123456"))
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var w map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &w))
	require.Equal(t, "0.05", w["amount"])
	require.Equal(t, "requested", w["state"])
	require.NotContains(t, w, "user_id", "users do not see the user id field")

	require.Len(t, e.wd.requests, 1)
	got := e.wd.requests[0]
	require.EqualValues(t, 42, got.UserId, "the user comes from the token")
	require.Equal(t, "BTC", got.Asset)
	require.Equal(t, "5000000", got.Amount.Units, "0.05 BTC in satoshis")
	require.Equal(t, "123456", got.TotpCode)
}

func TestWithdrawalRequestGuards(t *testing.T) {
	e := newEnv(t, generousLimits)
	body := `{"chain":"btc","asset":"BTC","amount":"0.05","destination":"bcrt1qdest"}`
	user := bearer(e.token(42, "user"))

	rec := e.do("POST", "/v1/withdrawals", body, user)
	require.Equal(t, http.StatusUnauthorized, rec.Code, "no X-TOTP header")
	require.Equal(t, "totp_required", errCode(t, rec))

	rec = e.do("POST", "/v1/withdrawals", body, withTOTP(user, "000000"))
	require.Equal(t, http.StatusUnauthorized, rec.Code, "wrong code (from the service)")

	rec = e.do("POST", "/v1/withdrawals", `{"chain":"btc","asset":"BTC","amount":"0.000000001","destination":"x"}`, withTOTP(user, "123456"))
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code, "more decimals than the asset has")
	rec = e.do("POST", "/v1/withdrawals", `{"chain":"btc","asset":"DOGE","amount":"1","destination":"x"}`, withTOTP(user, "123456"))
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)

	unverified := bearer(e.tokenV(43, "user", false))
	rec = e.do("POST", "/v1/withdrawals", body, withTOTP(unverified, "123456"))
	require.Equal(t, http.StatusForbidden, rec.Code, "verified email required")

	// A signed API key (a bot) may never move funds out.
	e.identity.keys["botkey"] = &identityv1.LookupAPIKeyResponse{UserId: 42, Secret: "s3cret", CanTrade: true, Role: "user"}
	ts := time.Now().Unix()
	e.now.Store(time.Unix(ts, 0).UnixNano())
	rec = e.do("POST", "/v1/withdrawals", body, withTOTP(signedHeaders("s3cret", "botkey", "POST", "/v1/withdrawals", body, ts), "123456"))
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	require.Len(t, e.wd.requests, 1, "only the wrong-code request reached the service")
}

func TestAdminReviewEndpoints(t *testing.T) {
	e := newEnv(t, generousLimits)
	admin, user := bearer(e.token(1, "admin")), bearer(e.token(2, "user"))
	require.Equal(t, http.StatusForbidden, e.do("GET", "/v1/admin/withdrawals/review", "", user).Code)

	rec := e.do("GET", "/v1/admin/withdrawals/review", "", admin)
	require.Equal(t, http.StatusOK, rec.Code)
	var q struct{ Withdrawals []map[string]any }
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &q))
	require.Equal(t, "pending_review", q.Withdrawals[0]["state"])
	require.Equal(t, "2", q.Withdrawals[0]["user_id"], "admins see whose withdrawal it is")

	rec = e.do("POST", "/v1/admin/withdrawals/7/reject", `{"reason":"sanctions match"}`, admin)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, e.wd.reviews, 1)
	require.False(t, e.wd.reviews[0].Approve)
	require.EqualValues(t, 1, e.wd.reviews[0].AdminUserId, "the admin id comes from the token")
	require.Equal(t, "sanctions match", e.wd.reviews[0].Reason)

	require.Equal(t, http.StatusNotFound, e.do("POST", "/v1/admin/withdrawals/abc/approve", "", admin).Code)
}

func TestTOTPEnrolmentIsSessionOnly(t *testing.T) {
	e := newEnv(t, generousLimits)
	user := bearer(e.token(5, "user"))
	rec := e.do("POST", "/v1/2fa/totp", "", user)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Contains(t, rec.Body.String(), "otpauth://")
	require.Equal(t, http.StatusNoContent, e.do("POST", "/v1/2fa/totp/confirm", `{"code":"123456"}`, user).Code)
	require.Equal(t, http.StatusUnauthorized, e.do("POST", "/v1/2fa/totp/confirm", `{"code":"000000"}`, user).Code)
	require.Equal(t, http.StatusNoContent, e.do("PUT", "/v1/me/notifications", `{"order_fills":true}`, user).Code)
	require.Equal(t, http.StatusUnprocessableEntity, e.do("PUT", "/v1/me/notifications", `{}`, user).Code)
}
