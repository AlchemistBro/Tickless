package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
)

const specPath = "../../../../api/openapi/openapi.yaml"

func loadSpec(t *testing.T) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(specPath)
	require.NoError(t, err)
	require.NoError(t, doc.Validate(context.Background()), "OpenAPI document must be valid")
	return doc
}

// TestSpecIsValidAndMatchesRoutes keeps the hand-written OpenAPI document and
// the chi router in lockstep: every route is documented and every documented
// operation is routed.
func TestSpecIsValidAndMatchesRoutes(t *testing.T) {
	doc := loadSpec(t)
	want := map[string]bool{}
	for path, item := range doc.Paths.Map() {
		for method := range item.Operations() {
			want[method+" "+path] = true
		}
	}
	e := newEnv(t, generousLimits)
	got := map[string]bool{}
	require.NoError(t, chi.Walk(e.srv.Routes().(chi.Routes), func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if method == http.MethodOptions {
			return nil
		}
		got[method+" "+strings.TrimSuffix(route, "/")] = true
		return nil
	}))
	var missing, undocumented []string
	for k := range want {
		if !got[k] {
			missing = append(missing, k)
		}
	}
	for k := range got {
		if !want[k] {
			undocumented = append(undocumented, k)
		}
	}
	sort.Strings(missing)
	sort.Strings(undocumented)
	assert.Empty(t, missing, "documented but not routed")
	assert.Empty(t, undocumented, "routed but not documented")
}

// TestResponsesConformToSpec validates real responses against the schemas.
func TestResponsesConformToSpec(t *testing.T) {
	doc := loadSpec(t)
	router, err := gorillamux.NewRouter(doc)
	require.NoError(t, err)
	e := newEnv(t, generousLimits)
	admin, user := bearer(e.token(1, "admin")), bearer(e.token(2, "user"))
	e.identity.keys["x"] = &identityv1.LookupAPIKeyResponse{}

	for _, tc := range []struct {
		method, path, body string
		hdr                map[string]string
		wantStatus         int
	}{
		{"GET", "/v1/markets", "", nil, 200},
		{"POST", "/v1/auth/signup", `{"email":"a@b.co","password":"correct horse battery"}`, nil, 201},
		{"POST", "/v1/auth/login", `{"email":"a@b.co","password":"correct horse battery"}`, nil, 200},
		{"GET", "/v1/balances", "", user, 200},
		{"GET", "/v1/ledger", "", user, 200},
		{"POST", "/v1/orders", orderBody, user, 201},
		{"GET", "/v1/orders", "", user, 200},
		{"GET", "/v1/orders/5", "", user, 200},
		{"DELETE", "/v1/orders/5", "", user, 202},
		{"DELETE", "/v1/orders", "", user, 202},
		{"POST", "/v1/api-keys", `{"label":"bot","can_trade":true}`, user, 201},
		{"GET", "/v1/admin/users", "", admin, 200},
		{"GET", "/v1/admin/users/2/balances", "", admin, 200},
		{"GET", "/v1/admin/users/2/ledger", "", admin, 200},
		{"POST", "/v1/admin/markets/BTC-USDT/halt", "", admin, 200},
		{"GET", "/v1/balances", "", nil, 401},
		{"GET", "/v1/admin/users", "", user, 403},
		{"GET", "/v1/orders/404", "", user, 404},
	} {
		rec := e.do(tc.method, tc.path, tc.body, tc.hdr)
		require.Equal(t, tc.wantStatus, rec.Code, "%s %s: %s", tc.method, tc.path, rec.Body.String())

		req := httptest.NewRequest(tc.method, "http://localhost:8080"+tc.path, nil)
		route, pathParams, err := router.FindRoute(req)
		require.NoError(t, err, "%s %s must be documented", tc.method, tc.path)
		input := &openapi3filter.ResponseValidationInput{
			RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req, PathParams: pathParams, Route: route,
				Options: &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc}},
			Status: rec.Code, Header: rec.Header(),
		}
		input.SetBodyBytes(rec.Body.Bytes())
		assert.NoError(t, openapi3filter.ValidateResponse(context.Background(), input), "%s %s -> %d", tc.method, tc.path, rec.Code)
	}
	_ = os.Stdout
}
