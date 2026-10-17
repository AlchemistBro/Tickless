// Package httpapi is the public REST edge of Tickless: authentication (JWT and
// HMAC API keys), rate limiting, idempotency keys, request validation and the
// mapping from gRPC services to JSON.
package httpapi

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tickless/gateway/internal/idem"
	"github.com/tickless/gateway/internal/ratelimit"
	"github.com/tickless/pkg/auth"
	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	ordersv1 "github.com/tickless/pkg/gen/tickless/orders/v1"
)

// Limits are token-bucket parameters (tokens per second, burst).
type Limits struct {
	IPRate, IPBurst       float64 // all requests per client IP
	AuthRate, AuthBurst   float64 // signup/login/refresh per client IP
	UserRate, UserBurst   float64 // all authenticated requests per user
	OrderRate, OrderBurst float64 // order placement/cancellation per user
}

// DefaultLimits are generous for humans and bots yet bound abuse.
var DefaultLimits = Limits{
	IPRate: 200, IPBurst: 400, AuthRate: 0.5, AuthBurst: 10,
	UserRate: 100, UserBurst: 200, OrderRate: 50, OrderBurst: 100,
}

// Deps are the collaborators of the API.
type Deps struct {
	Identity identityv1.IdentityServiceClient
	Ledger   ledgerv1.LedgerServiceClient
	Orders   ordersv1.OrdersServiceClient
	Redis    redis.UniversalClient
	// PublicKey fetches the Ed25519 key that verifies access tokens.
	PublicKey      func(context.Context) (ed25519.PublicKey, error)
	Limits         Limits
	AllowedOrigins []string
	// TrustProxy makes the client IP come from X-Forwarded-For (set only behind a trusted proxy).
	TrustProxy bool
	Now        func() time.Time
}

// Server is the HTTP API.
type Server struct {
	d        Deps
	limiter  *ratelimit.Limiter
	idem     *idem.Store
	verif    atomic.Pointer[auth.Verifier]
	keyMu    sync.Mutex
	lastKey  time.Time
	keyCache sync.Map // key id -> cachedKey

	reqs     metric.Int64Counter
	dur      metric.Float64Histogram
	limited  metric.Int64Counter
	failOpen metric.Int64Counter
}

type cachedKey struct {
	resp    *identityv1.LookupAPIKeyResponse
	expires time.Time
}

// New builds the server.
func New(d Deps) *Server {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Limits == (Limits{}) {
		d.Limits = DefaultLimits
	}
	s := &Server{d: d, limiter: ratelimit.New(d.Redis), idem: idem.New(d.Redis)}
	m := otel.Meter("tickless/gateway")
	s.reqs, _ = m.Int64Counter("http_server_requests_total", metric.WithDescription("HTTP requests by route and status"))
	s.dur, _ = m.Float64Histogram("http_server_request_duration_seconds", metric.WithDescription("HTTP request latency"),
		metric.WithExplicitBucketBoundaries(.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5))
	s.limited, _ = m.Int64Counter("gateway_rate_limited_total", metric.WithDescription("Requests rejected by rate limiting"))
	s.failOpen, _ = m.Int64Counter("gateway_ratelimit_fail_open_total", metric.WithDescription("Rate-limit checks skipped because Redis was unreachable"))
	return s
}

// ---- JSON helpers ----

type errBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	var b errBody
	b.Error.Code, b.Error.Message = code, msg
	writeJSON(w, status, b)
}

// writeGRPCErr maps a backend error to an HTTP error. Server-side errors never
// leak internal messages.
func writeGRPCErr(w http.ResponseWriter, r *http.Request, err error) {
	st, _ := status.FromError(err)
	var httpStatus int
	switch st.Code() {
	case codes.InvalidArgument:
		httpStatus = http.StatusBadRequest
	case codes.Unauthenticated:
		httpStatus = http.StatusUnauthorized
	case codes.PermissionDenied:
		httpStatus = http.StatusForbidden
	case codes.NotFound:
		httpStatus = http.StatusNotFound
	case codes.AlreadyExists:
		httpStatus = http.StatusConflict
	case codes.FailedPrecondition:
		httpStatus = http.StatusUnprocessableEntity
	case codes.ResourceExhausted:
		httpStatus = http.StatusTooManyRequests
	case codes.Unavailable:
		httpStatus = http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		httpStatus = http.StatusGatewayTimeout
	default:
		httpStatus = http.StatusInternalServerError
	}
	if httpStatus >= 500 {
		if httpStatus == http.StatusInternalServerError {
			slog.ErrorContext(r.Context(), "backend error", "err", err)
		}
		writeErr(w, httpStatus, strings.ToLower(strings.ReplaceAll(st.Code().String(), " ", "_")), "service temporarily unavailable")
		return
	}
	writeErr(w, httpStatus, strings.ToLower(strings.ReplaceAll(st.Code().String(), " ", "_")), st.Message())
}

const maxBody = 1 << 20

// decode reads a strict JSON body: unknown fields and trailing data are errors.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErr(w, http.StatusRequestEntityTooLarge, "invalid_argument", "request body too large")
		} else {
			writeErr(w, http.StatusBadRequest, "invalid_argument", "malformed JSON body: "+err.Error())
		}
		return false
	}
	if dec.More() {
		writeErr(w, http.StatusBadRequest, "invalid_argument", "unexpected data after JSON body")
		return false
	}
	return true
}

// ---- router ----

// Routes builds the HTTP handler.
func (s *Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(s.recoverer, s.securityHeaders, s.cors, s.observe)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	r.Get("/readyz", func(w http.ResponseWriter, req *http.Request) {
		if err := s.d.Redis.Ping(req.Context()).Err(); err != nil {
			writeErr(w, http.StatusServiceUnavailable, "unavailable", "redis unreachable")
			return
		}
		_, _ = w.Write([]byte("ok"))
	})

	r.Route("/v1", func(r chi.Router) {
		r.Use(s.ipRateLimit)

		// Public.
		r.Get("/markets", s.listMarkets)
		r.Group(func(r chi.Router) {
			r.Use(s.authRateLimit)
			r.Post("/auth/signup", s.signup)
			r.Post("/auth/verify-email", s.verifyEmail)
			r.Post("/auth/login", s.login)
			r.Post("/auth/refresh", s.refresh)
			r.Post("/auth/logout", s.logout)
		})

		// Authenticated (JWT or signed API key).
		r.Group(func(r chi.Router) {
			r.Use(s.authenticate, s.userRateLimit, s.idem.Middleware(s.scope, writeErr))
			r.Get("/balances", s.balances)
			r.Get("/ledger", s.ledgerEntries)
			r.Get("/orders", s.listOrders)
			r.Get("/orders/{id}", s.getOrder)
			r.Group(func(r chi.Router) {
				r.Use(s.requireTrade, s.orderRateLimit)
				r.Post("/orders", s.placeOrder)
				r.Delete("/orders/{id}", s.cancelOrder)
				r.Delete("/orders", s.cancelAll)
			})
			// API keys are managed with a session token only, never with a key.
			r.Group(func(r chi.Router) {
				r.Use(s.requireSession)
				r.Get("/api-keys", s.listAPIKeys)
				r.Post("/api-keys", s.createAPIKey)
				r.Delete("/api-keys/{keyId}", s.revokeAPIKey)
			})
		})

		// Admin (role claim "admin" in a session token).
		r.Route("/admin", func(r chi.Router) {
			r.Use(s.authenticate, s.requireSession, s.requireAdmin, s.userRateLimit, s.idem.Middleware(s.scope, writeErr))
			r.Get("/users", s.adminListUsers)
			r.Post("/users/{id}/freeze", s.adminFreeze(true))
			r.Post("/users/{id}/unfreeze", s.adminFreeze(false))
			r.Get("/users/{id}/balances", s.adminBalances)
			r.Get("/users/{id}/ledger", s.adminLedger)
			r.Post("/markets/{symbol}/halt", s.adminHalt(true))
			r.Post("/markets/{symbol}/resume", s.adminHalt(false))
		})
	})
	return r
}

// ---- middleware ----

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(rec) // net/http's deliberate abort: let the server handle it
				}
				slog.ErrorContext(r.Context(), "panic in handler", "panic", rec, "stack", string(debug.Stack()))
				writeErr(w, http.StatusInternalServerError, "internal", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Cache-Control", "no-store")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			for _, o := range s.d.AllowedOrigins {
				if o == origin {
					h := w.Header()
					h.Set("Access-Control-Allow-Origin", origin)
					h.Set("Vary", "Origin")
					h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Idempotency-Key")
					h.Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
					h.Set("Access-Control-Max-Age", "600")
				}
			}
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(c int) { r.code = c; r.ResponseWriter.WriteHeader(c) }

// observe records RED metrics and an access log line per request.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := s.d.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		attrs := metric.WithAttributes(attrString("method", r.Method), attrString("route", route), attrInt("status", rec.code))
		s.reqs.Add(r.Context(), 1, attrs)
		s.dur.Record(r.Context(), s.d.Now().Sub(start).Seconds(), attrs)
		if rec.code >= 500 {
			slog.WarnContext(r.Context(), "request failed", "method", r.Method, "route", route, "status", rec.code)
		}
	})
}

func (s *Server) clientIP(r *http.Request) string {
	if s.d.TrustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			return strings.TrimSpace(strings.Split(xff, ",")[0])
		}
	}
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	return host
}

func (s *Server) rateLimit(w http.ResponseWriter, r *http.Request, key string, rate, burst float64, class string) bool {
	res, err := s.limiter.Allow(r.Context(), key, rate, burst, 1)
	if err != nil {
		// Fail open: an unreachable limiter must not take the exchange down.
		s.failOpen.Add(r.Context(), 1)
		return true
	}
	if !res.Allowed {
		s.limited.Add(r.Context(), 1, metric.WithAttributes(attrString("class", class)))
		secs := int(res.RetryAfter.Seconds()) + 1
		w.Header().Set("Retry-After", itoa(secs))
		writeErr(w, http.StatusTooManyRequests, "rate_limited", "too many requests")
		return false
	}
	return true
}

func (s *Server) ipRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.rateLimit(w, r, "rl:ip:"+s.clientIP(r), s.d.Limits.IPRate, s.d.Limits.IPBurst, "ip") {
			next.ServeHTTP(w, r)
		}
	})
}

func (s *Server) authRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.rateLimit(w, r, "rl:auth:"+s.clientIP(r), s.d.Limits.AuthRate, s.d.Limits.AuthBurst, "auth") {
			next.ServeHTTP(w, r)
		}
	})
}

func (s *Server) userRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _ := principalFrom(r.Context())
		if s.rateLimit(w, r, "rl:user:"+itoa64(p.UserID), s.d.Limits.UserRate, s.d.Limits.UserBurst, "user") {
			next.ServeHTTP(w, r)
		}
	})
}

func (s *Server) orderRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _ := principalFrom(r.Context())
		if s.rateLimit(w, r, "rl:order:"+itoa64(p.UserID), s.d.Limits.OrderRate, s.d.Limits.OrderBurst, "order") {
			next.ServeHTTP(w, r)
		}
	})
}

// scope identifies the caller for idempotency keys.
func (s *Server) scope(r *http.Request) string {
	if p, ok := principalFrom(r.Context()); ok {
		return "u" + itoa64(p.UserID)
	}
	return "ip" + s.clientIP(r)
}

var _ = io.EOF
