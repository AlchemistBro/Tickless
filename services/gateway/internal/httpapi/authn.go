package httpapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tickless/pkg/auth"
	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
)

func attrString(k, v string) attribute.KeyValue  { return attribute.String(k, v) }
func attrInt(k string, v int) attribute.KeyValue { return attribute.Int(k, v) }
func itoa(n int) string                          { return strconv.Itoa(n) }
func itoa64(n int64) string                      { return strconv.FormatInt(n, 10) }

// Principal is the authenticated caller.
type Principal struct {
	UserID int64
	Role   string
	// ViaKey is true when authenticated by a signed API key (no session).
	ViaKey   bool
	CanTrade bool
}

type principalKey struct{}

func principalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

const (
	apiKeyCacheTTL = 30 * time.Second
	signatureSkew  = 30 * time.Second
	keyRefetchGap  = 10 * time.Second
)

// verifier returns the token verifier, fetching the identity public key lazily.
func (s *Server) verifier(ctx context.Context, force bool) (*auth.Verifier, error) {
	if v := s.verifierPtr(); v != nil && !force {
		return v, nil
	}
	s.keyMu.Lock()
	defer s.keyMu.Unlock()
	if v := s.verifierPtr(); v != nil && (!force || s.d.Now().Sub(s.lastKey) < keyRefetchGap) {
		return v, nil
	}
	pub, err := s.d.PublicKey(ctx)
	if err != nil {
		return nil, err
	}
	v := auth.NewVerifier(pub)
	s.verif.Store(v)
	s.lastKey = s.d.Now()
	return v, nil
}

func (s *Server) verifierPtr() *auth.Verifier { return s.verif.Load() }

// authenticate accepts either "Authorization: Bearer <jwt>" or the signed API
// key headers, and attaches the Principal. It rejects frozen users.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p Principal
		var err error
		switch {
		case strings.HasPrefix(r.Header.Get("Authorization"), "Bearer "):
			p, err = s.fromToken(r)
		case r.Header.Get("X-API-KEY") != "":
			p, err = s.fromAPIKey(r)
		default:
			err = errUnauthenticated
		}
		if err != nil {
			s.denyAuth(w, r, err)
			return
		}
		if s.isFrozen(r.Context(), p.UserID) {
			writeErr(w, http.StatusForbidden, "account_frozen", "account is frozen")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	})
}

var errUnauthenticated = errors.New("missing or invalid credentials")

func (s *Server) denyAuth(w http.ResponseWriter, r *http.Request, err error) {
	if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", "authentication temporarily unavailable")
		return
	}
	w.Header().Set("WWW-Authenticate", `Bearer realm="tickless"`)
	// One generic message for every failure: do not reveal which part was wrong.
	writeErr(w, http.StatusUnauthorized, "unauthenticated", "missing or invalid credentials")
}

func (s *Server) fromToken(r *http.Request) (Principal, error) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	v, err := s.verifier(r.Context(), false)
	if err != nil {
		return Principal{}, err
	}
	c, err := v.Verify(token, s.d.Now())
	if err != nil {
		// The identity service may have restarted with a new ephemeral key: refetch once.
		if v2, ferr := s.verifier(r.Context(), true); ferr == nil && v2 != v {
			c, err = v2.Verify(token, s.d.Now())
		}
		if err != nil {
			return Principal{}, errUnauthenticated
		}
	}
	return Principal{UserID: c.UserID, Role: c.Role, CanTrade: true}, nil
}

func (s *Server) fromAPIKey(r *http.Request) (Principal, error) {
	keyID := r.Header.Get("X-API-KEY")
	ts, err := strconv.ParseInt(r.Header.Get("X-API-TIMESTAMP"), 10, 64)
	sig := r.Header.Get("X-API-SIGNATURE")
	if err != nil || sig == "" {
		return Principal{}, errUnauthenticated
	}
	k, err := s.lookupKey(r.Context(), keyID)
	if err != nil {
		return Principal{}, err
	}
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxBody))
	if err != nil {
		return Principal{}, errUnauthenticated
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	if err := auth.VerifyRequest(k.Secret, ts, r.Method, r.URL.RequestURI(), body, sig, s.d.Now(), signatureSkew); err != nil {
		return Principal{}, errUnauthenticated
	}
	return Principal{UserID: k.UserId, Role: k.Role, ViaKey: true, CanTrade: k.CanTrade}, nil
}

func (s *Server) lookupKey(ctx context.Context, keyID string) (*identityv1.LookupAPIKeyResponse, error) {
	if v, ok := s.keyCache.Load(keyID); ok {
		if c := v.(cachedKey); s.d.Now().Before(c.expires) {
			return c.resp, nil
		}
	}
	resp, err := s.d.Identity.LookupAPIKey(ctx, &identityv1.LookupAPIKeyRequest{KeyId: keyID})
	if err != nil {
		if st, _ := status.FromError(err); st.Code() == codes.Unavailable {
			return nil, err
		}
		s.keyCache.Delete(keyID)
		return nil, errUnauthenticated
	}
	s.keyCache.Store(keyID, cachedKey{resp: resp, expires: s.d.Now().Add(apiKeyCacheTTL)})
	return resp, nil
}

// isFrozen consults the Redis set maintained from users.events. It fails open
// on Redis errors: frozen users cannot log in or refresh anyway, so the window
// is at most one access-token lifetime.
func (s *Server) isFrozen(ctx context.Context, uid int64) bool {
	ok, err := s.d.Redis.SIsMember(ctx, FrozenSet, itoa64(uid)).Result()
	return err == nil && ok
}

// FrozenSet is the Redis set of frozen user ids.
const FrozenSet = "frozen_users"

func (s *Server) requireTrade(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p, _ := principalFrom(r.Context()); !p.CanTrade {
			writeErr(w, http.StatusForbidden, "forbidden", "this API key may not trade")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p, _ := principalFrom(r.Context()); p.ViaKey {
			writeErr(w, http.StatusForbidden, "forbidden", "this endpoint requires a login session, not an API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) requireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p, _ := principalFrom(r.Context()); p.Role != auth.RoleAdmin {
			writeErr(w, http.StatusForbidden, "forbidden", "admin role required")
			return
		}
		next.ServeHTTP(w, r)
	})
}
