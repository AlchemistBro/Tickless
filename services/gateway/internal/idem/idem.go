// Package idem implements Idempotency-Key handling for mutating requests.
//
// First request with a key: executes and the response (status < 500) is stored
// for 24 h. Replay with the same key and body: the stored response is returned
// with "Idempotent-Replayed: true". Same key, different body: 422. Same key
// while the first request is still running: 409. Server errors are not stored,
// so a retry after a 5xx executes again.
package idem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	inProgressTTL = 60 * time.Second
	resultTTL     = 24 * time.Hour
	maxKeyLen     = 128
	maxBody       = 1 << 20
)

// Store is the Redis-backed idempotency store.
type Store struct{ rdb redis.Cmdable }

// New returns a Store.
func New(rdb redis.Cmdable) *Store { return &Store{rdb: rdb} }

// Middleware applies idempotency to POST/PUT/PATCH/DELETE requests that carry
// an Idempotency-Key. scope identifies the caller (user id or client IP) so
// keys of different callers never collide.
func (s *Store) Middleware(scope func(*http.Request) string, writeErr func(http.ResponseWriter, int, string, string)) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("Idempotency-Key")
			if key == "" || r.Method == http.MethodGet || r.Method == http.MethodHead {
				next.ServeHTTP(w, r)
				return
			}
			if len(key) > maxKeyLen {
				writeErr(w, http.StatusBadRequest, "invalid_argument", "Idempotency-Key too long")
				return
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
			if err != nil || len(body) > maxBody {
				writeErr(w, http.StatusRequestEntityTooLarge, "invalid_argument", "request body too large")
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			sum := sha256.Sum256(body)
			bodyHash := hex.EncodeToString(sum[:])
			rk := "idem:" + scope(r) + ":" + r.Method + ":" + r.URL.Path + ":" + key

			ok, err := s.rdb.SetNX(r.Context(), rk, "P|"+bodyHash, inProgressTTL).Result()
			if err != nil {
				// Redis down: run the request without idempotency rather than fail the exchange.
				next.ServeHTTP(w, r)
				return
			}
			if !ok {
				s.replay(w, r, rk, bodyHash, writeErr)
				return
			}
			rec := &recorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			if rec.status >= 500 {
				_ = s.rdb.Del(context.WithoutCancel(r.Context()), rk).Err()
				return
			}
			val := strings.Join([]string{"D", bodyHash, strconv.Itoa(rec.status), rec.header(), base64.StdEncoding.EncodeToString(rec.buf.Bytes())}, "|")
			_ = s.rdb.Set(context.WithoutCancel(r.Context()), rk, val, resultTTL).Err()
		})
	}
}

func (s *Store) replay(w http.ResponseWriter, r *http.Request, rk, bodyHash string, writeErr func(http.ResponseWriter, int, string, string)) {
	val, err := s.rdb.Get(r.Context(), rk).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) { // expired between SETNX and GET
			writeErr(w, http.StatusConflict, "idempotency_conflict", "request with this Idempotency-Key is being processed")
			return
		}
		writeErr(w, http.StatusServiceUnavailable, "unavailable", "idempotency store unavailable")
		return
	}
	parts := strings.SplitN(val, "|", 5)
	if len(parts) < 2 || parts[1] != bodyHash {
		writeErr(w, http.StatusUnprocessableEntity, "idempotency_key_reused", "Idempotency-Key was already used with a different request body")
		return
	}
	if parts[0] == "P" || len(parts) != 5 {
		writeErr(w, http.StatusConflict, "idempotency_conflict", "request with this Idempotency-Key is being processed")
		return
	}
	status, _ := strconv.Atoi(parts[2])
	body, _ := base64.StdEncoding.DecodeString(parts[4])
	if ct := parts[3]; ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.Header().Set("Idempotent-Replayed", "true")
	w.WriteHeader(status)
	_, _ = w.Write(body) //nolint:gosec // replays the gateway's own stored JSON response byte for byte
}

type recorder struct {
	http.ResponseWriter
	status int
	buf    bytes.Buffer
}

func (r *recorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }
func (r *recorder) Write(b []byte) (int, error) {
	if r.buf.Len() < maxBody {
		r.buf.Write(b)
	}
	return r.ResponseWriter.Write(b)
}
func (r *recorder) header() string { return r.Header().Get("Content-Type") }
