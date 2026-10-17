package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	candlesv1 "github.com/tickless/pkg/gen/tickless/candles/v1"
	"github.com/tickless/pkg/wshub"
)

// HTTPOptions configure the HTTP handler.
type HTTPOptions struct {
	// Ready reports readiness (e.g. caught up with the snapshot offsets).
	Ready func(context.Context) error
	// WSOrigins are Origin host patterns accepted for WebSocket upgrades.
	WSOrigins []string
	// MaxConnsPerIP caps simultaneous public WebSocket connections per client address (0 = 100).
	MaxConnsPerIP int
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

// Handler returns the public HTTP API: REST under /v1/market and the
// WebSocket at /ws/public.
func (s *Service) Handler(o HTTPOptions) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if o.Ready != nil {
			if err := o.Ready(r.Context()); err != nil {
				writeErr(w, http.StatusServiceUnavailable, "unavailable", err.Error())
				return
			}
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /v1/market/tickers", s.tickers)
	mux.HandleFunc("GET /v1/market/{market}/book", s.book)
	mux.HandleFunc("GET /v1/market/{market}/trades", s.trades)
	mux.HandleFunc("GET /v1/market/{market}/ticker", s.ticker)
	mux.HandleFunc("GET /v1/market/{market}/candles", s.candlesREST)

	limiter := newIPLimiter(o.MaxConnsPerIP)
	mux.HandleFunc("GET /ws/public", func(w http.ResponseWriter, r *http.Request) {
		ip := clientHost(r)
		s.Hub.Serve(w, r, wshub.Handler{
			Origins:   o.WSOrigins,
			Allow:     func(*http.Request) bool { return limiter.acquire(ip) },
			OnMessage: s.HandleMessage,
			OnClose:   func(*wshub.Conn) { limiter.release(ip) },
		})
	})
	return mux
}

func clientHost(r *http.Request) string {
	// Behind the gateway the real client is in X-Forwarded-For (set by the proxy); otherwise the peer.
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	h := r.RemoteAddr
	if i := strings.LastIndex(h, ":"); i > 0 {
		h = h[:i]
	}
	return h
}

func (s *Service) marketOr404(w http.ResponseWriter, r *http.Request) (*mkt, string, bool) {
	sym := r.PathValue("market")
	m := s.get(sym)
	if m == nil {
		writeErr(w, http.StatusNotFound, "not_found", "unknown market")
		return nil, "", false
	}
	return m, sym, true
}

func intParam(r *http.Request, key string, def, lo, hi int) int {
	n, err := strconv.Atoi(r.URL.Query().Get(key))
	if err != nil {
		return def
	}
	return min(max(n, lo), hi)
}

func (s *Service) book(w http.ResponseWriter, r *http.Request) {
	m, sym, ok := s.marketOr404(w, r)
	if !ok {
		return
	}
	spec, _ := s.specs.Spec(sym)
	depth := intParam(r, "depth", 50, 1, 500)
	m.mu.Lock()
	out := bookSnapshotMsg{Seq: m.st.BookSeq, Bids: levels(spec, m.st.Book.Best(true, depth)), Asks: levels(spec, m.st.Book.Best(false, depth))}
	m.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"market": sym, "seq": out.Seq, "bids": out.Bids, "asks": out.Asks})
}

func (s *Service) trades(w http.ResponseWriter, r *http.Request) {
	m, sym, ok := s.marketOr404(w, r)
	if !ok {
		return
	}
	spec, _ := s.specs.Spec(sym)
	limit := intParam(r, "limit", 50, 1, 500)
	m.mu.Lock()
	ts := m.st.Trades
	if len(ts) > limit {
		ts = ts[len(ts)-limit:]
	}
	out := make([]TradeMsg, 0, len(ts))
	for i := len(ts) - 1; i >= 0; i-- { // newest first
		out = append(out, tradeMsg(spec, ts[i]))
	}
	m.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"market": sym, "trades": out})
}

func (s *Service) ticker(w http.ResponseWriter, r *http.Request) {
	m, sym, ok := s.marketOr404(w, r)
	if !ok {
		return
	}
	spec, _ := s.specs.Spec(sym)
	m.mu.Lock()
	tm := tickerMsg(spec, m.st, s.now())
	m.mu.Unlock()
	writeJSON(w, http.StatusOK, tm)
}

func (s *Service) tickers(w http.ResponseWriter, _ *http.Request) {
	out := []TickerMsg{}
	for _, spec := range s.specs.All() {
		if m := s.get(spec.Symbol); m != nil {
			m.mu.Lock()
			out = append(out, tickerMsg(spec, m.st, s.now()))
			m.mu.Unlock()
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"tickers": out})
}

func (s *Service) candlesREST(w http.ResponseWriter, r *http.Request) {
	_, sym, ok := s.marketOr404(w, r)
	if !ok {
		return
	}
	spec, _ := s.specs.Spec(sym)
	interval := r.URL.Query().Get("interval")
	if interval == "" {
		interval = "1m"
	}
	if !validInterval(interval) {
		writeErr(w, http.StatusBadRequest, "invalid_argument", "interval must be one of "+strings.Join(Intervals, ", "))
		return
	}
	if s.candles == nil {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", "candle history unavailable")
		return
	}
	from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
	to, _ := strconv.ParseInt(r.URL.Query().Get("to"), 10, 64)
	if from < 0 || to < 0 || (to != 0 && to < from) {
		writeErr(w, http.StatusBadRequest, "invalid_argument", "invalid time range")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	resp, err := s.candles.GetCandles(ctx, &candlesv1.GetCandlesRequest{
		Market: sym, Interval: interval, FromUnix: from, ToUnix: to, Limit: int32(intParam(r, "limit", 500, 1, 2000))})
	if err != nil {
		if status.Code(err) == codes.InvalidArgument {
			writeErr(w, http.StatusBadRequest, "invalid_argument", status.Convert(err).Message())
			return
		}
		slog.ErrorContext(r.Context(), "candles backend failed", "err", err)
		writeErr(w, http.StatusServiceUnavailable, "unavailable", "candle history unavailable")
		return
	}
	out := make([]CandleMsg, len(resp.Candles))
	for i, c := range resp.Candles {
		out[len(resp.Candles)-1-i] = candleMsg(spec, c) // oldest first for charting
	}
	writeJSON(w, http.StatusOK, map[string]any{"market": sym, "interval": interval, "candles": out})
}

// ipLimiter caps concurrent WebSocket connections per client address.
type ipLimiter struct {
	max int
	mu  sync.Mutex
	n   map[string]int
}

func newIPLimiter(max int) *ipLimiter {
	if max <= 0 {
		max = 100
	}
	return &ipLimiter{max: max, n: map[string]int{}}
}

func (l *ipLimiter) acquire(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.n[ip] >= l.max {
		return false
	}
	l.n[ip]++
	return true
}

func (l *ipLimiter) release(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.n[ip] > 0 {
		l.n[ip]--
	}
	if l.n[ip] == 0 {
		delete(l.n, ip)
	}
}
