package httpapi

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

// newProxy forwards public market-data requests (REST and WebSocket upgrades)
// to the marketdata service. Credentials are never forwarded to it. The client
// address is passed in X-Forwarded-For (an inbound X-Forwarded-For is kept only
// when the gateway itself sits behind a trusted proxy).
func newProxy(target string, trustProxy bool) http.Handler {
	u, err := url.Parse(target)
	if err != nil {
		panic("httpapi: invalid market data URL: " + err.Error())
	}
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
			pr.Out.Host = pr.In.Host // keep the public Host so WebSocket origin checks behave like a direct connection
			if trustProxy {
				pr.Out.Header["X-Forwarded-For"] = pr.In.Header["X-Forwarded-For"]
			}
			pr.SetXForwarded()
			for _, h := range []string{"Authorization", "Cookie", "X-API-KEY", "X-API-SIGNATURE", "X-API-TIMESTAMP"} {
				pr.Out.Header.Del(h)
			}
		},
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   64,
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: 15 * time.Second,
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.WarnContext(r.Context(), "market data backend unavailable", "err", err)
			writeErr(w, http.StatusBadGateway, "unavailable", "market data temporarily unavailable")
		},
	}
	return rp
}
