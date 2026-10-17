// Package wshub is a small WebSocket fan-out hub: connections subscribe to
// named channels, publishers push messages to a channel without ever blocking.
//
// Back-pressure rule: every connection has a bounded outbound queue. A client
// that cannot keep up is disconnected (it reconnects and resyncs from a
// snapshot) instead of slowing the publisher or growing memory without bound.
package wshub

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const (
	queueSize    = 4096 // ~1 s of a very busy market; beyond that the client is too slow and is dropped
	writeTimeout = 5 * time.Second
	pingEvery    = 20 * time.Second
	readLimit    = 8 << 10
)

type message struct {
	data []byte
	ts   time.Time // when the underlying event happened (zero = do not measure)
	// closeAfter, if set, disconnects the client with this reason once the
	// message has been written.
	closeAfter string
}

// Conn is one client connection.
type Conn struct {
	hub    *Hub
	ws     *websocket.Conn
	out    chan message
	ctx    context.Context
	cancel context.CancelCauseFunc
	subs   map[string]struct{} // guarded by hub.mu
	// Data is free for the application (e.g. the authenticated user id).
	Data atomic.Value
	// Remote is the client address as seen by the server.
	Remote string
}

// Send queues a message for the client without blocking. It returns false (and
// disconnects the client) when the queue is full. ts, if non-zero, is the time
// of the originating event; delivery latency is measured against it.
func (c *Conn) Send(data []byte, ts time.Time) bool {
	select {
	case c.out <- message{data: data, ts: ts}:
		return true
	default:
		c.hub.kicks.Add(c.ctx, 1, metric.WithAttributes(attrHub(c.hub.name)))
		c.cancel(errors.New("slow consumer"))
		return false
	}
}

// SendAndClose queues a final message and disconnects the client once it has
// been written (Close alone could drop the message).
func (c *Conn) SendAndClose(data []byte, reason string) {
	select {
	case c.out <- message{data: data, closeAfter: reason}:
	default:
		c.cancel(errors.New(reason))
	}
}

// Close disconnects the client immediately.
func (c *Conn) Close(reason string) { c.cancel(errors.New(reason)) }

// Hub routes published messages to subscribed connections.
type Hub struct {
	name  string
	mu    sync.RWMutex
	chans map[string]map[*Conn]struct{}
	conns atomic.Int64

	sent      metric.Int64Counter
	kicks     metric.Int64Counter
	delivered metric.Float64Histogram
}

// New creates a hub; name labels its metrics.
func New(name string) *Hub {
	m := otel.Meter("tickless/wshub")
	h := &Hub{name: name, chans: map[string]map[*Conn]struct{}{}}
	h.sent, _ = m.Int64Counter("ws_messages_sent_total", metric.WithDescription("WebSocket messages written"))
	h.kicks, _ = m.Int64Counter("ws_slow_consumer_disconnects_total", metric.WithDescription("Clients dropped because their queue was full"))
	h.delivered, _ = m.Float64Histogram("ws_delivery_seconds",
		metric.WithDescription("Time from the originating event to the WebSocket write"),
		metric.WithExplicitBucketBoundaries(.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5))
	g, err := m.Int64ObservableGauge("ws_connections", metric.WithDescription("Open WebSocket connections"))
	if err == nil {
		_, _ = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(g, h.conns.Load(), metric.WithAttributes(attrHub(name)))
			return nil
		}, g)
	}
	return h
}

// Connections returns the number of open connections.
func (h *Hub) Connections() int64 { return h.conns.Load() }

// Subscribe adds the connection to a channel.
func (h *Hub) Subscribe(c *Conn, channel string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.chans[channel] == nil {
		h.chans[channel] = map[*Conn]struct{}{}
	}
	h.chans[channel][c] = struct{}{}
	c.subs[channel] = struct{}{}
}

// Unsubscribe removes the connection from a channel.
func (h *Hub) Unsubscribe(c *Conn, channel string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.drop(c, channel)
}

// Subscriptions returns how many channels the connection is on.
func (h *Hub) Subscriptions(c *Conn) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(c.subs)
}

func (h *Hub) drop(c *Conn, channel string) {
	if m := h.chans[channel]; m != nil {
		delete(m, c)
		if len(m) == 0 {
			delete(h.chans, channel)
		}
	}
	delete(c.subs, channel)
}

// Publish sends data to every subscriber of channel. It never blocks.
func (h *Hub) Publish(channel string, data []byte, ts time.Time) {
	h.mu.RLock()
	conns := h.chans[channel]
	if len(conns) == 0 {
		h.mu.RUnlock()
		return
	}
	targets := make([]*Conn, 0, len(conns))
	for c := range conns {
		targets = append(targets, c)
	}
	h.mu.RUnlock()
	for _, c := range targets {
		c.Send(data, ts)
	}
}

// Handler callbacks of Serve.
type Handler struct {
	// Allow, if set, can refuse the upgrade (e.g. per-IP connection limits).
	Allow func(r *http.Request) bool
	// OnOpen is called once the connection is established.
	OnOpen func(c *Conn)
	// OnMessage is called for every text message from the client, serially.
	OnMessage func(c *Conn, data []byte)
	// OnClose is called after the connection ended.
	OnClose func(c *Conn)
	// Origins are allowed Origin host patterns (empty = same origin only).
	Origins []string
}

// Serve upgrades the request and runs the connection until it ends.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, cb Handler) {
	if cb.Allow != nil && !cb.Allow(r) {
		http.Error(w, "too many connections", http.StatusTooManyRequests)
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: cb.Origins})
	if err != nil {
		return // Accept already wrote the HTTP error
	}
	ws.SetReadLimit(readLimit)
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(r.Context()))
	c := &Conn{hub: h, ws: ws, out: make(chan message, queueSize), ctx: ctx, cancel: cancel,
		subs: map[string]struct{}{}, Remote: r.RemoteAddr}
	h.conns.Add(1)
	defer func() {
		h.conns.Add(-1)
		h.mu.Lock()
		for ch := range c.subs {
			h.drop(c, ch)
		}
		h.mu.Unlock()
		cancel(nil)
		if cb.OnClose != nil {
			cb.OnClose(c)
		}
	}()
	if cb.OnOpen != nil {
		cb.OnOpen(c)
	}

	done := make(chan struct{})
	go func() { defer close(done); h.writeLoop(c) }()
	for ctx.Err() == nil {
		typ, data, err := ws.Read(ctx)
		if err != nil {
			cancel(err)
			break
		}
		if typ != websocket.MessageText {
			cancel(errors.New("binary messages are not supported"))
			break
		}
		if cb.OnMessage != nil {
			cb.OnMessage(c, data)
		}
	}
	cancel(nil)
	<-done
	reason := "closing"
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		reason = cause.Error()
		if len(reason) > 100 {
			reason = reason[:100]
		}
	}
	if reason == "slow consumer" {
		// The peer is not reading, so a close handshake would only wait for it: drop the socket.
		_ = ws.CloseNow()
		return
	}
	_ = ws.Close(websocket.StatusNormalClosure, reason)
}

func (h *Hub) writeLoop(c *Conn) {
	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case m := <-c.out:
			wctx, cancel := context.WithTimeout(c.ctx, writeTimeout)
			err := c.ws.Write(wctx, websocket.MessageText, m.data)
			cancel()
			if err != nil {
				c.cancel(err)
				return
			}
			h.sent.Add(c.ctx, 1, metric.WithAttributes(attrHub(h.name)))
			if !m.ts.IsZero() {
				h.delivered.Record(c.ctx, time.Since(m.ts).Seconds(), metric.WithAttributes(attrHub(h.name)))
			}
			if m.closeAfter != "" {
				c.cancel(errors.New(m.closeAfter))
				return
			}
		case <-ping.C:
			pctx, cancel := context.WithTimeout(c.ctx, writeTimeout)
			err := c.ws.Ping(pctx)
			cancel()
			if err != nil {
				slog.Debug("ws ping failed", "err", err)
				c.cancel(err)
				return
			}
		}
	}
}
