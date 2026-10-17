package wshub_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/pkg/wshub"
)

type harness struct {
	hub  *wshub.Hub
	srv  *httptest.Server
	open chan *wshub.Conn
}

func newHarness(t *testing.T) *harness {
	h := &harness{hub: wshub.New("test"), open: make(chan *wshub.Conn, 16)}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.hub.Serve(w, r, wshub.Handler{
			OnOpen: func(c *wshub.Conn) { h.open <- c },
			OnMessage: func(c *wshub.Conn, data []byte) {
				// protocol of the test: "sub:<channel>" / "unsub:<channel>"
				switch s := string(data); {
				case strings.HasPrefix(s, "sub:"):
					h.hub.Subscribe(c, strings.TrimPrefix(s, "sub:"))
					c.Send([]byte("subscribed "+strings.TrimPrefix(s, "sub:")), time.Time{})
				case strings.HasPrefix(s, "unsub:"):
					h.hub.Unsubscribe(c, strings.TrimPrefix(s, "unsub:"))
					c.Send([]byte("unsubscribed"), time.Time{})
				}
			},
		})
	}))
	t.Cleanup(h.srv.Close)
	return h
}

func (h *harness) dial(t *testing.T) (*websocket.Conn, *wshub.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(h.srv.URL, "http"), nil)
	closeBody(resp)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.CloseNow() })
	return c, <-h.open
}

func read(t *testing.T, c *websocket.Conn) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, b, err := c.Read(ctx)
	require.NoError(t, err)
	return string(b)
}

func write(t *testing.T, c *websocket.Conn, s string) {
	t.Helper()
	require.NoError(t, c.Write(context.Background(), websocket.MessageText, []byte(s)))
}

func TestPublishReachesOnlySubscribers(t *testing.T) {
	h := newHarness(t)
	a, _ := h.dial(t)
	b, _ := h.dial(t)
	write(t, a, "sub:book.X")
	assert.Equal(t, "subscribed book.X", read(t, a))
	write(t, b, "sub:book.Y")
	assert.Equal(t, "subscribed book.Y", read(t, b))

	h.hub.Publish("book.X", []byte("x1"), time.Now())
	h.hub.Publish("book.Y", []byte("y1"), time.Time{})
	assert.Equal(t, "x1", read(t, a))
	assert.Equal(t, "y1", read(t, b))

	write(t, a, "unsub:book.X")
	assert.Equal(t, "unsubscribed", read(t, a))
	h.hub.Publish("book.X", []byte("x2"), time.Time{})
	write(t, a, "sub:book.X")
	assert.Equal(t, "subscribed book.X", read(t, a), "nothing was delivered while unsubscribed")
}

func TestOrderingPerChannelIsPreserved(t *testing.T) {
	h := newHarness(t)
	a, _ := h.dial(t)
	write(t, a, "sub:c")
	read(t, a)
	for i := 0; i < 200; i++ {
		h.hub.Publish("c", []byte{byte(i)}, time.Time{})
	}
	for i := 0; i < 200; i++ {
		assert.Equal(t, string([]byte{byte(i)}), read(t, a))
	}
}

// A client that never reads must be disconnected without ever blocking the
// publisher, while a client that keeps reading is unaffected.
func TestSlowConsumerIsDisconnectedAndPublisherNeverBlocks(t *testing.T) {
	h := newHarness(t)
	slow, slowConn := h.dial(t)
	fast, _ := h.dial(t)
	write(t, slow, "sub:c")
	write(t, fast, "sub:c")
	read(t, fast)

	var received atomic.Int64
	go func() { // the healthy client drains continuously
		for {
			if _, _, err := fast.Read(context.Background()); err != nil {
				return
			}
			received.Add(1)
		}
	}()

	// slow never reads. Publish far more than its queue plus the socket buffers can hold.
	payload := []byte(strings.Repeat("x", 8<<10))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 9000; i++ { // more than the 4096-message queue plus socket buffers can hold
			h.hub.Publish("c", payload, time.Time{})
			if i%10 == 0 {
				time.Sleep(time.Millisecond) // let the fast reader keep up
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("publisher blocked behind a slow consumer")
	}
	require.Eventually(t, func() bool { return h.hub.Subscriptions(slowConn) == 0 }, 10*time.Second, 20*time.Millisecond,
		"the slow connection was dropped from its channels")
	require.Eventually(t, func() bool { return h.hub.Connections() == 1 }, 10*time.Second, 20*time.Millisecond, "only the healthy client remains")
	assert.Positive(t, received.Load(), "the healthy client kept receiving")
	h.hub.Publish("c", []byte("still-alive"), time.Time{})
}

func TestConnectionCountAndCleanupOnClose(t *testing.T) {
	h := newHarness(t)
	a, conn := h.dial(t)
	write(t, a, "sub:c")
	read(t, a)
	assert.EqualValues(t, 1, h.hub.Connections())
	assert.Equal(t, 1, h.hub.Subscriptions(conn))
	require.NoError(t, a.Close(websocket.StatusNormalClosure, "bye"))
	require.Eventually(t, func() bool { return h.hub.Connections() == 0 }, 5*time.Second, 10*time.Millisecond)
	h.hub.Publish("c", []byte("nobody"), time.Time{}) // must not panic or leak
}

func TestAllowRefusesUpgrade(t *testing.T) {
	hub := wshub.New("deny")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.Serve(w, r, wshub.Handler{Allow: func(*http.Request) bool { return false }})
	}))
	defer srv.Close()
	_, resp, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	defer closeBody(resp)
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
}

// closeBody releases the handshake response (the linter cannot know Dial already handled the 101 body).
func closeBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}
