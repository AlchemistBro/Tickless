//go:build integration

package rabbit_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/tickless/pkg/rabbit"
	"github.com/tickless/pkg/testkit"
)

func TestRetryWithBackoffThenSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := rabbit.Dial(ctx, testkit.RabbitMQ(t))
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	q := rabbit.RetryQueue{Name: "jobs", Steps: []time.Duration{200 * time.Millisecond, 400 * time.Millisecond}}
	require.NoError(t, c.Declare(q))
	require.NoError(t, c.Publish(ctx, q.Name, []byte("hello"), nil))

	var mu sync.Mutex
	var times []time.Time
	done := make(chan struct{})
	cctx, stop := context.WithCancel(ctx)
	go func() {
		_ = c.Consume(cctx, q, 4, func(_ context.Context, d amqp.Delivery) error {
			mu.Lock()
			defer mu.Unlock()
			times = append(times, time.Now())
			if len(times) < 3 {
				return errors.New("smtp down")
			}
			require.Equal(t, "hello", string(d.Body))
			close(done)
			return nil
		})
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("message was not retried to success")
	}
	stop()
	require.Len(t, times, 3, "two failures, then success on the third attempt")
	require.GreaterOrEqual(t, times[1].Sub(times[0]), 150*time.Millisecond, "first backoff step")
	require.GreaterOrEqual(t, times[2].Sub(times[1]), 350*time.Millisecond, "second step is longer")
	n, err := c.Depth(q.DLQ())
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestExhaustedAndPermanentFailuresGoToTheDLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := rabbit.Dial(ctx, testkit.RabbitMQ(t))
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	q := rabbit.RetryQueue{Name: "mail", Steps: []time.Duration{100 * time.Millisecond}}
	require.NoError(t, c.Declare(q))
	require.NoError(t, c.Publish(ctx, q.Name, []byte("always-fails"), nil))
	require.NoError(t, c.Publish(ctx, q.Name, []byte("bad-template"), nil))

	var mu sync.Mutex
	seen := map[string]int{}
	cctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		_ = c.Consume(cctx, q, 4, func(_ context.Context, d amqp.Delivery) error {
			mu.Lock()
			seen[string(d.Body)]++
			mu.Unlock()
			if string(d.Body) == "bad-template" {
				return rabbit.Permanent(errors.New("unknown template"))
			}
			return errors.New("smtp down")
		})
	}()
	require.Eventually(t, func() bool {
		n, err := c.Depth(q.DLQ())
		return err == nil && n == 2
	}, 30*time.Second, 100*time.Millisecond, "both messages end in the DLQ")
	mu.Lock()
	require.Equal(t, 2, seen["always-fails"], "1 + len(Steps) attempts")
	require.Equal(t, 1, seen["bad-template"], "a permanent failure is not retried")
	mu.Unlock()

	// The dead-lettered copy keeps the attempt count and the last error.
	ch, err := c.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()
	d, ok, err := ch.Get(q.DLQ(), true)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, d.Headers[rabbit.HeaderLastError])
	require.NotNil(t, d.Headers[rabbit.HeaderAttempts])
}
