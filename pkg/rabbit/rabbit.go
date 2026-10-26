// Package rabbit wraps RabbitMQ (amqp091-go) for Tickless work queues (ADR 0007:
// RabbitMQ for tasks where per-message ack, delayed retry and a DLQ matter more
// than ordering).
//
// Topology of a retried queue "q":
//
//	q              the work queue (durable)
//	q.retry.<n>    one queue per backoff step, queue-level TTL, dead-lettered back to q
//	q.dlq          messages that exhausted the retry budget
//
// One queue per step (instead of per-message TTL in one queue) avoids head-of-
// line blocking: RabbitMQ only expires messages at the head of a queue.
//
// Delivery is at-least-once: a handler may see a message again after a crash
// between its side effect and the ack, so handlers are idempotent.
package rabbit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// HeaderAttempts counts failed handling attempts of a message.
const HeaderAttempts = "x-attempts"

// HeaderLastError carries the last handler error of a dead-lettered message.
const HeaderLastError = "x-last-error"

// Client is a connection with a confirming publish channel.
type Client struct {
	url  string
	mu   sync.Mutex
	conn *amqp.Connection
	pub  *amqp.Channel
}

// Dial connects, retrying until ctx is done (brokers start slowly).
func Dial(ctx context.Context, url string) (*Client, error) {
	c := &Client{url: url}
	for {
		err := c.connect()
		if err == nil {
			return c, nil
		}
		slog.Warn("rabbitmq not reachable yet", "err", err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("rabbitmq: %w (last error: %w)", ctx.Err(), err)
		case <-time.After(time.Second):
		}
	}
}

func (c *Client) connect() error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}
	if err := ch.Confirm(false); err != nil {
		_ = conn.Close()
		return err
	}
	c.conn, c.pub = conn, ch
	return nil
}

// Close closes the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Channel opens a new channel (reconnecting if needed).
func (c *Client) Channel() (*amqp.Channel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil || c.conn.IsClosed() {
		if err := c.connect(); err != nil {
			return nil, err
		}
	}
	return c.conn.Channel()
}

// DeclareQueue declares a durable work queue.
func (c *Client) DeclareQueue(name string) error {
	ch, err := c.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	_, err = ch.QueueDeclare(name, true, false, false, false, nil)
	return err
}

// RetryQueue is a work queue with exponential backoff steps and a DLQ.
type RetryQueue struct {
	Name  string
	Steps []time.Duration // backoff before attempt 2, 3, ...; len(Steps)+1 attempts in total
}

func (q RetryQueue) step(i int) string { return fmt.Sprintf("%s.retry.%d", q.Name, i+1) }

// DLQ is the dead-letter queue name.
func (q RetryQueue) DLQ() string { return q.Name + ".dlq" }

// Declare creates the queue, its retry steps and its DLQ.
func (c *Client) Declare(q RetryQueue) error {
	ch, err := c.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	if _, err := ch.QueueDeclare(q.Name, true, false, false, false, nil); err != nil {
		return err
	}
	for i, d := range q.Steps {
		args := amqp.Table{
			"x-message-ttl":             d.Milliseconds(),
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": q.Name,
		}
		if _, err := ch.QueueDeclare(q.step(i), true, false, false, false, args); err != nil {
			return err
		}
	}
	_, err = ch.QueueDeclare(q.DLQ(), true, false, false, false, nil)
	return err
}

// Publish sends a persistent message to queue (default exchange) and waits for
// the broker's confirmation.
func (c *Client) Publish(ctx context.Context, queue string, body []byte, headers amqp.Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil || c.conn.IsClosed() || c.pub == nil || c.pub.IsClosed() {
		if err := c.connect(); err != nil {
			return err
		}
	}
	conf, err := c.pub.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, ContentType: "application/octet-stream", Body: body, Headers: headers, Timestamp: time.Now(),
	})
	if err != nil {
		return err
	}
	ok, err := conf.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("rabbitmq: publish was nacked by the broker")
	}
	return nil
}

// Handler processes one message. Returning an error schedules a retry (or the
// DLQ once the budget is spent); Permanent(err) goes straight to the DLQ.
type Handler func(ctx context.Context, d amqp.Delivery) error

type permanent struct{ err error }

func (p permanent) Error() string { return "permanent: " + p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent marks a failure that retrying cannot fix.
func Permanent(err error) error { return permanent{err} }

func attempts(h amqp.Table) int {
	switch v := h[HeaderAttempts].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	}
	return 0
}

// Consume handles messages of q until ctx is cancelled. A failed message is
// re-published to the next backoff step with an incremented attempt counter
// and then acked; after the last step it goes to the DLQ. The original is acked
// only after the retry/DLQ copy was confirmed, so nothing is lost.
func (c *Client) Consume(ctx context.Context, q RetryQueue, prefetch int, h Handler) error {
	ch, err := c.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return err
	}
	msgs, err := ch.ConsumeWithContext(ctx, q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-msgs:
			if !ok {
				return errors.New("rabbitmq: delivery channel closed")
			}
			herr := h(ctx, d)
			if herr == nil {
				if err := d.Ack(false); err != nil {
					return err
				}
				continue
			}
			n := attempts(d.Headers) + 1
			headers := amqp.Table{}
			for k, v := range d.Headers {
				headers[k] = v
			}
			headers[HeaderAttempts] = int32(n) //nolint:gosec // attempt counts are tiny
			headers[HeaderLastError] = herr.Error()
			target := q.DLQ()
			var p permanent
			if !errors.As(herr, &p) && n-1 < len(q.Steps) {
				target = q.step(n - 1)
			}
			if err := c.Publish(ctx, target, d.Body, headers); err != nil {
				_ = d.Nack(false, true) // could not park it: put it back
				return err
			}
			slog.Warn("message failed", "queue", q.Name, "attempt", n, "to", target, "err", herr)
			if err := d.Ack(false); err != nil {
				return err
			}
		}
	}
}

// Depth returns the number of ready messages in a queue.
func (c *Client) Depth(name string) (int, error) {
	ch, err := c.Channel()
	if err != nil {
		return 0, err
	}
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(name, true, false, false, false, nil)
	if err != nil {
		return 0, err
	}
	return q.Messages, nil
}
