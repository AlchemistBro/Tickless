//go:build integration

package outbox_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/tickless/pkg/idempotency"
	"github.com/tickless/pkg/kafka"
	"github.com/tickless/pkg/outbox"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/testkit"
)

func setup(t *testing.T) (*outboxEnv, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	dsn := testkit.Postgres(t)
	brokers := testkit.Kafka(t, "t.out")
	pool, err := pg.Connect(ctx, dsn, 5)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, outbox.Schema+idempotency.Schema)
	require.NoError(t, err)
	prod, err := kafka.NewProducer(brokers)
	require.NoError(t, err)
	t.Cleanup(prod.Close)
	return &outboxEnv{pool: pool, prod: prod, brokers: brokers}, ctx
}

type outboxEnv struct {
	pool    *pgxpool.Pool
	prod    *kafka.Producer
	brokers []string
}

func consume(t *testing.T, brokers []string, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics("t.out"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var got []*kgo.Record
	for len(got) < n && ctx.Err() == nil {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) { got = append(got, r) })
	}
	return got
}

func TestRelayPublishesCommittedRowsOnly(t *testing.T) {
	env, ctx := setup(t)
	pool := env.pool

	// Rolled back transaction: message must never be published.
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, outbox.Enqueue(ctx, tx, "t.out", "k", []byte("rolled-back")))
	require.NoError(t, tx.Rollback(ctx))

	// Committed ones, with a trace context.
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	sctx, span := tp.Tracer("t").Start(ctx, "req")
	for i := 0; i < 5; i++ {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		require.NoError(t, outbox.Enqueue(sctx, tx, "t.out", "k", []byte(fmt.Sprintf("m%d", i))))
		require.NoError(t, tx.Commit(ctx))
	}
	span.End()

	rctx, stop := context.WithCancel(ctx)
	defer stop()
	relay := &outbox.Relay{Name: "test", Pool: pool, Producer: env.prod, BatchSize: 2}
	go func() { _ = relay.Run(rctx) }()

	recs := consume(t, env.brokers, 5, 60*time.Second)
	require.Len(t, recs, 5)
	for i, r := range recs {
		assert.Equal(t, fmt.Sprintf("m%d", i), string(r.Value), "published in commit order")
		assert.Contains(t, kafka.Header(r, "traceparent"), span.SpanContext().TraceID().String(), "trace context propagated")
	}
	// Nothing else (in particular not the rolled back row) shows up.
	assert.Len(t, consume(t, env.brokers, 6, 3*time.Second), 5)
}

func TestRelayRepublishesAfterCrashBeforeCommit(t *testing.T) {
	// Simulates "kill the relay between Kafka ack and marking published": rows
	// stay unpublished, so the next relay publishes them again (at-least-once).
	env, ctx := setup(t)
	pool := env.pool
	tx, _ := pool.Begin(ctx)
	require.NoError(t, outbox.Enqueue(ctx, tx, "t.out", "k", []byte("once")))
	require.NoError(t, tx.Commit(ctx))

	// First "relay": produce, then abort without marking (crash).
	require.NoError(t, env.prod.Produce(ctx, []kafka.Record{{Topic: "t.out", Key: []byte("k"), Value: []byte("once")}}))

	rctx, stop := context.WithCancel(ctx)
	defer stop()
	relay := &outbox.Relay{Name: "test", Pool: pool, Producer: env.prod}
	go func() { _ = relay.Run(rctx) }()

	recs := consume(t, env.brokers, 2, 60*time.Second)
	assert.Len(t, recs, 2, "duplicate delivery is expected; consumers must dedupe")
}

func TestIdempotencyFirst(t *testing.T) {
	env, ctx := setup(t)
	pool := env.pool
	apply := func(id string) bool {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		first, err := idempotency.First(ctx, tx, "c1", id)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		return first
	}
	assert.True(t, apply("e1"))
	assert.False(t, apply("e1"))
	assert.True(t, apply("e2"))
}
