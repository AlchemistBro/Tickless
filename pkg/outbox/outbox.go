// Package outbox implements the transactional outbox: a service writes the
// domain change and the message to publish in ONE database transaction, and a
// relay publishes unpublished rows to Kafka afterwards (ADR 0005).
//
// Delivery is at-least-once: if the relay crashes after Kafka acknowledged a
// batch but before it marks the rows published, the batch is published again.
// Every consumer is therefore idempotent.
//
// Relay mechanism: polling with SELECT ... FOR UPDATE SKIP LOCKED, plus a
// transaction-scoped advisory lock so that only one relay instance publishes at
// a time, which preserves the commit order of messages across replicas.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/tickless/pkg/kafka"
)

// Schema is the table every service owning an outbox creates in its migrations.
const Schema = `
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT        NOT NULL,
    key          TEXT        NOT NULL,
    payload      BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
`

// Enqueue adds a message to the outbox inside tx. The current trace context is
// stored so the consumer continues the same trace.
func Enqueue(ctx context.Context, tx pgx.Tx, topic, key string, payload []byte) error {
	h, err := json.Marshal(kafka.TraceHeaders(ctx))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox (topic, key, payload, headers) VALUES ($1, $2, $3, $4::jsonb)`, topic, key, payload, string(h))
	return err
}

// Relay publishes outbox rows to Kafka.
type Relay struct {
	Name      string // identifies the outbox for the advisory lock and metrics
	Pool      *pgxpool.Pool
	Producer  *kafka.Producer
	BatchSize int

	backlog atomic.Int64
	oldest  atomic.Int64 // age of the oldest unpublished row, seconds
}

// Run publishes until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	if r.BatchSize == 0 {
		r.BatchSize = 200
	}
	r.registerMetrics()
	lastPrune, lastStats := time.Now(), time.Time{}
	for ctx.Err() == nil {
		n, err := r.publishBatch(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Error("outbox publish failed", "outbox", r.Name, "err", err)
			sleep(ctx, 500*time.Millisecond)
			continue
		}
		if time.Since(lastStats) > time.Second {
			r.refreshStats(ctx)
			lastStats = time.Now()
		}
		if time.Since(lastPrune) > time.Minute {
			if _, err := r.Pool.Exec(ctx, `DELETE FROM outbox WHERE published_at < now() - interval '1 hour'`); err != nil && ctx.Err() == nil {
				slog.Warn("outbox prune failed", "err", err)
			}
			lastPrune = time.Now()
		}
		if n == 0 {
			sleep(ctx, 20*time.Millisecond)
		}
	}
	return ctx.Err()
}

func (r *Relay) lockKey() int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("outbox:" + r.Name))
	return int64(h.Sum64())
}

// publishBatch publishes up to BatchSize rows in a single transaction and
// returns how many were published.
func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var leader bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, r.lockKey()).Scan(&leader); err != nil {
		return 0, err
	}
	if !leader {
		return 0, nil // another relay instance is publishing
	}
	rows, err := tx.Query(ctx, `
		SELECT id, topic, key, payload, headers FROM outbox
		WHERE published_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, r.BatchSize)
	if err != nil {
		return 0, err
	}
	var ids []int64
	var recs []kafka.Record
	for rows.Next() {
		var id int64
		var rec kafka.Record
		var key string
		var hdr []byte
		if err := rows.Scan(&id, &rec.Topic, &key, &rec.Value, &hdr); err != nil {
			rows.Close()
			return 0, err
		}
		rec.Key = []byte(key)
		if err := json.Unmarshal(hdr, &rec.Headers); err != nil {
			rows.Close()
			return 0, fmt.Errorf("outbox row %d headers: %w", id, err)
		}
		ids = append(ids, id)
		recs = append(recs, rec)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	if err := r.Producer.Produce(ctx, recs); err != nil {
		return 0, err
	}
	// A crash here (after Kafka acked, before commit) re-publishes the batch.
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, err
	}
	return len(ids), tx.Commit(ctx)
}

func (r *Relay) refreshStats(ctx context.Context) {
	var n int64
	var age float64
	err := r.Pool.QueryRow(ctx, `
		SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)
		FROM outbox WHERE published_at IS NULL`).Scan(&n, &age)
	if err == nil {
		r.backlog.Store(n)
		r.oldest.Store(int64(age))
	}
}

func (r *Relay) registerMetrics() {
	m := otel.Meter("tickless/outbox")
	g1, err1 := m.Int64ObservableGauge("outbox_backlog", metric.WithDescription("Unpublished outbox rows"))
	g2, err2 := m.Int64ObservableGauge("outbox_oldest_age_seconds", metric.WithDescription("Age of the oldest unpublished row"))
	if err1 != nil || err2 != nil {
		return
	}
	_, _ = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(g1, r.backlog.Load(), metric.WithAttributes(attrOutbox(r.Name)))
		o.ObserveInt64(g2, r.oldest.Load(), metric.WithAttributes(attrOutbox(r.Name)))
		return nil
	}, g1, g2)
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
