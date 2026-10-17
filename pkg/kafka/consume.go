package kafka

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// permanentError marks a handler failure that retrying cannot fix.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err to tell Consume that retrying will never succeed (a
// poison record). Consume then stops consuming that partition only, keeps the
// others running, raises consumer_partition_stalled and never commits past the
// record, so the record is neither skipped nor lost. Any other error is
// transient and retried with backoff.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or an error it wraps) is Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// Handler processes a batch of records of ONE partition, in offset order.
// Returning a plain error makes the consumer retry the same batch with backoff
// (at-least-once, so handlers MUST be idempotent). Returning Permanent(err)
// stalls the partition (see Permanent).
type Handler func(ctx context.Context, recs []*kgo.Record) error

// ConsumerConfig configures Consume.
type ConsumerConfig struct {
	Brokers  []string
	Group    string
	Topics   []string
	MaxBatch int
	// LagInterval is how often committed-offset lag is computed (default 5 s).
	LagInterval time.Duration
	// OnRevoked, if set, is called when partitions are taken away from this
	// member (rebalance or loss), so handlers can drop partition-local caches.
	OnRevoked func()
}

type tp struct {
	topic string
	part  int32
}

// Consume runs a consumer-group loop until ctx is cancelled. Records are
// handled per partition and offsets are committed only after the handler
// succeeded for that partition's batch. New groups start at the earliest
// offset so no event is lost.
//
// Metrics: kafka_consumer_lag{group,topic,partition} is computed from the
// group's COMMITTED offsets against the log end (kadm), so it keeps growing
// while a partition is stalled; consumer_partition_stalled{...} is 1 for a
// partition stopped by a permanent error.
func Consume(ctx context.Context, cfg ConsumerConfig, h Handler) error {
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = 500
	}
	if cfg.LagInterval == 0 {
		cfg.LagInterval = 5 * time.Second
	}
	st := newStats(cfg.Group)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		// A crashed member is evicted after 10 s (default 45 s) so failover is quick.
		kgo.SessionTimeout(10*time.Second),
		kgo.HeartbeatInterval(3*time.Second),
		// A partition we no longer own is no longer stalled by us.
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			st.clearStalled(m)
			if cfg.OnRevoked != nil {
				cfg.OnRevoked()
			}
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			st.clearStalled(m)
			if cfg.OnRevoked != nil {
				cfg.OnRevoked()
			}
		}),
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	// With BlockRebalanceOnPoll, Close (which leaves the group) blocks until
	// rebalances are allowed again; always allow on the way out.
	defer cl.AllowRebalance()

	go st.pollLag(ctx, kadm.NewClient(cl), cfg.LagInterval)

	for ctx.Err() == nil {
		fetches := cl.PollRecords(ctx, cfg.MaxBatch)
		if ctx.Err() != nil {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				slog.Error("kafka fetch error", "topic", t, "partition", p, "err", err)
			}
		})
		var commit []*kgo.Record
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 || ctx.Err() != nil {
				return
			}
			err := handleWithRetry(ctx, cfg.Group, h, p.Records)
			switch {
			case err == nil:
				commit = append(commit, p.Records...)
			case IsPermanent(err):
				first, last := p.Records[0], p.Records[len(p.Records)-1]
				slog.Error("PARTITION STALLED by a permanent handler error; other partitions keep running; offsets not committed",
					"group", cfg.Group, "topic", p.Topic, "partition", p.Partition,
					"first_offset", first.Offset, "last_offset", last.Offset, "err", err)
				st.setStalled(tp{p.Topic, p.Partition})
				cl.PauseFetchPartitions(map[string][]int32{p.Topic: {p.Partition}})
			}
		})
		if ctx.Err() != nil {
			break
		}
		if len(commit) > 0 {
			if err := cl.CommitRecords(ctx, commit...); err != nil && ctx.Err() == nil {
				slog.Error("kafka commit failed", "group", cfg.Group, "err", err)
			}
		}
		cl.AllowRebalance()
	}
	return ctx.Err()
}

// handleWithRetry retries transient errors with exponential backoff until the
// handler succeeds, fails permanently, or ctx ends (returned as ctx.Err()).
func handleWithRetry(ctx context.Context, group string, h Handler, recs []*kgo.Record) error {
	backoff := 100 * time.Millisecond
	for {
		err := h(ctx, recs)
		if err == nil || IsPermanent(err) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Error("kafka handler failed, retrying batch", "group", group, "err", err,
			"partition", recs[0].Partition, "first_offset", recs[0].Offset, "records", len(recs))
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// stats holds the consumer's observable state for metrics.
type stats struct {
	group   string
	mu      sync.Mutex
	lag     map[tp]int64
	stalled map[tp]bool
}

func newStats(group string) *stats {
	s := &stats{group: group, lag: map[tp]int64{}, stalled: map[tp]bool{}}
	m := otel.Meter("tickless/kafka")
	lagG, err1 := m.Int64ObservableGauge("kafka_consumer_lag",
		metric.WithDescription("Messages between the group's committed offset and the log end, per partition"))
	stallG, err2 := m.Int64ObservableGauge("consumer_partition_stalled",
		metric.WithDescription("1 while a partition is stopped by a permanent handler error"))
	if err1 != nil || err2 != nil {
		return s
	}
	_, _ = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		for k, v := range s.lag {
			o.ObserveInt64(lagG, v, s.attrs(k))
		}
		for k, on := range s.stalled {
			var v int64
			if on {
				v = 1
			}
			o.ObserveInt64(stallG, v, s.attrs(k))
		}
		return nil
	}, lagG, stallG)
	return s
}

func (s *stats) attrs(k tp) metric.MeasurementOption {
	return metric.WithAttributes(attrString("group", s.group), attrString("topic", k.topic),
		attrString("partition", strconv.Itoa(int(k.part))))
}

func (s *stats) setStalled(k tp) {
	s.mu.Lock()
	s.stalled[k] = true
	s.mu.Unlock()
}

func (s *stats) clearStalled(m map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for t, ps := range m {
		for _, p := range ps {
			if _, ok := s.stalled[tp{t, p}]; ok {
				s.stalled[tp{t, p}] = false // keep the series, now at 0
			}
		}
	}
}

func (s *stats) pollLag(ctx context.Context, adm *kadm.Client, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		s.refreshLag(ctx, adm)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *stats) refreshLag(ctx context.Context, adm *kadm.Client) {
	lags, err := adm.Lag(ctx, s.group)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("cannot compute consumer lag", "group", s.group, "err", err)
		}
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, dl := range lags {
		for topic, parts := range dl.Lag {
			for part, l := range parts {
				if l.Err == nil {
					s.lag[tp{topic, part}] = max(l.Lag, 0)
				}
			}
		}
	}
}
