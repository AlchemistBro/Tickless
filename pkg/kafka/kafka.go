// Package kafka wraps franz-go with what every Tickless service needs:
// a synchronous batch producer, a group consumer with at-least-once handling,
// W3C trace-context propagation in record headers and consumer-lag metrics.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
)

// Brokers parses a comma separated broker list (e.g. KAFKA_BROKERS).
func Brokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// Record is a message to publish.
type Record struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// Producer is a synchronous, idempotent, acks=all producer.
type Producer struct {
	cl *kgo.Client
	// duplicate publishes every record twice. It exists only for the
	// "duplicate every Kafka message" chaos scenario (CHAOS_DUPLICATE_PUBLISH=true)
	// and proves that every consumer is idempotent.
	duplicate bool
}

// NewProducer connects a producer.
func NewProducer(brokers []string, opts ...kgo.Opt) (*Producer, error) {
	all := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(time.Millisecond),
		kgo.RecordRetries(10),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	}, opts...)
	cl, err := kgo.NewClient(all...)
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl, duplicate: os.Getenv("CHAOS_DUPLICATE_PUBLISH") == "true"}, nil
}

// Produce publishes records and returns once all are acknowledged.
func (p *Producer) Produce(ctx context.Context, recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	rs := make([]*kgo.Record, 0, len(recs)*2)
	for _, r := range recs {
		kr := toKgo(r)
		rs = append(rs, kr)
		if p.duplicate {
			rs = append(rs, toKgo(r))
		}
	}
	var errs []error
	for _, res := range p.cl.ProduceSync(ctx, rs...) {
		if res.Err != nil {
			errs = append(errs, res.Err)
		}
	}
	return errors.Join(errs...)
}

func toKgo(r Record) *kgo.Record {
	kr := &kgo.Record{Topic: r.Topic, Key: r.Key, Value: r.Value}
	for k, v := range r.Headers {
		kr.Headers = append(kr.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return kr
}

// Client exposes the underlying client (admin/tests).
func (p *Producer) Client() *kgo.Client { return p.cl }

// Close flushes and closes.
func (p *Producer) Close() { p.cl.Close() }

// headerCarrier adapts record headers to an OpenTelemetry TextMapCarrier.
type headerCarrier struct{ h *[]kgo.RecordHeader }

func (c headerCarrier) Get(key string) string {
	for _, h := range *c.h {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, val string) {
	for i, h := range *c.h {
		if h.Key == key {
			(*c.h)[i].Value = []byte(val)
			return
		}
	}
	*c.h = append(*c.h, kgo.RecordHeader{Key: key, Value: []byte(val)})
}

func (c headerCarrier) Keys() []string {
	ks := make([]string, len(*c.h))
	for i, h := range *c.h {
		ks[i] = h.Key
	}
	return ks
}

// TraceHeaders returns the W3C trace headers of ctx for publishing.
func TraceHeaders(ctx context.Context) map[string]string {
	m := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, m)
	return m
}

// ExtractContext continues the trace carried in a record's headers.
func ExtractContext(ctx context.Context, r *kgo.Record) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier{&r.Headers})
}

// Header returns a record header value.
func Header(r *kgo.Record, key string) string { return headerCarrier{&r.Headers}.Get(key) }

// Handler processes a batch of records in partition order. Returning an error
// makes the consumer retry the same batch (at-least-once), so handlers MUST be
// idempotent.
type Handler func(ctx context.Context, recs []*kgo.Record) error

// ConsumerConfig configures Consume.
type ConsumerConfig struct {
	Brokers  []string
	Group    string
	Topics   []string
	MaxBatch int
}

// Consume runs a consumer-group loop until ctx is cancelled. Offsets are
// committed only after the handler succeeds. New groups start at the earliest
// offset so no event is lost. Consumer lag is exported as a metric.
func Consume(ctx context.Context, cfg ConsumerConfig, h Handler) error {
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = 500
	}
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
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	// With BlockRebalanceOnPoll, Close (which leaves the group) blocks until
	// rebalances are allowed again; always allow on the way out.
	defer cl.AllowRebalance()
	lag := newLagTracker(cfg.Group)

	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		fetches := cl.PollRecords(ctx, cfg.MaxBatch)
		if ctx.Err() != nil {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			slog.Error("kafka fetch error", "topic", t, "partition", p, "err", err)
		})
		lag.observe(fetches)
		var recs []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
		if len(recs) == 0 {
			cl.AllowRebalance()
			continue
		}
		for ctx.Err() == nil {
			if err := h(ctx, recs); err != nil {
				slog.Error("kafka handler failed, retrying batch", "group", cfg.Group, "err", err, "records", len(recs))
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
				}
				backoff = min(backoff*2, 5*time.Second)
				continue
			}
			backoff = 100 * time.Millisecond
			break
		}
		if ctx.Err() != nil {
			break
		}
		if err := cl.CommitRecords(ctx, recs...); err != nil && ctx.Err() == nil {
			slog.Error("kafka commit failed", "group", cfg.Group, "err", err)
		}
		cl.AllowRebalance()
	}
	return ctx.Err()
}

// lagTracker records the number of unconsumed messages per partition
// (high watermark minus last fetched offset) as the kafka_consumer_lag gauge.
type lagTracker struct {
	mu    sync.Mutex
	group string
	lag   map[string]int64 // "topic/partition" -> lag
}

func newLagTracker(group string) *lagTracker {
	t := &lagTracker{group: group, lag: map[string]int64{}}
	m := otel.Meter("tickless/kafka")
	g, err := m.Int64ObservableGauge("kafka_consumer_lag", metric.WithDescription("Messages behind the log end per partition"))
	if err == nil {
		_, _ = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			t.mu.Lock()
			defer t.mu.Unlock()
			for k, v := range t.lag {
				topic, part, _ := strings.Cut(k, "/")
				o.ObserveInt64(g, v, metric.WithAttributes(
					attrString("group", group), attrString("topic", topic), attrString("partition", part)))
			}
			return nil
		}, g)
	}
	return t
}

func (t *lagTracker) observe(f kgo.Fetches) {
	t.mu.Lock()
	defer t.mu.Unlock()
	f.EachPartition(func(p kgo.FetchTopicPartition) {
		if n := len(p.Records); n > 0 {
			last := p.Records[n-1].Offset
			t.lag[fmt.Sprintf("%s/%d", p.Topic, p.Partition)] = max(p.HighWatermark-last-1, 0)
		}
	})
}
