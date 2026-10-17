// Package kafka wraps franz-go with what every Tickless service needs:
// a synchronous batch producer, a group consumer with at-least-once handling,
// W3C trace-context propagation in record headers and consumer-lag metrics.
package kafka

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
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
