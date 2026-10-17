//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/tickless/pkg/kafka"
	"github.com/tickless/pkg/testkit"
)

type seen struct {
	mu   sync.Mutex
	vals map[string]bool
}

func (s *seen) add(v string) { s.mu.Lock(); s.vals[v] = true; s.mu.Unlock() }
func (s *seen) has(v string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.vals[v]
}

// gauge reads an int64 gauge value for the given partition from the reader.
func gauge(t *testing.T, r *sdkmetric.ManualReader, name string, partition string) (int64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, r.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				continue
			}
			for _, dp := range g.DataPoints {
				if v, ok := dp.Attributes.Value(attribute.Key("partition")); ok && v.AsString() == partition {
					return dp.Value, true
				}
			}
		}
	}
	return 0, false
}

// TestPermanentErrorStallsOnlyThatPartition: a poison record stops its own
// partition (nothing after it is handled or committed), the other partitions
// keep being consumed, consumer_partition_stalled is 1 for it, and
// kafka_consumer_lag - computed from committed offsets - keeps growing.
func TestPermanentErrorStallsOnlyThatPartition(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	brokers := testkit.Kafka(t, "t.poison") // 3 partitions
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	prod, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	defer prod.Close()
	send := func(partition int32, val string) {
		res := prod.ProduceSync(ctx, &kgo.Record{Topic: "t.poison", Partition: partition, Value: []byte(val)})
		require.NoError(t, res.FirstErr())
	}

	got := &seen{vals: map[string]bool{}}
	cctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- kafka.Consume(cctx, kafka.ConsumerConfig{Brokers: brokers, Group: "g-poison", Topics: []string{"t.poison"}, LagInterval: 300 * time.Millisecond},
			func(_ context.Context, recs []*kgo.Record) error {
				for _, r := range recs {
					if string(r.Value) == "poison" {
						return kafka.Permanent(errors.New("cannot settle"))
					}
					got.add(fmt.Sprintf("p%d:%s", r.Partition, r.Value))
				}
				return nil
			})
	}()

	send(0, "a0")
	send(0, "poison")
	for i := 0; i < 5; i++ {
		send(0, fmt.Sprintf("after%d", i))
	}
	send(1, "b0")
	send(2, "c0")

	require.Eventually(t, func() bool { return got.has("p0:a0") && got.has("p1:b0") && got.has("p2:c0") },
		30*time.Second, 100*time.Millisecond, "healthy partitions are consumed")
	require.Eventually(t, func() bool { v, ok := gauge(t, reader, "consumer_partition_stalled", "0"); return ok && v == 1 },
		30*time.Second, 200*time.Millisecond, "consumer_partition_stalled{partition=0} must be 1")

	// More traffic: healthy partitions still flow, the stalled one does not.
	send(1, "b1")
	send(0, "after-stall")
	require.Eventually(t, func() bool { return got.has("p1:b1") }, 20*time.Second, 100*time.Millisecond, "other partitions keep running")
	time.Sleep(2 * time.Second)
	assert.False(t, got.has("p0:after-stall"), "nothing after the poison record is handled")
	for i := 0; i < 5; i++ {
		assert.False(t, got.has(fmt.Sprintf("p0:after%d", i)))
	}
	v, _ := gauge(t, reader, "consumer_partition_stalled", "1")
	assert.Zero(t, v, "healthy partitions report 0")

	// Lag is derived from COMMITTED offsets, so it is non-zero and keeps growing while stalled.
	lagNow := func() int64 { v, _ := gauge(t, reader, "kafka_consumer_lag", "0"); return v }
	require.Eventually(t, func() bool { return lagNow() >= 7 }, 20*time.Second, 200*time.Millisecond, "lag counts the poison record and everything behind it")
	before := lagNow()
	for i := 0; i < 10; i++ {
		send(0, fmt.Sprintf("more%d", i))
	}
	require.Eventually(t, func() bool { return lagNow() >= before+10 }, 20*time.Second, 200*time.Millisecond, "lag keeps growing while the partition is stalled")

	stop()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(20 * time.Second):
		t.Fatal("consumer did not stop")
	}
}

func TestTransientErrorIsRetriedThenCommitted(t *testing.T) {
	brokers := testkit.Kafka(t, "t.transient")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	prod, err := kafka.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()
	require.NoError(t, prod.Produce(ctx, []kafka.Record{{Topic: "t.transient", Key: []byte("k"), Value: []byte("x")}}))

	var mu sync.Mutex
	calls := 0
	ok := make(chan struct{})
	cctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		_ = kafka.Consume(cctx, kafka.ConsumerConfig{Brokers: brokers, Group: "g-transient", Topics: []string{"t.transient"}},
			func(context.Context, []*kgo.Record) error {
				mu.Lock()
				defer mu.Unlock()
				calls++
				if calls < 3 {
					return errors.New("database is down")
				}
				select {
				case <-ok:
				default:
					close(ok)
				}
				return nil
			})
	}()
	select {
	case <-ok:
	case <-time.After(30 * time.Second):
		t.Fatal("transient errors must be retried until they succeed")
	}
}

func TestPermanentHelpers(t *testing.T) {
	base := errors.New("boom")
	assert.False(t, kafka.IsPermanent(base))
	assert.True(t, kafka.IsPermanent(kafka.Permanent(base)))
	assert.True(t, kafka.IsPermanent(fmt.Errorf("wrapped: %w", kafka.Permanent(base))))
	assert.ErrorIs(t, kafka.Permanent(base), base)
	assert.NoError(t, kafka.Permanent(nil))
}
