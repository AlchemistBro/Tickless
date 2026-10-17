//go:build integration

package app_test

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/matching/app"
	"github.com/tickless/matching/internal/partition"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/kafka"
	"github.com/tickless/pkg/testkit"
)

func genCommands(seed int64, n int) []*matchingv1.Command {
	r := rand.New(rand.NewSource(seed))
	markets := []string{"BTC-USDT", "ETH-USDT", "ETH-BTC"}
	var out []*matchingv1.Command
	var id uint64
	for i := 0; i < n; i++ {
		m := markets[r.Intn(len(markets))]
		if r.Intn(5) == 0 && id > 0 {
			out = append(out, &matchingv1.Command{Market: m, Type: matchingv1.CommandType_COMMAND_TYPE_CANCEL,
				OrderId: uint64(1 + r.Intn(int(id))), UserId: uint64(1 + r.Intn(4)), TimeUnixNano: time.Now().UnixNano()})
			continue
		}
		id++
		side := commonv1.Side_SIDE_BUY
		if r.Intn(2) == 0 {
			side = commonv1.Side_SIDE_SELL
		}
		out = append(out, &matchingv1.Command{Market: m, Type: matchingv1.CommandType_COMMAND_TYPE_PLACE, OrderId: id,
			UserId: uint64(1 + r.Intn(4)), Side: side, OrderType: matchingv1.OrderType_ORDER_TYPE_LIMIT,
			Tif: matchingv1.TimeInForce(1 + r.Intn(3)), Price: int64(95 + r.Intn(11)), Qty: int64(1 + r.Intn(9)),
			TimeUnixNano: time.Now().UnixNano()})
	}
	return out
}

func produce(t *testing.T, ctx context.Context, p *kafka.Producer, cmds []*matchingv1.Command) {
	t.Helper()
	var recs []kafka.Record
	for _, c := range cmds {
		b, err := proto.Marshal(c)
		require.NoError(t, err)
		recs = append(recs, kafka.Record{Topic: app.TopicCommands, Key: []byte(c.Market), Value: b})
	}
	require.NoError(t, p.Produce(ctx, recs))
}

// readEvents returns every record currently on matching.events once at least
// `min` have been seen and the topic has been quiet for a moment.
func readEvents(t *testing.T, brokers []string, min int) []*matchingv1.Event {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(app.TopicEvents),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	var out []*matchingv1.Event
	deadline := time.Now().Add(90 * time.Second)
	quietSince := time.Now()
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		n := len(out)
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			var e matchingv1.Event
			require.NoError(t, proto.Unmarshal(r.Value, &e))
			out = append(out, &e)
		})
		cancel()
		if len(out) != n {
			quietSince = time.Now()
		}
		if len(out) >= min && time.Since(quietSince) > 2*time.Second {
			break
		}
	}
	return out
}

func startApp(t *testing.T, brokers []string, dir string) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx, app.Config{KafkaBrokers: brokers[0], SnapshotDir: dir, SnapshotEvery: 40, BatchSize: 25, AdminAddr: "127.0.0.1:0"})
	}()
	return func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Fatal("matching did not stop")
		}
	}
}

// TestRestartMidStreamLosesNothingAndReplaysIdentically is the "kill matching
// mid-stream" scenario: the engine is stopped, its newest checkpoint is
// discarded to simulate a crash between checkpoints, and a new instance must
// (a) lose no command, (b) re-emit identical events for the replayed range and
// (c) produce a gap-free per-market sequence equal to an uninterrupted run.
func TestRestartMidStreamLosesNothingAndReplaysIdentically(t *testing.T) {
	brokers := testkit.Kafka(t, app.TopicCommands, app.TopicEvents)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	prod, err := kafka.NewProducer(brokers)
	require.NoError(t, err)
	defer prod.Close()
	dir := t.TempDir()

	cmds := genCommands(42, 800)
	ref := partition.New() // uninterrupted reference (one partition object is fine: markets are independent)
	var want []partition.Result
	for _, c := range cmds {
		want = ref.Apply(c, want)
	}

	// Phase 1: first half, then stop.
	produce(t, ctx, prod, cmds[:400])
	stop := startApp(t, brokers, dir)
	var half []partition.Result
	h := partition.New()
	for _, c := range cmds[:400] {
		half = h.Apply(c, half)
	}
	got := readEvents(t, brokers, len(half))
	require.GreaterOrEqual(t, len(got), len(half), "first half fully processed")
	stop()

	// Simulate a crash: drop the newest checkpoint of every partition so the
	// restart must replay commands whose events were already published.
	dropped := 0
	dirs, _ := filepath.Glob(filepath.Join(dir, "p*"))
	for _, d := range dirs {
		files, _ := filepath.Glob(filepath.Join(d, "*.ckpt"))
		sort.Strings(files)
		if len(files) > 1 {
			require.NoError(t, os.Remove(files[len(files)-1]))
			dropped++
		}
	}
	t.Logf("discarded the newest checkpoint of %d partitions", dropped)

	// Phase 2: second half, new instance.
	produce(t, ctx, prod, cmds[400:])
	stop = startApp(t, brokers, dir)
	defer stop()
	got = readEvents(t, brokers, len(want))

	// Group by market, check duplicates are identical and sequences gap-free.
	bySeq := map[string]map[uint64]*matchingv1.Event{}
	dups := 0
	for _, e := range got {
		m := bySeq[e.Market]
		if m == nil {
			m = map[uint64]*matchingv1.Event{}
			bySeq[e.Market] = m
		}
		if prev, ok := m[e.Seq]; ok {
			dups++
			require.True(t, proto.Equal(prev, e), "replayed event %s:%d differs from the original", e.Market, e.Seq)
			continue
		}
		m[e.Seq] = e
	}
	t.Logf("%d events read, %d re-emitted duplicates, all identical", len(got), dups)
	if dropped > 0 {
		assert.Positive(t, dups, "the replay must have re-emitted events")
	}
	wantByMarket := map[string][]partition.Result{}
	for _, r := range want {
		wantByMarket[r.Market] = append(wantByMarket[r.Market], r)
	}
	for market, exp := range wantByMarket {
		require.Len(t, bySeq[market], len(exp), "%s: every event present exactly once after dedupe", market)
		for i, r := range exp {
			ev := bySeq[market][uint64(i+1)]
			require.NotNil(t, ev, "%s seq %d missing (gap)", market, i+1)
			assert.True(t, proto.Equal(partition.ToProto(market, r.Event), ev), "%s seq %d differs from the uninterrupted run", market, i+1)
		}
	}
}
