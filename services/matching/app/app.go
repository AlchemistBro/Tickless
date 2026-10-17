// Package app wires the matching service: it consumes matching.commands,
// drives the engines and publishes matching.events.
//
// There is no database on the hot path and no consumer group: the position in
// the command log is stored in checkpoints next to the engine state, so
// recovery is "restore the newest checkpoint, replay the log from its offset"
// (ADR 0003). Events re-emitted by a replay are identical (same market, same
// sequence number) and are deduplicated by their consumers.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/matching/internal/partition"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/kafka"
	"github.com/tickless/pkg/otelx"
	"github.com/tickless/pkg/svc"
)

// Topics.
const (
	TopicCommands = "matching.commands"
	TopicEvents   = "matching.events"
)

// Config is the matching configuration.
type Config struct {
	AdminAddr     string
	KafkaBrokers  string
	SnapshotDir   string
	SnapshotEvery int // commands per partition between checkpoints
	BatchSize     int
}

// ConfigFromEnv reads the configuration from environment variables.
func ConfigFromEnv() Config {
	return Config{
		AdminAddr:     svc.Env("ADMIN_ADDR", ":9204"),
		KafkaBrokers:  svc.Env("KAFKA_BROKERS", "localhost:9092"),
		SnapshotDir:   svc.Env("SNAPSHOT_DIR", "./snapshots"),
		SnapshotEvery: svc.EnvInt("SNAPSHOT_EVERY", 10000),
		BatchSize:     svc.EnvInt("BATCH_SIZE", 256),
	}
}

var (
	tracer = otel.Tracer("tickless/matching")
	meter  = otel.Meter("tickless/matching")
)

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	shutdown, err := otelx.Setup(ctx, "matching")
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(context.Background()) }()
	if cfg.SnapshotEvery <= 0 {
		cfg.SnapshotEvery = 10000
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	brokers := kafka.Brokers(cfg.KafkaBrokers)

	parts, err := waitPartitions(ctx, brokers)
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	// Restore every partition from its newest valid checkpoint.
	workers := make(map[int32]*worker, len(parts))
	offsets := map[int32]kgo.Offset{}
	for _, pid := range parts {
		w, next, err := newWorker(pid, cfg, producer)
		if err != nil {
			return err
		}
		workers[pid] = w
		offsets[pid] = kgo.NewOffset().At(next)
		slog.Info("partition ready", "partition", pid, "resume_offset", next, "markets", w.p.Markets())
	}
	registerGauges(workers)

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{TopicCommands: offsets}),
		kgo.FetchMaxWait(50*time.Millisecond))
	if err != nil {
		return err
	}
	defer cl.Close()

	var ready atomic.Bool
	ready.Store(true)
	svc.ServeAdmin(ctx, cfg.AdminAddr, func(context.Context) error {
		if !ready.Load() {
			return errors.New("not ready")
		}
		return nil
	})

	g, gctx := errgroup.WithContext(ctx)
	for _, w := range workers {
		g.Go(func() error { return w.run(gctx) })
	}
	g.Go(func() error {
		defer func() {
			for _, w := range workers {
				close(w.in)
			}
		}()
		for gctx.Err() == nil {
			fetches := cl.PollRecords(gctx, cfg.BatchSize*4)
			if gctx.Err() != nil {
				return nil
			}
			fetches.EachError(func(t string, p int32, err error) {
				slog.Error("fetch error", "topic", t, "partition", p, "err", err)
			})
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				if len(p.Records) == 0 {
					return
				}
				w := workers[p.Partition]
				for i := 0; i < len(p.Records); i += cfg.BatchSize {
					select {
					case w.in <- p.Records[i:min(i+cfg.BatchSize, len(p.Records))]: // bounded: back-pressure
					case <-gctx.Done():
						return
					}
				}
			})
		}
		return nil
	})
	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// waitPartitions returns the partition ids of the command topic, waiting for
// it to exist (topics are created by the stack's init job).
func waitPartitions(ctx context.Context, brokers []string) ([]int32, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	for {
		td, err := adm.ListTopics(ctx, TopicCommands)
		if err == nil {
			if t, ok := td[TopicCommands]; ok && t.Err == nil && len(t.Partitions) > 0 {
				ids := t.Partitions.Numbers()
				sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
				return ids, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
			slog.Info("waiting for topic", "topic", TopicCommands)
		}
	}
}

// worker owns one partition: it is the only goroutine that touches the engines.
type worker struct {
	id       int32
	cfg      Config
	dir      string
	p        *partition.Partition
	next     int64 // offset of the next command to apply
	lastCkpt int64
	prod     *kafka.Producer
	in       chan []*kgo.Record
	open     atomic.Int64 // resting orders across the partition's markets (for the gauge)
	cmds     metric.Int64Counter
	evs      metric.Int64Counter
	latency  metric.Float64Histogram
	ckptDur  metric.Float64Histogram
}

func newWorker(id int32, cfg Config, prod *kafka.Producer) (*worker, int64, error) {
	dir := filepath.Join(cfg.SnapshotDir, fmt.Sprintf("p%d", id))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, 0, err
	}
	p, next, err := loadCheckpoint(dir)
	if err != nil {
		return nil, 0, err
	}
	w := &worker{id: id, cfg: cfg, dir: dir, p: p, next: next, lastCkpt: next, prod: prod, in: make(chan []*kgo.Record, 8)}
	w.cmds, _ = meter.Int64Counter("matching_commands_total", metric.WithDescription("Commands applied by the engine"))
	w.evs, _ = meter.Int64Counter("matching_events_total", metric.WithDescription("Events emitted by the engine"))
	w.latency, _ = meter.Float64Histogram("matching_command_latency_seconds",
		metric.WithDescription("Time from the order command timestamp to its events being published"),
		metric.WithExplicitBucketBoundaries(.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5))
	w.ckptDur, _ = meter.Float64Histogram("matching_checkpoint_seconds", metric.WithDescription("Checkpoint write duration"))
	w.refreshGauge()
	return w, next, nil
}

func (w *worker) refreshGauge() {
	var n int64
	for _, m := range w.p.Markets() {
		n += int64(w.p.Engine(m).OpenOrders())
	}
	w.open.Store(n)
}

func registerGauges(ws map[int32]*worker) {
	g, err := meter.Int64ObservableGauge("matching_open_orders", metric.WithDescription("Resting orders per partition"))
	if err != nil {
		return
	}
	_, _ = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for id, w := range ws {
			o.ObserveInt64(g, w.open.Load(), metric.WithAttributes(attribute.Int("partition", int(id))))
		}
		return nil
	}, g)
}

func (w *worker) run(ctx context.Context) error {
	attrs := metric.WithAttributes(attribute.Int("partition", int(w.id)))
	var results []partition.Result
	for recs := range w.in {
		var out []kafka.Record
		for _, r := range recs {
			if r.Offset < w.next {
				continue // already applied before the checkpoint we resumed from
			}
			w.next = r.Offset + 1
			var cmd matchingv1.Command
			if err := proto.Unmarshal(r.Value, &cmd); err != nil {
				slog.Error("dropping undecodable command", "partition", w.id, "offset", r.Offset, "err", err)
				continue
			}
			cctx, span := tracer.Start(kafka.ExtractContext(ctx, r), "matching.process")
			results = w.p.Apply(&cmd, results[:0])
			hdr := kafka.TraceHeaders(cctx)
			for _, res := range results {
				b, err := proto.Marshal(partition.ToProto(res.Market, res.Event))
				if err != nil {
					span.End()
					return err
				}
				out = append(out, kafka.Record{Topic: TopicEvents, Key: []byte(res.Market), Value: b, Headers: hdr})
			}
			span.End()
			w.cmds.Add(ctx, 1, attrs)
			w.evs.Add(ctx, int64(len(results)), attrs)
		}
		if err := w.publish(ctx, out); err != nil {
			return err
		}
		now := time.Now().UnixNano()
		for _, r := range recs {
			var c matchingv1.Command
			if proto.Unmarshal(r.Value, &c) == nil && c.TimeUnixNano > 0 {
				w.latency.Record(ctx, float64(now-c.TimeUnixNano)/1e9, attrs)
			}
		}
		w.refreshGauge()
		if w.next-w.lastCkpt >= int64(w.cfg.SnapshotEvery) {
			if err := w.checkpoint(ctx); err != nil {
				slog.Error("checkpoint failed", "partition", w.id, "err", err)
			}
		}
	}
	// Input closed (shutdown): persist the final state.
	if w.next != w.lastCkpt {
		if err := w.checkpoint(context.Background()); err != nil {
			slog.Error("final checkpoint failed", "partition", w.id, "err", err)
		}
	}
	return nil
}

// publish sends events and does not return until Kafka acknowledged them:
// events must never be lost or reordered, so it retries until ctx ends.
func (w *worker) publish(ctx context.Context, recs []kafka.Record) error {
	backoff := 100 * time.Millisecond
	for len(recs) > 0 {
		err := w.prod.Produce(ctx, recs)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Error("publishing events failed, retrying", "partition", w.id, "err", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = min(backoff*2, 5*time.Second)
	}
	return nil
}

func (w *worker) checkpoint(ctx context.Context) error {
	start := time.Now()
	data := w.p.Checkpoint(w.next)
	name := filepath.Join(w.dir, fmt.Sprintf("%020d.ckpt", w.next))
	tmp := name + ".tmp"
	if err := writeFileSync(tmp, data); err != nil {
		return err
	}
	if err := os.Rename(tmp, name); err != nil {
		return err
	}
	w.lastCkpt = w.next
	pruneCheckpoints(w.dir, 3)
	w.ckptDur.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attribute.Int("partition", int(w.id))))
	return nil
}

func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // path is inside the operator-configured snapshot directory
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// loadCheckpoint restores the newest valid checkpoint in dir, falling back to
// older ones (a crash can leave a torn newest file) and finally to an empty
// partition replaying the whole log.
func loadCheckpoint(dir string) (*partition.Partition, int64, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.ckpt"))
	if err != nil {
		return nil, 0, err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(files)))
	for _, f := range files {
		data, err := os.ReadFile(f) //nolint:gosec // f comes from globbing the operator-configured snapshot directory
		if err != nil {
			continue
		}
		p, off, err := partition.Restore(data)
		if err != nil {
			slog.Warn("ignoring corrupt checkpoint", "file", f, "err", err)
			continue
		}
		return p, off, nil
	}
	return partition.New(), 0, nil
}

func pruneCheckpoints(dir string, keep int) {
	files, _ := filepath.Glob(filepath.Join(dir, "*.ckpt"))
	sort.Sort(sort.Reverse(sort.StringSlice(files)))
	for i := keep; i < len(files); i++ {
		_ = os.Remove(files[i])
	}
}
