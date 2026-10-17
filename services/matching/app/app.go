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

	// Restore every partition from its newest valid checkpoint, or refuse to start.
	workers, offsets, err := prepare(ctx, brokers, cfg, parts, producer)
	if err != nil {
		return err
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
	nparts   int // partition count of matching.commands, stored in every checkpoint
	in       chan []*kgo.Record
	open     atomic.Int64 // resting orders across the partition's markets (for the gauge)
	cmds     metric.Int64Counter
	evs      metric.Int64Counter
	latency  metric.Float64Histogram
	ckptDur  metric.Float64Histogram
}

func newWorker(id int32, cfg Config, prod *kafka.Producer, topicPartitions int) (*worker, int64, error) {
	dir := filepath.Join(cfg.SnapshotDir, fmt.Sprintf("p%d", id))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, 0, err
	}
	p, next, err := loadCheckpoint(dir, topicPartitions)
	if err != nil {
		return nil, 0, fmt.Errorf("partition %d: %w", id, err)
	}
	w := &worker{id: id, cfg: cfg, dir: dir, p: p, next: next, lastCkpt: next, prod: prod, nparts: topicPartitions, in: make(chan []*kgo.Record, 8)}
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
	data := w.p.Checkpoint(w.next, w.nparts)
	name := filepath.Join(w.dir, ckptName(w.next))
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

// ErrRefuseToStart wraps every recovery failure that must stop the service
// instead of letting it diverge from the true book (ADR 0003).
var ErrRefuseToStart = errors.New("matching refuses to start")

// loadCheckpoint restores the newest valid checkpoint in dir. A torn newest
// file falls back to an older valid one. It REFUSES (rather than starting from
// an empty book) when checkpoint files exist but none restores, or when the
// topic partition count differs from the one recorded in the checkpoint. An
// empty directory is a fresh start; verifyLogStart then makes sure the log
// really begins at offset 0.
func loadCheckpoint(dir string, topicPartitions int) (*partition.Partition, int64, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.ckpt"))
	if err != nil {
		return nil, 0, err
	}
	if len(files) == 0 {
		return partition.New(), 0, nil
	}
	sort.Sort(sort.Reverse(sort.StringSlice(files)))
	var lastErr error
	for _, f := range files {
		data, err := os.ReadFile(f) //nolint:gosec // f comes from globbing the operator-configured snapshot directory
		if err != nil {
			lastErr = err
			continue
		}
		p, off, parts, err := partition.Restore(data)
		if err != nil {
			slog.Warn("ignoring corrupt checkpoint", "file", f, "err", err)
			lastErr = err
			continue
		}
		if parts != topicPartitions {
			return nil, 0, fmt.Errorf("%w: %s was written for %d partitions of %s but the topic now has %d; markets would map to different partitions",
				ErrRefuseToStart, filepath.Base(f), parts, TopicCommands, topicPartitions)
		}
		return p, off, nil
	}
	return nil, 0, fmt.Errorf("%w: %d checkpoint files in %s but none can be restored (last error: %w); starting from an empty book would diverge from the log",
		ErrRefuseToStart, len(files), dir, lastErr)
}

// prepare restores all partitions and verifies the command log still covers
// every resume offset.
func prepare(ctx context.Context, brokers []string, cfg Config, parts []int32, prod *kafka.Producer) (map[int32]*worker, map[int32]kgo.Offset, error) {
	workers := make(map[int32]*worker, len(parts))
	offsets := map[int32]kgo.Offset{}
	for _, pid := range parts {
		w, next, err := newWorker(pid, cfg, prod, len(parts))
		if err != nil {
			return nil, nil, err
		}
		workers[pid] = w
		offsets[pid] = kgo.NewOffset().At(next)
		slog.Info("partition ready", "partition", pid, "resume_offset", next, "markets", w.p.Markets())
	}
	if err := verifyLogStart(ctx, brokers, workers); err != nil {
		return nil, nil, err
	}
	return workers, offsets, nil
}

// verifyLogStart refuses to start when a partition would resume before the
// first offset still present in the log: those commands were deleted, so the
// book cannot be rebuilt and the engine would restart from a book that does not
// match history (and from seq 1, which consumers would silently dedupe away).
func verifyLogStart(ctx context.Context, brokers []string, workers map[int32]*worker) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	starts, err := adm.ListStartOffsets(ctx, TopicCommands)
	if err != nil {
		return fmt.Errorf("cannot read log start offsets: %w", err)
	}
	for pid, w := range workers {
		lo, ok := starts.Lookup(TopicCommands, pid)
		if !ok || lo.Err != nil {
			return fmt.Errorf("cannot read log start offset of %s[%d]: %w", TopicCommands, pid, lo.Err)
		}
		if w.next < lo.Offset {
			return fmt.Errorf("%w: partition %d would resume at offset %d but the log now starts at %d; the commands in between were deleted (retention or a lost checkpoint), so the book cannot be rebuilt. Restore the snapshot directory or recreate the topic and its snapshots together",
				ErrRefuseToStart, pid, w.next, lo.Offset)
		}
	}
	return nil
}

func pruneCheckpoints(dir string, keep int) {
	files, _ := filepath.Glob(filepath.Join(dir, "*.ckpt"))
	sort.Sort(sort.Reverse(sort.StringSlice(files)))
	for i := keep; i < len(files); i++ {
		_ = os.Remove(files[i])
	}
}

// ckptName is the file name of the checkpoint at a log offset (sorts by offset).
func ckptName(offset int64) string { return fmt.Sprintf("%020d.ckpt", offset) }
