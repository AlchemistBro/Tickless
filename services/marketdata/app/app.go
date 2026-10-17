// Package app wires the marketdata service.
//
// It consumes matching.events WITHOUT a consumer group: every instance needs
// every event to build its own in-memory books, and resumes from the offsets
// stored together with the market snapshots in Redis. candles.live is consumed
// from its end (live updates only).
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/marketdata/internal/persist"
	"github.com/tickless/marketdata/internal/server"
	"github.com/tickless/marketdata/internal/state"
	candlesv1 "github.com/tickless/pkg/gen/tickless/candles/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	ordersv1 "github.com/tickless/pkg/gen/tickless/orders/v1"
	"github.com/tickless/pkg/kafka"
	"github.com/tickless/pkg/market"
	"github.com/tickless/pkg/otelx"
	"github.com/tickless/pkg/svc"
)

const (
	topicEvents = "matching.events"
	topicLive   = "candles.live"
)

// Config is the marketdata configuration.
type Config struct {
	HTTPAddr, AdminAddr string
	RedisAddr           string
	KafkaBrokers        string
	OrdersAddr          string
	CandlesAddr         string
	WSOrigins           []string
	SnapshotEvery       time.Duration
}

// ConfigFromEnv reads the configuration from environment variables.
func ConfigFromEnv() Config {
	var origins []string
	for _, o := range strings.Split(svc.Env("CORS_ALLOWED_ORIGINS", ""), ",") {
		if o = strings.TrimSpace(o); o != "" {
			origins = append(origins, strings.TrimPrefix(strings.TrimPrefix(o, "http://"), "https://"))
		}
	}
	return Config{
		HTTPAddr: svc.Env("HTTP_ADDR", ":8081"), AdminAddr: svc.Env("ADMIN_ADDR", ":9207"),
		RedisAddr: svc.Env("REDIS_ADDR", "localhost:6379"), KafkaBrokers: svc.Env("KAFKA_BROKERS", "localhost:9092"),
		OrdersAddr: svc.Env("ORDERS_ADDR", "localhost:9102"), CandlesAddr: svc.Env("CANDLES_ADDR", "localhost:9106"),
		WSOrigins: origins, SnapshotEvery: time.Duration(svc.EnvInt("SNAPSHOT_EVERY_MS", 1000)) * time.Millisecond,
	}
}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	shutdown, err := otelx.Setup(ctx, "marketdata")
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(context.Background()) }()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()

	ordersConn, err := svc.Dial(cfg.OrdersAddr)
	if err != nil {
		return err
	}
	defer func() { _ = ordersConn.Close() }()
	candlesConn, err := svc.Dial(cfg.CandlesAddr)
	if err != nil {
		return err
	}
	defer func() { _ = candlesConn.Close() }()

	specs := market.NewCache(market.FetchFromOrders(ordersv1.NewOrdersServiceClient(ordersConn)))
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { specs.Run(gctx, 30*time.Second); return nil })
	select {
	case <-specs.Ready():
	case <-ctx.Done():
		return ctx.Err()
	}

	service := server.NewService(specsAdapter{specs}, candlesv1.NewCandlesServiceClient(candlesConn))
	brokers := kafka.Brokers(cfg.KafkaBrokers)
	rt, err := recoverState(ctx, brokers, persist.New(rdb), service, specs, cfg)
	if err != nil {
		return err
	}

	srv := &http.Server{Addr: cfg.HTTPAddr, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 2 * time.Minute,
		Handler: service.Handler(server.HTTPOptions{WSOrigins: cfg.WSOrigins, Ready: rt.ready})}
	svc.ServeAdmin(ctx, cfg.AdminAddr, rt.ready)

	g.Go(func() error { return rt.run(gctx) })
	g.Go(func() error { service.RunTickerFlusher(gctx, 250*time.Millisecond); return nil })
	g.Go(func() error { return consumeLive(gctx, brokers, service) })
	g.Go(func() error {
		slog.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(gctx), 5*time.Second)
		defer cancel()
		return srv.Shutdown(sctx)
	})
	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

type specsAdapter struct{ c *market.Cache }

func (a specsAdapter) Spec(symbol string) (market.Spec, bool) { return a.c.Spec(symbol) }
func (a specsAdapter) All() []market.Spec                     { return a.c.All() }

// runtimeState holds the per-partition workers.
type runtimeState struct {
	svc     *server.Service
	store   *persist.Store
	workers map[int32]*worker
	offsets map[int32]kgo.Offset
	brokers []string
	targets map[int32]int64 // end offsets at start: ready once every worker reached its target
	every   time.Duration
}

func (rt *runtimeState) ready(context.Context) error {
	for pid, w := range rt.workers {
		if w.next.Load() < rt.targets[pid] {
			return fmt.Errorf("replaying partition %d: at offset %d of %d", pid, w.next.Load(), rt.targets[pid])
		}
	}
	return nil
}

// recoverState loads the Redis snapshots, checks the log still covers them and
// prepares one worker per partition.
func recoverState(ctx context.Context, brokers []string, store *persist.Store, svcr *server.Service, specs *market.Cache, cfg Config) (*runtimeState, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	var parts []int32
	for {
		td, err := adm.ListTopics(ctx, topicEvents)
		if err == nil {
			if t, ok := td[topicEvents]; ok && t.Err == nil && len(t.Partitions) > 0 {
				parts = t.Partitions.Numbers()
				break
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	starts, err := adm.ListStartOffsets(ctx, topicEvents)
	if err != nil {
		return nil, err
	}
	ends, err := adm.ListEndOffsets(ctx, topicEvents)
	if err != nil {
		return nil, err
	}

	rt := &runtimeState{svc: svcr, store: store, workers: map[int32]*worker{}, offsets: map[int32]kgo.Offset{},
		brokers: brokers, targets: map[int32]int64{}, every: cfg.SnapshotEvery}
	for _, pid := range parts {
		snap, err := store.Load(ctx, pid)
		if err != nil {
			return nil, err
		}
		w := &worker{id: pid, svc: svcr, store: store, owned: map[string]struct{}{}, in: make(chan []*kgo.Record, 8), every: cfg.SnapshotEvery}
		var next int64
		if snap != nil {
			next = snap.Offset
			for _, raw := range snap.Markets {
				m, err := state.Unmarshal(raw)
				if err != nil {
					return nil, fmt.Errorf("partition %d snapshot: %w", pid, err)
				}
				svcr.Load(m)
				w.owned[m.Symbol] = struct{}{}
			}
		}
		lo, _ := starts.Lookup(topicEvents, pid)
		if next < lo.Offset {
			return nil, fmt.Errorf("partition %d: resume offset %d is before the log start %d; the market data cannot be rebuilt", pid, next, lo.Offset)
		}
		hi, _ := ends.Lookup(topicEvents, pid)
		w.next.Store(next)
		w.saved = next
		rt.workers[pid] = w
		rt.offsets[pid] = kgo.NewOffset().At(next)
		rt.targets[pid] = hi.Offset
		slog.Info("partition ready", "partition", pid, "resume_offset", next, "end_offset", hi.Offset, "markets", len(w.owned))
	}
	return rt, nil
}

func (rt *runtimeState) run(ctx context.Context) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(rt.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topicEvents: rt.offsets}),
		kgo.FetchMaxWait(20*time.Millisecond))
	if err != nil {
		return err
	}
	defer cl.Close()

	g, gctx := errgroup.WithContext(ctx)
	for _, w := range rt.workers {
		g.Go(func() error { return w.run(gctx) })
	}
	g.Go(func() error {
		defer func() {
			for _, w := range rt.workers {
				close(w.in)
			}
		}()
		for gctx.Err() == nil {
			fetches := cl.PollRecords(gctx, 2000)
			if gctx.Err() != nil {
				return nil
			}
			fetches.EachError(func(t string, p int32, err error) { slog.Error("fetch error", "topic", t, "partition", p, "err", err) })
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				if len(p.Records) == 0 {
					return
				}
				select {
				case rt.workers[p.Partition].in <- p.Records:
				case <-gctx.Done():
				}
			})
		}
		return nil
	})
	return g.Wait()
}

// worker applies the events of one partition and snapshots them to Redis.
type worker struct {
	id    int32
	svc   *server.Service
	store *persist.Store
	owned map[string]struct{}
	in    chan []*kgo.Record
	every time.Duration

	next    atomic.Int64 // offset of the next record to apply
	saved   int64
	lastSav time.Time
}

func (w *worker) run(ctx context.Context) error {
	for recs := range w.in {
		for _, r := range recs {
			if r.Offset < w.next.Load() {
				continue
			}
			var ev matchingv1.Event
			if err := proto.Unmarshal(r.Value, &ev); err != nil {
				slog.Error("dropping undecodable matching event", "partition", w.id, "offset", r.Offset, "err", err)
				w.next.Store(r.Offset + 1)
				continue
			}
			if err := w.apply(ctx, &ev, r.Timestamp); err != nil {
				return fmt.Errorf("partition %d offset %d: %w", w.id, r.Offset, err)
			}
			w.owned[ev.Market] = struct{}{}
			w.next.Store(r.Offset + 1)
		}
		if time.Since(w.lastSav) >= w.every {
			w.save(ctx)
		}
	}
	w.save(context.WithoutCancel(ctx)) // final snapshot on shutdown
	return nil
}

// apply retries while the market configuration is not known yet; a sequence
// gap is fatal (the process restarts and rebuilds from the last snapshot).
func (w *worker) apply(ctx context.Context, ev *matchingv1.Event, ts time.Time) error {
	for {
		err := w.svc.Apply(ev, ts)
		if err == nil || errors.Is(err, state.ErrGap) {
			return err
		}
		slog.Warn("cannot apply event yet", "market", ev.Market, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (w *worker) save(ctx context.Context) {
	next := w.next.Load()
	if next == w.saved {
		w.lastSav = time.Now()
		return
	}
	syms := make([]string, 0, len(w.owned))
	for s := range w.owned {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	var blobs [][]byte
	for _, s := range syms {
		b, err := w.svc.Snapshot(s)
		if err != nil || b == nil {
			slog.Error("snapshot failed", "market", s, "err", err)
			return
		}
		blobs = append(blobs, b)
	}
	if err := w.store.Save(ctx, w.id, next, blobs); err != nil {
		slog.Error("saving snapshot failed", "partition", w.id, "err", err)
		return
	}
	w.saved, w.lastSav = next, time.Now()
}

// consumeLive fans candles.live out to WebSocket subscribers. Only live
// updates matter, so it starts at the end of the topic and needs no group.
func consumeLive(ctx context.Context, brokers []string, s *server.Service) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topicLive),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()), kgo.FetchMaxWait(20*time.Millisecond))
	if err != nil {
		return err
	}
	defer cl.Close()
	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var u candlesv1.CandleUpdate
			if proto.Unmarshal(r.Value, &u) == nil {
				s.ApplyCandle(&u, r.Timestamp)
			}
		})
	}
	return nil
}
