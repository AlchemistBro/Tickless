// Package app wires the candles service: Kafka -> ClickHouse ingestion of
// trades, a gRPC API for historical candles and live candle updates published
// to candles.live.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/candles/internal/store"
	candlesv1 "github.com/tickless/pkg/gen/tickless/candles/v1"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/kafka"
	"github.com/tickless/pkg/otelx"
	"github.com/tickless/pkg/svc"
)

// TopicLive carries live candle updates.
const TopicLive = "candles.live"

// Config is the candles configuration.
type Config struct {
	GRPCAddr, AdminAddr string
	KafkaBrokers        string
	ClickHouse          store.Options
}

// ConfigFromEnv reads the configuration from environment variables.
func ConfigFromEnv() Config {
	return Config{
		GRPCAddr: svc.Env("GRPC_ADDR", ":9106"), AdminAddr: svc.Env("ADMIN_ADDR", ":9206"),
		KafkaBrokers: svc.Env("KAFKA_BROKERS", "localhost:9092"),
		ClickHouse: store.Options{
			Addr: svc.Env("CLICKHOUSE_ADDR", "localhost:9000"), Database: svc.Env("CLICKHOUSE_DB", "tickless"),
			User: svc.Env("CLICKHOUSE_USER", "tickless"), Password: svc.Env("CLICKHOUSE_PASSWORD", "tickless"),
		},
	}
}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	shutdown, err := otelx.Setup(ctx, "candles")
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(context.Background()) }()

	var st *store.Store
	for { // ClickHouse may still be starting
		if st, err = store.Open(ctx, cfg.ClickHouse); err == nil {
			break
		}
		slog.Warn("clickhouse not ready", "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	defer func() { _ = st.Close() }()

	brokers := kafka.Brokers(cfg.KafkaBrokers)
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	ing := &ingester{st: st, prod: producer}
	ing.inserted, _ = otel.Meter("tickless/candles").Int64Counter("candles_trades_inserted_total", metric.WithDescription("Trades stored in ClickHouse"))

	grpcSrv, health := svc.NewGRPCServer()
	candlesv1.RegisterCandlesServiceServer(grpcSrv, &server{st: st})
	health.SetServingStatus("", 1)
	svc.ServeAdmin(ctx, cfg.AdminAddr, st.Ping)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		err := kafka.Consume(gctx, kafka.ConsumerConfig{Brokers: brokers, Group: "candles-ingest", Topics: []string{"matching.events"}, OnRevoked: st.ForgetCache}, ing.handle)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	})
	g.Go(func() error { return svc.ServeGRPC(gctx, cfg.GRPCAddr, grpcSrv) })
	return g.Wait()
}

type ingester struct {
	st       *store.Store
	prod     *kafka.Producer
	inserted metric.Int64Counter
}

// handle stores the trades of one partition batch (idempotently) and publishes
// the candles they changed. The live feed is best effort: if publishing fails
// after the insert the batch is retried, the store skips the already stored
// trades, and the next trade refreshes the candle anyway.
func (g *ingester) handle(ctx context.Context, recs []*kgo.Record) error {
	var trades []store.Trade
	for _, r := range recs {
		var ev matchingv1.Event
		if err := proto.Unmarshal(r.Value, &ev); err != nil {
			slog.Error("dropping undecodable matching event", "offset", r.Offset, "err", err)
			continue
		}
		if ev.Type != matchingv1.EventType_EVENT_TYPE_TRADE {
			continue
		}
		trades = append(trades, store.Trade{
			Market: ev.Market, TradeID: ev.TradeId, Seq: ev.Seq, Time: time.Unix(0, ev.TimeUnixNano).UTC(),
			Price: ev.Price, Qty: ev.Qty, TakerBuy: ev.Side == commonv1.Side_SIDE_BUY, TakerUser: ev.UserId, MakerUser: ev.MakerUserId,
		})
	}
	if len(trades) == 0 {
		return nil
	}
	fresh, err := g.st.Insert(ctx, trades)
	if err != nil {
		return fmt.Errorf("insert trades: %w", err)
	}
	g.inserted.Add(ctx, int64(len(fresh)))
	if len(fresh) == 0 {
		return nil
	}
	return g.publishLive(ctx, fresh)
}

func (g *ingester) publishLive(ctx context.Context, fresh []store.Trade) error {
	earliest := map[string]time.Time{}
	for _, t := range fresh {
		if e, ok := earliest[t.Market]; !ok || t.Time.Before(e) {
			earliest[t.Market] = t.Time
		}
	}
	var out []kafka.Record
	hdr := kafka.TraceHeaders(ctx)
	for market, from := range earliest {
		for _, iv := range store.IntervalNames {
			secs := store.Intervals[iv].Seconds
			cs, err := g.st.Candles(ctx, market, iv, from.Unix()/secs*secs, 0, 2000)
			if err != nil {
				return err
			}
			for i := len(cs) - 1; i >= 0; i-- { // oldest first
				b, err := proto.Marshal(&candlesv1.CandleUpdate{
					Meta:   &commonv1.EventMeta{EventId: uuid.Must(uuid.NewV7()).String()},
					Market: market, Interval: iv, Candle: toProto(cs[i]),
				})
				if err != nil {
					return err
				}
				out = append(out, kafka.Record{Topic: TopicLive, Key: []byte(market), Value: b, Headers: hdr})
			}
		}
	}
	return g.prod.Produce(ctx, out)
}

func toProto(c store.Candle) *candlesv1.Candle {
	return &candlesv1.Candle{OpenTimeUnix: c.OpenTime, Open: c.Open, High: c.High, Low: c.Low, Close: c.Close,
		VolumeLots: c.Lots, Turnover: c.Turnover, Trades: c.Trades}
}

type server struct {
	candlesv1.UnimplementedCandlesServiceServer
	st *store.Store
}

func (s *server) GetCandles(ctx context.Context, r *candlesv1.GetCandlesRequest) (*candlesv1.GetCandlesResponse, error) {
	if r.Market == "" {
		return nil, status.Error(codes.InvalidArgument, "market is required")
	}
	if _, ok := store.Intervals[r.Interval]; !ok {
		return nil, status.Errorf(codes.InvalidArgument, "interval must be one of %v", store.IntervalNames)
	}
	if r.FromUnix < 0 || r.ToUnix < 0 || (r.ToUnix != 0 && r.ToUnix < r.FromUnix) {
		return nil, status.Error(codes.InvalidArgument, "invalid time range")
	}
	cs, err := s.st.Candles(ctx, r.Market, r.Interval, r.FromUnix, r.ToUnix, int(r.Limit))
	if err != nil {
		slog.ErrorContext(ctx, "candles query failed", "err", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	out := &candlesv1.GetCandlesResponse{}
	for _, c := range cs {
		out.Candles = append(out.Candles, toProto(c))
	}
	return out, nil
}
