// Package app wires the ledger service: PostgreSQL, the settlement consumer
// of matching.events, the outbox relay for ledger.events and the gRPC API.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/ledger/internal/store"
	"github.com/tickless/ledger/migrations"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	ordersv1 "github.com/tickless/pkg/gen/tickless/orders/v1"
	"github.com/tickless/pkg/kafka"
	"github.com/tickless/pkg/market"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/otelx"
	"github.com/tickless/pkg/outbox"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/svc"
)

// Config is the ledger configuration.
type Config struct {
	GRPCAddr, AdminAddr      string
	DatabaseURL, MigrateURL  string
	KafkaBrokers, OrdersAddr string
	// AllowSeed enables the AdminCredit RPC. DEVELOPMENT ONLY: it creates money
	// out of the EXTERNAL account without any on-chain deposit.
	AllowSeed bool
}

// ConfigFromEnv reads the configuration from environment variables.
func ConfigFromEnv() Config {
	db := svc.Env("DATABASE_URL", "postgres://tickless:tickless@localhost:5432/ledger?sslmode=disable")
	return Config{
		GRPCAddr: svc.Env("GRPC_ADDR", ":9101"), AdminAddr: svc.Env("ADMIN_ADDR", ":9201"),
		DatabaseURL: db, MigrateURL: svc.Env("MIGRATE_DATABASE_URL", db),
		KafkaBrokers: svc.Env("KAFKA_BROKERS", "localhost:9092"),
		OrdersAddr:   svc.Env("ORDERS_ADDR", "localhost:9102"),
		AllowSeed:    svc.EnvBool("LEDGER_ALLOW_SEED", false),
	}
}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	shutdown, err := otelx.Setup(ctx, "ledger")
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(context.Background()) }()

	if err := pg.Migrate(ctx, cfg.MigrateURL, migrations.FS, "."); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	pool, err := pg.Connect(ctx, cfg.DatabaseURL, 20)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool)

	ordersConn, err := svc.Dial(cfg.OrdersAddr)
	if err != nil {
		return err
	}
	defer ordersConn.Close()
	markets := market.NewCache(market.FetchFromOrders(ordersv1.NewOrdersServiceClient(ordersConn)))

	brokers := kafka.Brokers(cfg.KafkaBrokers)
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	grpcSrv, health := svc.NewGRPCServer()
	ledgerv1.RegisterLedgerServiceServer(grpcSrv, &server{st: st, allowSeed: cfg.AllowSeed})
	health.SetServingStatus("", 1)
	svc.ServeAdmin(ctx, cfg.AdminAddr, func(ctx context.Context) error { return pool.Ping(ctx) })

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { markets.Run(gctx, 30*time.Second); return nil })
	g.Go(func() error {
		return (&outbox.Relay{Name: "ledger", Pool: pool, Producer: producer}).Run(gctx)
	})
	g.Go(func() error {
		err := kafka.Consume(gctx, kafka.ConsumerConfig{Brokers: brokers, Group: "ledger-settle", Topics: []string{"matching.events"}},
			func(ctx context.Context, recs []*kgo.Record) error { return handleEvents(ctx, st, markets, recs) })
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	})
	g.Go(func() error { return svc.ServeGRPC(gctx, cfg.GRPCAddr, grpcSrv) })
	return g.Wait()
}

func handleEvents(ctx context.Context, st *store.Store, markets store.MarketLookup, recs []*kgo.Record) error {
	in := make([]store.Incoming, 0, len(recs))
	for _, r := range recs {
		var ev matchingv1.Event
		if err := proto.Unmarshal(r.Value, &ev); err != nil {
			// A corrupt record can never succeed; skip it loudly instead of wedging the partition.
			slog.Error("dropping undecodable matching event", "partition", r.Partition, "offset", r.Offset, "err", err)
			continue
		}
		in = append(in, store.Incoming{Ctx: kafka.ExtractContext(ctx, r), Event: &ev})
	}
	return st.ApplyEvents(ctx, markets, in)
}

type server struct {
	ledgerv1.UnimplementedLedgerServiceServer
	st        *store.Store
	allowSeed bool
}

func amount(a *commonv1.Amount) (money.Int128, error) {
	v, err := money.ParseInt128(a.GetUnits())
	if err != nil {
		return money.Int128{}, status.Errorf(codes.InvalidArgument, "invalid amount: %v", err)
	}
	return v, nil
}

func units(v money.Int128) *commonv1.Amount { return &commonv1.Amount{Units: v.String()} }

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrInsufficientFunds):
		return status.Error(codes.FailedPrecondition, "insufficient funds")
	case errors.Is(err, store.ErrInvalidAmount):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		slog.Error("ledger rpc failed", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}

func (s *server) PlaceHold(ctx context.Context, r *ledgerv1.PlaceHoldRequest) (*ledgerv1.PlaceHoldResponse, error) {
	amt, err := amount(r.Amount)
	if err != nil {
		return nil, err
	}
	if r.OrderId == 0 || r.UserId <= 0 || r.Asset == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id, user_id and asset are required")
	}
	already, err := s.st.PlaceHold(ctx, r.OrderId, r.UserId, r.Asset, amt)
	if err != nil {
		return nil, mapErr(err)
	}
	return &ledgerv1.PlaceHoldResponse{AlreadyHeld: already}, nil
}

func (s *server) ReleaseHold(ctx context.Context, r *ledgerv1.ReleaseHoldRequest) (*ledgerv1.ReleaseHoldResponse, error) {
	rel, err := s.st.ReleaseHold(ctx, r.OrderId)
	if err != nil {
		return nil, mapErr(err)
	}
	return &ledgerv1.ReleaseHoldResponse{Released: units(rel)}, nil
}

func (s *server) GetBalances(ctx context.Context, r *ledgerv1.GetBalancesRequest) (*ledgerv1.GetBalancesResponse, error) {
	bs, err := s.st.Balances(ctx, r.UserId)
	if err != nil {
		return nil, mapErr(err)
	}
	out := &ledgerv1.GetBalancesResponse{}
	for _, b := range bs {
		out.Balances = append(out.Balances, &ledgerv1.Balance{Asset: b.Asset, Available: units(b.Available), Held: units(b.Held)})
	}
	return out, nil
}

func (s *server) ListEntries(ctx context.Context, r *ledgerv1.ListEntriesRequest) (*ledgerv1.ListEntriesResponse, error) {
	es, err := s.st.Entries(ctx, r.UserId, r.Asset, r.BeforeId, int(r.Limit))
	if err != nil {
		return nil, mapErr(err)
	}
	out := &ledgerv1.ListEntriesResponse{}
	for _, e := range es {
		pe := &ledgerv1.Entry{Id: e.ID, BusinessKey: e.BusinessKey, Kind: e.Kind, CreatedAtUnixNano: e.CreatedAt}
		for _, p := range e.Postings {
			pe.Postings = append(pe.Postings, &ledgerv1.Posting{Asset: p.Asset, AccountKind: p.Kind, Amount: units(p.Amount)})
		}
		out.Entries = append(out.Entries, pe)
	}
	return out, nil
}

func (s *server) AdminCredit(ctx context.Context, r *ledgerv1.AdminCreditRequest) (*ledgerv1.AdminCreditResponse, error) {
	if !s.allowSeed {
		return nil, status.Error(codes.PermissionDenied, "AdminCredit is disabled (LEDGER_ALLOW_SEED=false)")
	}
	amt, err := amount(r.Amount)
	if err != nil {
		return nil, err
	}
	if r.BusinessKey == "" || r.UserId <= 0 || r.Asset == "" {
		return nil, status.Error(codes.InvalidArgument, "business_key, user_id and asset are required")
	}
	id, already, err := s.st.Credit(ctx, r.BusinessKey, r.UserId, r.Asset, amt)
	if err != nil {
		return nil, mapErr(err)
	}
	return &ledgerv1.AdminCreditResponse{AlreadyApplied: already, EntryId: id}, nil
}
