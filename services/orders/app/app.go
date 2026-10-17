// Package app wires the orders service.
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

	"github.com/tickless/orders/internal/orders"
	"github.com/tickless/orders/migrations"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	ordersv1 "github.com/tickless/pkg/gen/tickless/orders/v1"
	"github.com/tickless/pkg/kafka"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/otelx"
	"github.com/tickless/pkg/outbox"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/svc"
)

// Config is the orders configuration.
type Config struct {
	GRPCAddr, AdminAddr     string
	DatabaseURL, MigrateURL string
	KafkaBrokers            string
	LedgerAddr              string
	MaxOpenPerMarket        int
}

// ConfigFromEnv reads the configuration from environment variables.
func ConfigFromEnv() Config {
	db := svc.Env("DATABASE_URL", "postgres://tickless:tickless@localhost:5432/orders?sslmode=disable")
	return Config{
		GRPCAddr: svc.Env("GRPC_ADDR", ":9102"), AdminAddr: svc.Env("ADMIN_ADDR", ":9202"),
		DatabaseURL: db, MigrateURL: svc.Env("MIGRATE_DATABASE_URL", db),
		KafkaBrokers:     svc.Env("KAFKA_BROKERS", "localhost:9092"),
		LedgerAddr:       svc.Env("LEDGER_ADDR", "localhost:9101"),
		MaxOpenPerMarket: svc.EnvInt("MAX_OPEN_ORDERS_PER_MARKET", 200),
	}
}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	shutdown, err := otelx.Setup(ctx, "orders")
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(context.Background()) }()

	if err := pg.Migrate(ctx, cfg.MigrateURL, migrations.FS, "."); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	pool, err := pg.Connect(ctx, cfg.DatabaseURL, 30)
	if err != nil {
		return err
	}
	defer pool.Close()
	markets, err := orders.NewMarkets(ctx, pool)
	if err != nil {
		return err
	}
	ledgerConn, err := svc.Dial(cfg.LedgerAddr)
	if err != nil {
		return err
	}
	defer func() { _ = ledgerConn.Close() }()
	service := orders.New(pool, grpcLedger{ledgerv1.NewLedgerServiceClient(ledgerConn)}, markets, orders.Config{MaxOpenPerMarket: cfg.MaxOpenPerMarket})

	brokers := kafka.Brokers(cfg.KafkaBrokers)
	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	grpcSrv, health := svc.NewGRPCServer()
	ordersv1.RegisterOrdersServiceServer(grpcSrv, &server{s: service, markets: markets})
	health.SetServingStatus("", 1)
	svc.ServeAdmin(ctx, cfg.AdminAddr, func(ctx context.Context) error { return pool.Ping(ctx) })

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { markets.Run(gctx); return nil })
	g.Go(func() error { service.RunSweeper(gctx, 2*time.Second); return nil })
	g.Go(func() error { return (&outbox.Relay{Name: "orders", Pool: pool, Producer: producer}).Run(gctx) })
	g.Go(func() error {
		err := kafka.Consume(gctx, kafka.ConsumerConfig{Brokers: brokers, Group: "orders-fills", Topics: []string{"matching.events"}},
			func(ctx context.Context, recs []*kgo.Record) error {
				in := make([]orders.Incoming, 0, len(recs))
				for _, r := range recs {
					var ev matchingv1.Event
					if err := proto.Unmarshal(r.Value, &ev); err != nil {
						slog.Error("dropping undecodable matching event", "offset", r.Offset, "err", err)
						continue
					}
					in = append(in, orders.Incoming{Ctx: kafka.ExtractContext(ctx, r), Event: &ev})
				}
				return service.ApplyEvents(ctx, in)
			})
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	})
	g.Go(func() error { return svc.ServeGRPC(gctx, cfg.GRPCAddr, grpcSrv) })
	return g.Wait()
}

// grpcLedger adapts the ledger gRPC client to orders.Ledger.
type grpcLedger struct{ c ledgerv1.LedgerServiceClient }

func (l grpcLedger) PlaceHold(ctx context.Context, orderID uint64, userID int64, asset string, amount money.Int128) error {
	_, err := l.c.PlaceHold(ctx, &ledgerv1.PlaceHoldRequest{OrderId: orderID, UserId: userID, Asset: asset,
		Amount: &commonv1.Amount{Units: amount.String()}})
	if status.Code(err) == codes.FailedPrecondition {
		return orders.ErrInsufficientFunds
	}
	return err
}

type server struct {
	ordersv1.UnimplementedOrdersServiceServer
	s       *orders.Service
	markets *orders.Markets
}

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, orders.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, orders.ErrMarketNotFound), errors.Is(err, orders.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, orders.ErrMarketHalted), errors.Is(err, orders.ErrInsufficientFunds), errors.Is(err, orders.ErrNotCancellable):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, orders.ErrTooManyOpen):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, orders.ErrLedgerUnavailable):
		return status.Error(codes.Unavailable, "ledger unavailable, retry with the same client_order_id")
	default:
		slog.Error("orders rpc failed", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}

func (v *server) order(o orders.Order) *ordersv1.Order {
	spec, _ := v.s.Spec(o.Market)
	st := map[string]ordersv1.OrderStatus{
		orders.StatusPendingHold: ordersv1.OrderStatus_ORDER_STATUS_PENDING_HOLD,
		orders.StatusNew:         ordersv1.OrderStatus_ORDER_STATUS_NEW,
		orders.StatusPartial:     ordersv1.OrderStatus_ORDER_STATUS_PARTIALLY_FILLED,
		orders.StatusFilled:      ordersv1.OrderStatus_ORDER_STATUS_FILLED,
		orders.StatusCancelled:   ordersv1.OrderStatus_ORDER_STATUS_CANCELLED,
		orders.StatusRejected:    ordersv1.OrderStatus_ORDER_STATUS_REJECTED,
	}[o.Status]
	price := ""
	if o.PriceTicks > 0 {
		price = spec.FormatPrice(o.PriceTicks)
	}
	return &ordersv1.Order{
		Id: o.ID, UserId: o.UserID, Market: o.Market, Side: commonv1.Side(o.Side), OrderType: o.OrderType,
		TimeInForce: o.TIF, PostOnly: o.PostOnly, Price: price, Quantity: spec.FormatQty(o.QtyLots),
		FilledQuantity: spec.FormatQty(o.FilledLots), Status: st, ClientOrderId: o.ClientID, RejectReason: o.RejectReason,
		CreatedAtUnixNano: o.CreatedAt.UnixNano(), UpdatedAtUnixNano: o.UpdatedAt.UnixNano(),
	}
}

func (v *server) PlaceOrder(ctx context.Context, r *ordersv1.PlaceOrderRequest) (*ordersv1.PlaceOrderResponse, error) {
	o, dup, err := v.s.Place(ctx, orders.PlaceRequest{
		UserID: r.UserId, Market: r.Market, Side: r.Side, OrderType: r.OrderType, TIF: r.TimeInForce,
		PostOnly: r.PostOnly, Price: r.Price, Quantity: r.Quantity, ClientOrderID: r.ClientOrderId,
	})
	if err != nil {
		return nil, mapErr(err)
	}
	return &ordersv1.PlaceOrderResponse{Order: v.order(o), Duplicate: dup}, nil
}

func (v *server) CancelOrder(ctx context.Context, r *ordersv1.CancelOrderRequest) (*ordersv1.CancelOrderResponse, error) {
	o, err := v.s.Cancel(ctx, r.UserId, r.OrderId)
	if err != nil {
		return nil, mapErr(err)
	}
	return &ordersv1.CancelOrderResponse{Order: v.order(o)}, nil
}

func (v *server) CancelAll(ctx context.Context, r *ordersv1.CancelAllRequest) (*ordersv1.CancelAllResponse, error) {
	n, err := v.s.CancelAll(ctx, r.UserId, r.Market)
	if err != nil {
		return nil, mapErr(err)
	}
	return &ordersv1.CancelAllResponse{Requested: uint32(n)}, nil
}

func (v *server) GetOrder(ctx context.Context, r *ordersv1.GetOrderRequest) (*ordersv1.GetOrderResponse, error) {
	o, err := v.s.Get(ctx, r.UserId, r.OrderId)
	if err != nil {
		return nil, mapErr(err)
	}
	return &ordersv1.GetOrderResponse{Order: v.order(o)}, nil
}

func (v *server) ListOrders(ctx context.Context, r *ordersv1.ListOrdersRequest) (*ordersv1.ListOrdersResponse, error) {
	os, err := v.s.List(ctx, r.UserId, r.Market, r.OpenOnly, r.BeforeId, int(r.Limit))
	if err != nil {
		return nil, mapErr(err)
	}
	out := &ordersv1.ListOrdersResponse{}
	for _, o := range os {
		out.Orders = append(out.Orders, v.order(o))
	}
	return out, nil
}

func (v *server) ListMarkets(context.Context, *ordersv1.ListMarketsRequest) (*ordersv1.ListMarketsResponse, error) {
	out := &ordersv1.ListMarketsResponse{}
	for _, m := range v.markets.All() {
		out.Markets = append(out.Markets, m.ToProto())
	}
	return out, nil
}

func (v *server) SetMarketStatus(ctx context.Context, r *ordersv1.SetMarketStatusRequest) (*ordersv1.SetMarketStatusResponse, error) {
	if r.Status != ordersv1.MarketStatus_MARKET_STATUS_ACTIVE && r.Status != ordersv1.MarketStatus_MARKET_STATUS_HALTED {
		return nil, status.Error(codes.InvalidArgument, "status must be ACTIVE or HALTED")
	}
	mi, err := v.markets.SetHalted(ctx, r.Market, r.Status == ordersv1.MarketStatus_MARKET_STATUS_HALTED)
	if err != nil {
		return nil, mapErr(err)
	}
	return &ordersv1.SetMarketStatusResponse{Market: mi.ToProto()}, nil
}
