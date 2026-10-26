// Package app wires the gateway: REST edge, Redis, gRPC clients and the
// users.events consumer that keeps the frozen-user set current.
package app

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/gateway/internal/httpapi"
	chainwatcherv1 "github.com/tickless/pkg/gen/tickless/chainwatcher/v1"
	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	ordersv1 "github.com/tickless/pkg/gen/tickless/orders/v1"
	usersv1 "github.com/tickless/pkg/gen/tickless/users/v1"
	walletv1 "github.com/tickless/pkg/gen/tickless/wallet/v1"
	withdrawalsv1 "github.com/tickless/pkg/gen/tickless/withdrawals/v1"
	"github.com/tickless/pkg/kafka"
	"github.com/tickless/pkg/market"
	"github.com/tickless/pkg/otelx"
	"github.com/tickless/pkg/svc"
)

// Config is the gateway configuration.
type Config struct {
	HTTPAddr, AdminAddr                  string
	IdentityAddr, LedgerAddr, OrdersAddr string
	WalletAddr, ChainwatcherAddr         string // empty disables deposits
	WithdrawalsAddr                      string // empty disables withdrawals
	RedisAddr                            string
	KafkaBrokers                         string
	MarketDataURL                        string
	WSOrigins                            []string
	AllowedOrigins                       []string
	TrustProxy                           bool
	Limits                               httpapi.Limits
}

// ConfigFromEnv reads the configuration from environment variables.
func ConfigFromEnv() Config {
	l := httpapi.DefaultLimits
	l.IPRate = float64(svc.EnvInt("RATE_IP_PER_SEC", int(l.IPRate)))
	l.IPBurst = l.IPRate * 2
	l.UserRate = float64(svc.EnvInt("RATE_USER_PER_SEC", int(l.UserRate)))
	l.UserBurst = l.UserRate * 2
	l.OrderRate = float64(svc.EnvInt("RATE_ORDER_PER_SEC", int(l.OrderRate)))
	l.OrderBurst = l.OrderRate * 2
	l.AuthBurst = float64(svc.EnvInt("RATE_AUTH_BURST", int(l.AuthBurst)))
	var origins []string
	for _, o := range strings.Split(svc.Env("CORS_ALLOWED_ORIGINS", ""), ",") {
		if o = strings.TrimSpace(o); o != "" {
			origins = append(origins, o)
		}
	}
	var wsOrigins []string
	for _, o := range origins {
		wsOrigins = append(wsOrigins, strings.TrimPrefix(strings.TrimPrefix(o, "http://"), "https://"))
	}
	return Config{
		MarketDataURL: svc.Env("MARKETDATA_URL", "http://localhost:8081"), WSOrigins: wsOrigins,
		HTTPAddr: svc.Env("HTTP_ADDR", ":8080"), AdminAddr: svc.Env("ADMIN_ADDR", ":9205"),
		IdentityAddr: svc.Env("IDENTITY_ADDR", "localhost:9103"), LedgerAddr: svc.Env("LEDGER_ADDR", "localhost:9101"),
		OrdersAddr: svc.Env("ORDERS_ADDR", "localhost:9102"), RedisAddr: svc.Env("REDIS_ADDR", "localhost:6379"),
		WalletAddr: svc.Env("WALLET_ADDR", ""), ChainwatcherAddr: svc.Env("CHAINWATCHER_ADDR", ""),
		WithdrawalsAddr: svc.Env("WITHDRAWALS_ADDR", ""),
		KafkaBrokers:    svc.Env("KAFKA_BROKERS", "localhost:9092"), AllowedOrigins: origins,
		TrustProxy: svc.EnvBool("TRUST_PROXY", false), Limits: l,
	}
}

// Run starts the gateway and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	shutdown, err := otelx.Setup(ctx, "gateway")
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(context.Background()) }()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = rdb.Close() }()

	idConn, err := svc.Dial(cfg.IdentityAddr)
	if err != nil {
		return err
	}
	defer func() { _ = idConn.Close() }()
	ledgerConn, err := svc.Dial(cfg.LedgerAddr)
	if err != nil {
		return err
	}
	defer func() { _ = ledgerConn.Close() }()
	ordersConn, err := svc.Dial(cfg.OrdersAddr)
	if err != nil {
		return err
	}
	defer func() { _ = ordersConn.Close() }()
	var wallet walletv1.WalletServiceClient
	var chainwatcher chainwatcherv1.ChainwatcherServiceClient
	if cfg.WalletAddr != "" && cfg.ChainwatcherAddr != "" {
		walletConn, err := svc.Dial(cfg.WalletAddr)
		if err != nil {
			return err
		}
		defer func() { _ = walletConn.Close() }()
		cwConn, err := svc.Dial(cfg.ChainwatcherAddr)
		if err != nil {
			return err
		}
		defer func() { _ = cwConn.Close() }()
		wallet, chainwatcher = walletv1.NewWalletServiceClient(walletConn), chainwatcherv1.NewChainwatcherServiceClient(cwConn)
	}
	var withdrawals withdrawalsv1.WithdrawalsServiceClient
	if cfg.WithdrawalsAddr != "" {
		wdConn, err := svc.Dial(cfg.WithdrawalsAddr)
		if err != nil {
			return err
		}
		defer func() { _ = wdConn.Close() }()
		withdrawals = withdrawalsv1.NewWithdrawalsServiceClient(wdConn)
	}
	identity := identityv1.NewIdentityServiceClient(idConn)
	markets := market.NewCache(market.FetchFromOrders(ordersv1.NewOrdersServiceClient(ordersConn)))

	api := httpapi.New(httpapi.Deps{
		Identity: identity, Ledger: ledgerv1.NewLedgerServiceClient(ledgerConn), Orders: ordersv1.NewOrdersServiceClient(ordersConn),
		Wallet: wallet, Chainwatcher: chainwatcher, Withdrawals: withdrawals,
		Redis: rdb, Limits: cfg.Limits, AllowedOrigins: cfg.AllowedOrigins, TrustProxy: cfg.TrustProxy, RequireFrozenSync: true, MarketData: cfg.MarketDataURL, WSOrigins: cfg.WSOrigins, Markets: markets,
		PublicKey: func(ctx context.Context) (ed25519.PublicKey, error) {
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			r, err := identity.GetPublicKey(ctx, &identityv1.GetPublicKeyRequest{})
			if err != nil {
				return nil, err
			}
			return ed25519.PublicKey(r.Ed25519PublicKey), nil
		},
	})
	srv := &http.Server{
		Addr: cfg.HTTPAddr, Handler: otelhttp.NewHandler(api.Routes(), "gateway",
			otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method + " " + r.URL.Path })),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
		MaxHeaderBytes: 16 << 10,
	}
	svc.ServeAdmin(ctx, cfg.AdminAddr, func(ctx context.Context) error {
		if !api.FrozenSynced() {
			return errors.New("frozen-user set not yet rebuilt from identity")
		}
		return rdb.Ping(ctx).Err()
	})

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		slog.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(sctx)
	})
	g.Go(func() error { markets.Run(gctx, 30*time.Second); return nil })
	g.Go(func() error { return privateFeed(gctx, kafka.Brokers(cfg.KafkaBrokers), api) })
	g.Go(func() error {
		// Rebuild the frozen-user set from identity (retry until identity is reachable).
		for gctx.Err() == nil {
			sctx, cancel := context.WithTimeout(gctx, 5*time.Second)
			err := api.SyncFrozen(sctx)
			cancel()
			if err == nil {
				slog.Info("frozen-user set rebuilt from identity")
				return nil
			}
			slog.Warn("cannot rebuild frozen-user set yet", "err", err)
			select {
			case <-time.After(time.Second):
			case <-gctx.Done():
			}
		}
		return nil
	})
	g.Go(func() error {
		// Unique group per instance so that every gateway sees every freeze event.
		group := fmt.Sprintf("gateway-frozen-%d", time.Now().UnixNano())
		err := kafka.Consume(gctx, kafka.ConsumerConfig{Brokers: kafka.Brokers(cfg.KafkaBrokers), Group: group, Topics: []string{"users.events"}},
			func(ctx context.Context, recs []*kgo.Record) error {
				for _, r := range recs {
					var ev usersv1.UserEvent
					if err := proto.Unmarshal(r.Value, &ev); err != nil {
						slog.Error("dropping undecodable user event", "offset", r.Offset, "err", err)
						continue
					}
					if f := ev.GetFrozen(); f != nil {
						if err := applyFrozen(ctx, rdb, ev.UserId, f.Frozen); err != nil {
							return err
						}
					}
				}
				return nil
			})
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	})
	return g.Wait()
}

func applyFrozen(ctx context.Context, rdb redis.Cmdable, userID int64, frozen bool) error {
	id := fmt.Sprint(userID)
	if frozen {
		return rdb.SAdd(ctx, httpapi.FrozenSet, id).Err()
	}
	return rdb.SRem(ctx, httpapi.FrozenSet, id).Err()
}

// privateFeed fans matching and ledger events out to the private WebSocket
// connections of the users they belong to. Only live events matter (a client
// loads history over REST), so it starts at the end of the topics, without a
// consumer group.
func privateFeed(ctx context.Context, brokers []string, api *httpapi.Server) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics("matching.events", "ledger.events"),
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
			switch r.Topic {
			case "matching.events":
				var ev matchingv1.Event
				if proto.Unmarshal(r.Value, &ev) == nil {
					api.PublishMatching(&ev, r.Timestamp)
				}
			case "ledger.events":
				var ev ledgerv1.BalanceChanged
				if proto.Unmarshal(r.Value, &ev) == nil {
					api.PublishBalance(&ev, r.Timestamp)
				}
			}
		})
	}
	return nil
}
