// Package app wires the withdrawals service: the gRPC API (requests, admin
// review), the RabbitMQ screening consumer, the execution/confirmation/
// finalisation workers, the outbox relay for withdrawals.events and metrics.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/tickless/pkg/btcrpc"
	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	signerv1 "github.com/tickless/pkg/gen/tickless/signer/v1"
	withdrawalsv1 "github.com/tickless/pkg/gen/tickless/withdrawals/v1"
	"github.com/tickless/pkg/kafka"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/mtls"
	"github.com/tickless/pkg/otelx"
	"github.com/tickless/pkg/outbox"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/rabbit"
	"github.com/tickless/pkg/svc"
	"github.com/tickless/withdrawals/internal/engine"
	"github.com/tickless/withdrawals/internal/rules"
	"github.com/tickless/withdrawals/internal/store"
	"github.com/tickless/withdrawals/migrations"
)

// Config is the withdrawals configuration.
type Config struct {
	GRPCAddr, AdminAddr              string
	DatabaseURL, MigrateURL          string
	KafkaBrokers, RabbitURL          string
	LedgerAddr, IdentityAddr         string
	SignerAddr, SignerPKIDir         string
	BTCRPCURL, BTCRPCUser, BTCRPCPwd string
	ETHRPCURL                        string
	DenylistPath                     string
	DailyLimit, ReviewAbove          map[string]string // decimal amounts per asset
	BTCConfirmations                 int64
	ETHConfirmations                 int64
}

// ConfigFromEnv reads the configuration from environment variables.
func ConfigFromEnv() Config {
	db := svc.Env("DATABASE_URL", "postgres://tickless:tickless@localhost:5432/withdrawals?sslmode=disable")
	return Config{
		GRPCAddr: svc.Env("GRPC_ADDR", ":9111"), AdminAddr: svc.Env("ADMIN_ADDR", ":9211"),
		DatabaseURL: db, MigrateURL: svc.Env("MIGRATE_DATABASE_URL", db),
		KafkaBrokers: svc.Env("KAFKA_BROKERS", "localhost:9092"), RabbitURL: svc.Env("RABBITMQ_URL", "amqp://tickless:tickless@localhost:5672/"),
		LedgerAddr: svc.Env("LEDGER_ADDR", "localhost:9101"), IdentityAddr: svc.Env("IDENTITY_ADDR", "localhost:9103"),
		SignerAddr: svc.Env("SIGNER_ADDR", "localhost:9110"), SignerPKIDir: svc.Env("SIGNER_PKI_DIR", "/pki"),
		BTCRPCURL: svc.Env("BTC_RPC_URL", ""), BTCRPCUser: svc.Env("BTC_RPC_USER", ""), BTCRPCPwd: svc.Env("BTC_RPC_PASSWORD", ""),
		ETHRPCURL:        svc.Env("ETH_RPC_URL", ""),
		DenylistPath:     svc.Env("DENYLIST_PATH", ""),
		DailyLimit:       map[string]string{"BTC": svc.Env("DAILY_LIMIT_BTC", "1"), "ETH": svc.Env("DAILY_LIMIT_ETH", "10"), "USDT": svc.Env("DAILY_LIMIT_USDT", "100000")},
		ReviewAbove:      map[string]string{"BTC": svc.Env("REVIEW_ABOVE_BTC", "0.1"), "ETH": svc.Env("REVIEW_ABOVE_ETH", "1"), "USDT": svc.Env("REVIEW_ABOVE_USDT", "10000")},
		BTCConfirmations: int64(svc.EnvInt("BTC_CONFIRMATIONS", 2)), ETHConfirmations: int64(svc.EnvInt("ETH_CONFIRMATIONS", 12)),
	}
}

func amounts(m map[string]string) (map[string]money.Int128, error) {
	out := map[string]money.Int128{}
	for sym, dec := range m {
		a, ok := money.LookupAsset(sym)
		if !ok {
			return nil, fmt.Errorf("unknown asset %s", sym)
		}
		v, err := a.Parse(dec)
		if err != nil {
			return nil, fmt.Errorf("%s amount %q: %w", sym, dec, err)
		}
		out[sym] = v
	}
	return out, nil
}

// ScreeningQueue is the RabbitMQ topology of screening tasks.
var ScreeningQueue = rabbit.RetryQueue{Name: QueueScreening, Steps: []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	shutdown, err := otelx.Setup(ctx, "withdrawals")
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(context.Background()) }()
	limits, err := amounts(cfg.DailyLimit)
	if err != nil {
		return err
	}
	review, err := amounts(cfg.ReviewAbove)
	if err != nil {
		return err
	}
	deny, err := rules.LoadDenylist(cfg.DenylistPath)
	if err != nil {
		return fmt.Errorf("denylist: %w", err)
	}
	if err := pg.Migrate(ctx, cfg.MigrateURL, migrations.FS, "."); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	pool, err := pg.Connect(ctx, cfg.DatabaseURL, 20)
	if err != nil {
		return err
	}
	defer pool.Close()
	producer, err := kafka.NewProducer(kafka.Brokers(cfg.KafkaBrokers))
	if err != nil {
		return err
	}
	defer producer.Close()
	mq, err := rabbit.Dial(ctx, cfg.RabbitURL)
	if err != nil {
		return err
	}
	defer func() { _ = mq.Close() }()
	if err := mq.Declare(ScreeningQueue); err != nil {
		return err
	}
	if err := mq.DeclareQueue(QueueReview); err != nil {
		return err
	}

	ledgerConn, err := svc.Dial(cfg.LedgerAddr)
	if err != nil {
		return err
	}
	defer func() { _ = ledgerConn.Close() }()
	idConn, err := svc.Dial(cfg.IdentityAddr)
	if err != nil {
		return err
	}
	defer func() { _ = idConn.Close() }()
	tlsCfg, err := mtls.ClientTLS(cfg.SignerPKIDir, "withdrawals", "signer")
	if err != nil {
		return fmt.Errorf("signer client certificate %s: %w", mtls.Describe(cfg.SignerPKIDir, "withdrawals"), err)
	}
	signerConn, err := grpc.NewClient(cfg.SignerAddr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		return err
	}
	defer func() { _ = signerConn.Close() }()

	chains := map[string]engine.Chain{}
	confs := map[string]int64{}
	if cfg.BTCRPCURL != "" {
		chains["btc"] = btcChain{c: btcrpc.New(cfg.BTCRPCURL, cfg.BTCRPCUser, cfg.BTCRPCPwd)}
		confs["btc"] = cfg.BTCConfirmations
	}
	if cfg.ETHRPCURL != "" {
		c, err := rpc.DialContext(ctx, cfg.ETHRPCURL)
		if err != nil {
			return err
		}
		defer c.Close()
		chains["eth"] = ethChain{c: c}
		confs["eth"] = cfg.ETHConfirmations
	}

	st := store.New(pool)
	e := &engine.Engine{St: st, Ledger: ledgerAdapter{c: ledgerv1.NewLedgerServiceClient(ledgerConn)},
		Signer: signerAdapter{c: signerv1.NewSignerServiceClient(signerConn)}, Chains: chains,
		Verifier: verifier{c: identityv1.NewIdentityServiceClient(idConn)}, Queue: rabbitQueue{c: mq},
		Cfg: engine.Config{DailyLimit: limits, ReviewAbove: review, Confirmations: confs, Denylist: deny, RetryAfter: 10 * time.Second}}

	grpcSrv, health := svc.NewGRPCServer()
	withdrawalsv1.RegisterWithdrawalsServiceServer(grpcSrv, &server{e: e})
	health.SetServingStatus("", 1)
	svc.ServeAdmin(ctx, cfg.AdminAddr, func(ctx context.Context) error { return pool.Ping(ctx) })
	registerMetrics(st)
	slog.Info("withdrawals configured", "denylisted", deny.Len(), "chains", len(chains))

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return (&outbox.Relay{Name: "withdrawals", Pool: pool, Producer: producer}).Run(gctx) })
	g.Go(func() error {
		for gctx.Err() == nil {
			err := mq.Consume(gctx, ScreeningQueue, 8, func(ctx context.Context, d amqp.Delivery) error {
				id, err := strconv.ParseInt(string(d.Body), 10, 64)
				if err != nil {
					return rabbit.Permanent(fmt.Errorf("bad screening task %q", d.Body))
				}
				return e.Screen(ctx, id)
			})
			if gctx.Err() == nil {
				slog.Warn("screening consumer stopped, reconnecting", "err", err)
				sleep(gctx, 2*time.Second)
			}
		}
		return nil
	})
	loop := func(every time.Duration, name string, f func(context.Context) error) {
		g.Go(func() error {
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				if err := f(gctx); err != nil && gctx.Err() == nil {
					slog.Warn("worker failed", "worker", name, "err", err)
				}
				select {
				case <-gctx.Done():
					return nil
				case <-t.C:
				}
			}
		})
	}
	loop(time.Second, "execute", func(ctx context.Context) error { _, err := e.ExecuteOnce(ctx); return err })
	loop(2*time.Second, "confirm", func(ctx context.Context) error { _, err := e.ConfirmOnce(ctx); return err })
	loop(5*time.Second, "finalize", e.FinalizeOnce)
	loop(30*time.Second, "sweep", func(ctx context.Context) error { return e.SweepOnce(ctx, time.Minute) })
	loop(time.Minute, "denylist", func(context.Context) error { return deny.Reload() })
	g.Go(func() error { return svc.ServeGRPC(gctx, cfg.GRPCAddr, grpcSrv) })
	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (s *server) farFuture() time.Time { return time.Now().AddDate(100, 0, 0) }

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

// registerMetrics exposes withdrawals per non-final state (alert: queue stuck).
func registerMetrics(st *store.Store) {
	m := otel.Meter("tickless/withdrawals")
	g, err := m.Int64ObservableGauge("withdrawals_in_state", metric.WithDescription("Withdrawals per non-final state"))
	if err != nil {
		return
	}
	_, _ = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		counts, err := st.CountByState(ctx)
		if err != nil {
			return nil
		}
		for _, s := range []rules.State{rules.Requested, rules.Screening, rules.PendingReview, rules.Approved, rules.Signing, rules.Broadcast} {
			o.ObserveInt64(g, counts[s], metric.WithAttributes(attribute.String("state", string(s))))
		}
		return nil
	}, g)
}
