// Package app wires the chainwatcher service: one block scanner per configured
// chain (bitcoind regtest, Anvil), the deposit address registry and deposit
// queries over gRPC, and the outbox relay that publishes deposits.events.
package app

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tickless/chainwatcher/internal/btc"
	"github.com/tickless/chainwatcher/internal/eth"
	"github.com/tickless/chainwatcher/internal/scan"
	"github.com/tickless/chainwatcher/internal/store"
	"github.com/tickless/chainwatcher/migrations"
	chainwatcherv1 "github.com/tickless/pkg/gen/tickless/chainwatcher/v1"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	"github.com/tickless/pkg/kafka"
	"github.com/tickless/pkg/otelx"
	"github.com/tickless/pkg/outbox"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/svc"
)

// ChainConfig configures one followed chain. An empty RPCURL disables it.
type ChainConfig struct {
	Name          string
	RPCURL        string
	RPCUser       string
	RPCPassword   string
	Token         string // eth: ERC-20 contract of the USDT asset
	Confirmations int64
	StartHeight   int64
}

// Config is the chainwatcher configuration.
type Config struct {
	GRPCAddr, AdminAddr     string
	DatabaseURL, MigrateURL string
	KafkaBrokers            string
	PollInterval            time.Duration
	BTC, ETH                ChainConfig
}

// ConfigFromEnv reads the configuration from environment variables.
func ConfigFromEnv() Config {
	db := svc.Env("DATABASE_URL", "postgres://tickless:tickless@localhost:5432/chainwatcher?sslmode=disable")
	return Config{
		GRPCAddr: svc.Env("GRPC_ADDR", ":9108"), AdminAddr: svc.Env("ADMIN_ADDR", ":9208"),
		DatabaseURL: db, MigrateURL: svc.Env("MIGRATE_DATABASE_URL", db),
		KafkaBrokers: svc.Env("KAFKA_BROKERS", "localhost:9092"),
		PollInterval: time.Duration(svc.EnvInt("POLL_INTERVAL_MS", 1000)) * time.Millisecond,
		BTC: ChainConfig{Name: "btc", RPCURL: svc.Env("BTC_RPC_URL", ""), RPCUser: svc.Env("BTC_RPC_USER", ""), RPCPassword: svc.Env("BTC_RPC_PASSWORD", ""),
			Confirmations: int64(svc.EnvInt("BTC_CONFIRMATIONS", 2)), StartHeight: int64(svc.EnvInt("BTC_START_HEIGHT", 1))},
		ETH: ChainConfig{Name: "eth", RPCURL: svc.Env("ETH_RPC_URL", ""), Token: svc.Env("ETH_USDT_CONTRACT", ""),
			Confirmations: int64(svc.EnvInt("ETH_CONFIRMATIONS", 12)), StartHeight: int64(svc.EnvInt("ETH_START_HEIGHT", 1))},
	}
}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	shutdown, err := otelx.Setup(ctx, "chainwatcher")
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(context.Background()) }()

	if err := pg.Migrate(ctx, cfg.MigrateURL, migrations.FS, "."); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	pool, err := pg.Connect(ctx, cfg.DatabaseURL, 10)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool)

	producer, err := kafka.NewProducer(kafka.Brokers(cfg.KafkaBrokers))
	if err != nil {
		return err
	}
	defer producer.Close()

	required := map[string]int64{}
	var scanners []*follower
	if cfg.BTC.RPCURL != "" {
		c := btc.NewClient(cfg.BTC.RPCURL, cfg.BTC.RPCUser, cfg.BTC.RPCPassword)
		scanners = append(scanners, newFollower(cfg.BTC, btc.Chain{C: c}, st))
		required[cfg.BTC.Name] = cfg.BTC.Confirmations
	}
	if cfg.ETH.RPCURL != "" {
		token, err := eth.ParseToken(cfg.ETH.Token)
		if err != nil {
			return err
		}
		c, err := rpc.DialContext(ctx, cfg.ETH.RPCURL)
		if err != nil {
			return fmt.Errorf("eth rpc: %w", err)
		}
		defer c.Close()
		scanners = append(scanners, newFollower(cfg.ETH, eth.Chain{RPC: c, Token: token, TokenAsset: "USDT"}, st))
		required[cfg.ETH.Name] = cfg.ETH.Confirmations
	}
	if len(scanners) == 0 {
		return errors.New("no chain configured (BTC_RPC_URL / ETH_RPC_URL)")
	}
	registerMetrics(scanners)

	grpcSrv, health := svc.NewGRPCServer()
	chainwatcherv1.RegisterChainwatcherServiceServer(grpcSrv, &server{st: st, required: required})
	health.SetServingStatus("", 1)
	svc.ServeAdmin(ctx, cfg.AdminAddr, func(ctx context.Context) error { return pool.Ping(ctx) })

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return (&outbox.Relay{Name: "chainwatcher", Pool: pool, Producer: producer}).Run(gctx)
	})
	for _, f := range scanners {
		g.Go(func() error { return f.run(gctx, cfg.MigrateURL, cfg.PollInterval) })
	}
	g.Go(func() error { return svc.ServeGRPC(gctx, cfg.GRPCAddr, grpcSrv) })
	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// follower runs one chain's scanner while holding that chain's leader lock.
type follower struct {
	name    string
	scanner *scan.Scanner
	chain   scan.Chain

	tip, head  atomic.Int64
	lastStep   atomic.Int64 // unix seconds of the last successful step
	deepReorgs metric.Int64Counter
	stepErrors metric.Int64Counter
}

func newFollower(cc ChainConfig, chain scan.Chain, st *store.Store) *follower {
	f := &follower{name: cc.Name, chain: chain}
	m := otel.Meter("tickless/chainwatcher")
	f.deepReorgs, _ = m.Int64Counter("chainwatcher_deep_reorg_deposits_total", metric.WithDescription("Confirmed deposits removed by a reorg deeper than the threshold"))
	f.stepErrors, _ = m.Int64Counter("chainwatcher_step_errors_total", metric.WithDescription("Failed scanner steps (retried)"))
	f.scanner = &scan.Scanner{Chain: chain, Store: st.Chain(cc.Name), Name: cc.Name, Start: cc.StartHeight, Required: cc.Confirmations,
		OnDeepReorg: func(n int) {
			f.deepReorgs.Add(context.Background(), int64(n), metric.WithAttributes(attribute.String("chain", cc.Name)))
		}}
	return f
}

func lockKey(chain string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("chainwatcher:" + chain))
	return int64(h.Sum64()) //nolint:gosec // any 64-bit value is a valid advisory lock key
}

// run follows the chain. Only the instance holding the chain's session
// advisory lock scans it; the lock lives on a direct (non-pgbouncer)
// connection, because transaction pooling cannot hold session locks.
func (f *follower) run(ctx context.Context, directURL string, every time.Duration) error {
	for ctx.Err() == nil {
		if err := f.lead(ctx, directURL, every); err != nil && ctx.Err() == nil {
			slog.Error("chain follower stopped, retrying", "chain", f.name, "err", err)
			sleep(ctx, 2*time.Second)
		}
	}
	return nil
}

func (f *follower) lead(ctx context.Context, directURL string, every time.Duration) error {
	conn, err := pgx.Connect(ctx, directURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	for {
		var got bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, lockKey(f.name)).Scan(&got); err != nil {
			return err
		}
		if got {
			break
		}
		sleep(ctx, every)
		if ctx.Err() != nil {
			return nil
		}
	}
	slog.Info("following chain", "chain", f.name)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		f.step(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := conn.Ping(ctx); err != nil {
				return fmt.Errorf("leader connection lost: %w", err) // the lock went with it
			}
		}
	}
}

func (f *follower) step(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := f.scanner.Step(ctx)
		if err != nil {
			if ctx.Err() == nil {
				f.stepErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("chain", f.name)))
				slog.Warn("scanner step failed", "chain", f.name, "err", err)
			}
			return
		}
		f.lastStep.Store(time.Now().Unix())
		if tip, err := f.chain.TipHeight(ctx); err == nil {
			f.tip.Store(tip)
		}
		if h, _, ok, err := f.scanner.Store.Head(ctx); err == nil && ok {
			f.head.Store(h)
		}
		if n == 0 {
			return
		}
	}
}

func registerMetrics(fs []*follower) {
	m := otel.Meter("tickless/chainwatcher")
	tip, e1 := m.Int64ObservableGauge("chainwatcher_node_tip_height", metric.WithDescription("Best block height reported by the node"))
	head, e2 := m.Int64ObservableGauge("chainwatcher_head_height", metric.WithDescription("Highest block processed by the scanner"))
	last, e3 := m.Int64ObservableGauge("chainwatcher_last_step_unixtime", metric.WithDescription("Unix time of the last successful scanner step"))
	if err := errors.Join(e1, e2, e3); err != nil {
		slog.Warn("chainwatcher metrics", "err", err)
		return
	}
	_, _ = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, f := range fs {
			a := metric.WithAttributes(attribute.String("chain", f.name))
			o.ObserveInt64(tip, f.tip.Load(), a)
			o.ObserveInt64(head, f.head.Load(), a)
			o.ObserveInt64(last, f.lastStep.Load(), a)
		}
		return nil
	}, tip, head, last)
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

type server struct {
	chainwatcherv1.UnimplementedChainwatcherServiceServer
	st       *store.Store
	required map[string]int64
}

func (s *server) RegisterAddress(ctx context.Context, r *chainwatcherv1.RegisterAddressRequest) (*chainwatcherv1.RegisterAddressResponse, error) {
	if _, ok := s.required[r.Chain]; !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown chain %q", r.Chain)
	}
	if r.Address == "" || r.UserId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "address and user_id are required")
	}
	err := s.st.RegisterAddress(ctx, r.Chain, r.Address, r.UserId, r.DerivationIndex)
	switch {
	case errors.Is(err, store.ErrAddressTaken):
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	case err != nil:
		slog.ErrorContext(ctx, "register address", "err", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	return &chainwatcherv1.RegisterAddressResponse{}, nil
}

var statuses = map[string]chainwatcherv1.DepositStatus{
	store.StatusPending:   chainwatcherv1.DepositStatus_DEPOSIT_STATUS_PENDING,
	store.StatusConfirmed: chainwatcherv1.DepositStatus_DEPOSIT_STATUS_CONFIRMED,
	store.StatusReorged:   chainwatcherv1.DepositStatus_DEPOSIT_STATUS_REORGED,
}

func (s *server) ListDeposits(ctx context.Context, r *chainwatcherv1.ListDepositsRequest) (*chainwatcherv1.ListDepositsResponse, error) {
	if r.UserId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	ds, err := s.st.ListDeposits(ctx, r.UserId, r.BeforeId, int(r.Limit))
	if err != nil {
		slog.ErrorContext(ctx, "list deposits", "err", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	out := &chainwatcherv1.ListDepositsResponse{}
	for _, d := range ds {
		pd := &chainwatcherv1.Deposit{
			Id: d.ID, Chain: d.Chain, Txid: d.TxID, Index: d.Index, Asset: d.Asset,
			Amount: &commonv1.Amount{Units: d.Amount.String()}, Address: d.Address, Status: statuses[d.Status],
			BlockHeight: d.BlockHeight, Confirmations: d.Confirmations, RequiredConfirmations: s.required[d.Chain],
			DetectedAtUnixNano: d.DetectedAt.UnixNano(),
		}
		if d.ConfirmedAt != nil {
			pd.ConfirmedAtUnixNano = d.ConfirmedAt.UnixNano()
		}
		out.Deposits = append(out.Deposits, pd)
	}
	return out, nil
}
