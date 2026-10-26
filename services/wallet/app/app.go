// Package app wires the wallet service: deposit addresses derived from account
// xpubs (no private key anywhere in this service, ADR 0009), stored in the
// wallet database and registered with chainwatcher before they are returned.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/btcsuite/btcd/chaincfg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	chainwatcherv1 "github.com/tickless/pkg/gen/tickless/chainwatcher/v1"
	walletv1 "github.com/tickless/pkg/gen/tickless/wallet/v1"
	"github.com/tickless/pkg/otelx"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/svc"
	"github.com/tickless/wallet/internal/derive"
	"github.com/tickless/wallet/internal/store"
	"github.com/tickless/wallet/migrations"
)

// Config is the wallet configuration.
type Config struct {
	GRPCAddr, AdminAddr     string
	DatabaseURL, MigrateURL string
	ChainwatcherAddr        string
	BTCXPub, ETHXPub        string
	BTCNetwork              string // regtest | testnet
}

// ConfigFromEnv reads the configuration from environment variables.
func ConfigFromEnv() Config {
	db := svc.Env("DATABASE_URL", "postgres://tickless:tickless@localhost:5432/wallet?sslmode=disable")
	return Config{
		GRPCAddr: svc.Env("GRPC_ADDR", ":9109"), AdminAddr: svc.Env("ADMIN_ADDR", ":9209"),
		DatabaseURL: db, MigrateURL: svc.Env("MIGRATE_DATABASE_URL", db),
		ChainwatcherAddr: svc.Env("CHAINWATCHER_ADDR", "localhost:9108"),
		BTCXPub:          svc.Env("WALLET_BTC_XPUB", ""), ETHXPub: svc.Env("WALLET_ETH_XPUB", ""),
		BTCNetwork: svc.Env("WALLET_BTC_NETWORK", "regtest"),
	}
}

// btcParams refuses mainnet: Tickless runs on regtest and public testnets only.
func btcParams(network string) (*chaincfg.Params, error) {
	switch network {
	case "regtest":
		return &chaincfg.RegressionNetParams, nil
	case "testnet", "testnet3":
		return &chaincfg.TestNet3Params, nil
	default:
		return nil, fmt.Errorf("unsupported BTC network %q (regtest or testnet only)", network)
	}
}

// Derivers builds the per-chain derivers from the configured xpubs.
func Derivers(cfg Config) (map[string]derive.Deriver, error) {
	out := map[string]derive.Deriver{}
	if cfg.BTCXPub != "" {
		p, err := btcParams(cfg.BTCNetwork)
		if err != nil {
			return nil, err
		}
		d, err := derive.NewBTC(cfg.BTCXPub, p)
		if err != nil {
			return nil, fmt.Errorf("WALLET_BTC_XPUB: %w", err)
		}
		out["btc"] = d
	}
	if cfg.ETHXPub != "" {
		d, err := derive.NewETH(cfg.ETHXPub)
		if err != nil {
			return nil, fmt.Errorf("WALLET_ETH_XPUB: %w", err)
		}
		out["eth"] = d
	}
	if len(out) == 0 {
		return nil, errors.New("no xpub configured (WALLET_BTC_XPUB / WALLET_ETH_XPUB)")
	}
	return out, nil
}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	shutdown, err := otelx.Setup(ctx, "wallet")
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(context.Background()) }()
	ds, err := Derivers(cfg)
	if err != nil {
		return err
	}
	if err := pg.Migrate(ctx, cfg.MigrateURL, migrations.FS, "."); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	pool, err := pg.Connect(ctx, cfg.DatabaseURL, 10)
	if err != nil {
		return err
	}
	defer pool.Close()
	cw, err := svc.Dial(cfg.ChainwatcherAddr)
	if err != nil {
		return err
	}
	defer func() { _ = cw.Close() }()

	grpcSrv, health := svc.NewGRPCServer()
	walletv1.RegisterWalletServiceServer(grpcSrv, &Server{Store: store.New(pool), Derivers: ds, Registrar: chainwatcherv1.NewChainwatcherServiceClient(cw)})
	health.SetServingStatus("", 1)
	svc.ServeAdmin(ctx, cfg.AdminAddr, func(ctx context.Context) error { return pool.Ping(ctx) })
	return svc.ServeGRPC(ctx, cfg.GRPCAddr, grpcSrv)
}

// Registrar is the part of chainwatcher the wallet calls.
type Registrar interface {
	RegisterAddress(ctx context.Context, in *chainwatcherv1.RegisterAddressRequest, opts ...grpc.CallOption) (*chainwatcherv1.RegisterAddressResponse, error)
}

// Server implements walletv1.WalletServiceServer.
type Server struct {
	walletv1.UnimplementedWalletServiceServer
	Store     *store.Store
	Derivers  map[string]derive.Deriver
	Registrar Registrar
}

// GetDepositAddress implements walletv1.WalletServiceServer.
func (s *Server) GetDepositAddress(ctx context.Context, r *walletv1.GetDepositAddressRequest) (*walletv1.GetDepositAddressResponse, error) {
	d, ok := s.Derivers[r.Chain]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown chain %q", r.Chain)
	}
	if r.UserId <= 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	a, err := s.Store.GetOrCreate(ctx, r.UserId, r.Chain, d.Address)
	if err != nil {
		slog.ErrorContext(ctx, "deposit address", "err", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	// The address is only handed out once chainwatcher watches it, so no
	// deposit can arrive at an address nobody is scanning for.
	if !a.Registered {
		if _, err := s.Registrar.RegisterAddress(ctx, &chainwatcherv1.RegisterAddressRequest{
			Chain: r.Chain, Address: a.Address, UserId: r.UserId, DerivationIndex: a.Index,
		}); err != nil {
			slog.WarnContext(ctx, "register deposit address", "chain", r.Chain, "err", err)
			return nil, status.Error(codes.Unavailable, "deposit address registration failed, retry")
		}
		if err := s.Store.MarkRegistered(ctx, r.UserId, r.Chain); err != nil {
			slog.WarnContext(ctx, "mark registered", "err", err) // re-registering next time is harmless
		}
	}
	return &walletv1.GetDepositAddressResponse{Chain: r.Chain, Address: a.Address, DerivationIndex: a.Index}, nil
}
