// Package app wires the signer: hot-wallet keys in its own volume, a gRPC API
// served only over mutual TLS to the withdrawals service, and an `init`
// command that prepares the development PKI and publishes the hot-wallet
// addresses (never the keys) for funding scripts.
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	signerv1 "github.com/tickless/pkg/gen/tickless/signer/v1"
	"github.com/tickless/pkg/mtls"
	"github.com/tickless/pkg/otelx"
	"github.com/tickless/pkg/svc"
	"github.com/tickless/signer/internal/journal"
	"github.com/tickless/signer/internal/keys"
	"github.com/tickless/signer/internal/server"
)

// Config is the signer configuration.
type Config struct {
	GRPCAddr, AdminAddr string
	DataDir             string // keys and journal (signer only)
	PKIDir              string // server certificate (signer only)
	ClientPKIDir        string // client certificate for withdrawals (written by init)
	PublicDir           string // hotwallet.json with addresses (written by init)
	ServerHosts         []string
	AllowedClients      []string
	BTCRPCURL           string
	BTCRPCUser          string
	BTCRPCPassword      string
	BTCNetwork          string
	BTCFeeRate          int64
	ETHRPCURL           string
	ETHToken            string
}

// ConfigFromEnv reads the configuration from environment variables.
func ConfigFromEnv() Config {
	return Config{
		GRPCAddr: svc.Env("GRPC_ADDR", ":9110"), AdminAddr: svc.Env("ADMIN_ADDR", ":9210"),
		DataDir: svc.Env("SIGNER_DATA_DIR", "/data"), PKIDir: svc.Env("SIGNER_PKI_DIR", "/pki"),
		ClientPKIDir: svc.Env("SIGNER_CLIENT_PKI_DIR", "/client-pki"), PublicDir: svc.Env("SIGNER_PUBLIC_DIR", "/public"),
		ServerHosts:    strings.Split(svc.Env("SIGNER_SERVER_HOSTS", "signer,localhost,127.0.0.1"), ","),
		AllowedClients: strings.Split(svc.Env("SIGNER_ALLOWED_CLIENTS", "withdrawals"), ","),
		BTCRPCURL:      svc.Env("BTC_RPC_URL", ""), BTCRPCUser: svc.Env("BTC_RPC_USER", ""), BTCRPCPassword: svc.Env("BTC_RPC_PASSWORD", ""),
		BTCNetwork: svc.Env("BTC_NETWORK", "regtest"), BTCFeeRate: int64(svc.EnvInt("BTC_FEE_RATE", 2)),
		ETHRPCURL: svc.Env("ETH_RPC_URL", ""), ETHToken: svc.Env("ETH_USDT_CONTRACT", ""),
	}
}

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

// Init prepares the PKI and keys and writes the public hot-wallet addresses.
func Init(cfg Config) error {
	created, err := mtls.EnsureDevPKI(cfg.PKIDir, "signer", cfg.ServerHosts, cfg.ClientPKIDir, "withdrawals")
	if err != nil {
		return err
	}
	params, err := btcParams(cfg.BTCNetwork)
	if err != nil {
		return err
	}
	w, err := keys.LoadOrCreate(cfg.DataDir, params)
	if err != nil {
		return err
	}
	btcAddr, err := w.BTCAddress()
	if err != nil {
		return err
	}
	pub := map[string]string{"btc": btcAddr.EncodeAddress(), "eth": w.ETHAddress().Hex()}
	if err := os.MkdirAll(cfg.PublicDir, 0o755); err != nil { //nolint:gosec // public addresses only
		return err
	}
	b, _ := json.MarshalIndent(pub, "", "  ")
	if err := os.WriteFile(filepath.Join(cfg.PublicDir, "hotwallet.json"), b, 0o644); err != nil { //nolint:gosec // public addresses only
		return err
	}
	// Plain files for shell scripts: the address and nothing else.
	for chain, a := range pub {
		if err := os.WriteFile(filepath.Join(cfg.PublicDir, chain+".address"), []byte(a), 0o644); err != nil { //nolint:gosec // public addresses only
			return err
		}
	}
	slog.Info("signer initialised", "new_pki", created, "btc", pub["btc"], "eth", pub["eth"])
	return nil
}

// Run serves the signer until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	shutdown, err := otelx.Setup(ctx, "signer")
	if err != nil {
		return err
	}
	defer func() { _ = shutdown(context.Background()) }()
	params, err := btcParams(cfg.BTCNetwork)
	if err != nil {
		return err
	}
	w, err := keys.LoadOrCreate(cfg.DataDir, params)
	if err != nil {
		return err
	}
	j, err := journal.Open(filepath.Join(cfg.DataDir, "journal"))
	if err != nil {
		return err
	}
	s := &server.Server{Wallet: w, Journal: j, BTCFeeRate: cfg.BTCFeeRate, ETHTip: big.NewInt(1_000_000_000)}
	if cfg.BTCRPCURL != "" {
		s.BTC = server.NewBitcoindRPC(cfg.BTCRPCURL, cfg.BTCRPCUser, cfg.BTCRPCPassword)
	}
	if cfg.ETHRPCURL != "" {
		c, err := rpc.DialContext(ctx, cfg.ETHRPCURL)
		if err != nil {
			return err
		}
		defer c.Close()
		s.ETH = c
		if cfg.ETHToken != "" {
			if !common.IsHexAddress(cfg.ETHToken) {
				return fmt.Errorf("invalid ETH_USDT_CONTRACT %q", cfg.ETHToken)
			}
			s.Token = common.HexToAddress(cfg.ETHToken)
		}
	}
	tlsCfg, err := mtls.ServerTLS(cfg.PKIDir, "signer")
	if err != nil {
		return fmt.Errorf("signer PKI %s: %w (run `signer init` first)", mtls.Describe(cfg.PKIDir, "signer"), err)
	}
	g := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)), grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(mtls.RequireClientCN(cfg.AllowedClients...)))
	signerv1.RegisterSignerServiceServer(g, s)
	hs := health.NewServer()
	healthpb.RegisterHealthServer(g, hs)
	svc.ServeAdmin(ctx, cfg.AdminAddr, nil)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		done := make(chan struct{})
		go func() { g.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			g.Stop()
		}
	}()
	slog.Info("signer listening (mTLS)", "addr", cfg.GRPCAddr, "clients", cfg.AllowedClients)
	return g.Serve(lis)
}
