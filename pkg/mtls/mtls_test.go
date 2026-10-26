package mtls_test

import (
	"context"
	"crypto/tls"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/tickless/pkg/mtls"
)

// serve starts a gRPC health server with the server PKI in dir, allowing allowed CNs.
func serve(t *testing.T, dir string, allowed ...string) string {
	t.Helper()
	cfg, err := mtls.ServerTLS(dir, "signer")
	require.NoError(t, err)
	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(cfg)), grpc.UnaryInterceptor(mtls.RequireClientCN(allowed...)))
	healthpb.RegisterHealthServer(s, health.NewServer())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func call(t *testing.T, addr string, cfg *tls.Config) error {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

func pki(t *testing.T, client string) (srvDir, cliDir string) {
	t.Helper()
	root := t.TempDir()
	srvDir, cliDir = filepath.Join(root, "srv"), filepath.Join(root, "cli")
	created, err := mtls.EnsureDevPKI(srvDir, "signer", []string{"localhost", "127.0.0.1"}, cliDir, client)
	require.NoError(t, err)
	require.True(t, created)
	return srvDir, cliDir
}

func TestOnlyTheListedClientOfTheSameCAGetsThrough(t *testing.T) {
	srvDir, cliDir := pki(t, "withdrawals")
	addr := serve(t, srvDir, "withdrawals")

	good, err := mtls.ClientTLS(cliDir, "withdrawals", "localhost")
	require.NoError(t, err)
	require.NoError(t, call(t, addr, good), "the withdrawals client is accepted")

	// A client without a certificate fails the handshake.
	bare := good.Clone()
	bare.Certificates = nil
	require.Error(t, call(t, addr, bare))

	// A client certificate from another CA fails the handshake.
	_, otherCli := pki(t, "withdrawals")
	foreign, err := mtls.ClientTLS(otherCli, "withdrawals", "localhost")
	require.NoError(t, err)
	foreign.RootCAs = good.RootCAs // trust the real server, present a foreign client cert
	require.Error(t, call(t, addr, foreign))
}

func TestValidCertificateWithAnotherNameIsRefused(t *testing.T) {
	srvDir, cliDir := pki(t, "intruder")
	addr := serve(t, srvDir, "withdrawals")
	cfg, err := mtls.ClientTLS(cliDir, "intruder", "localhost")
	require.NoError(t, err)
	err = call(t, addr, cfg)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "TLS passes, the name does not: %v", err)
}

func TestEnsureDevPKIIsIdempotent(t *testing.T) {
	srvDir, cliDir := pki(t, "withdrawals")
	created, err := mtls.EnsureDevPKI(srvDir, "signer", nil, cliDir, "withdrawals")
	require.NoError(t, err)
	require.False(t, created, "existing material is kept")
}
