// Package mtls provides mutual TLS for internal gRPC links that carry
// privileged requests (withdrawals -> signer): a development PKI generator,
// TLS configurations for both sides and a server interceptor that accepts only
// listed client certificate common names.
//
// The development PKI is created once per deployment by an init job. The CA
// private key is discarded after issuing the two certificates, so nothing can
// mint further client certificates later; regenerating means a new CA.
package mtls

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// File names inside a PKI directory.
const (
	CAFile = "ca.pem"
)

func certFile(name string) string { return name + ".pem" }
func keyFile(name string) string  { return name + "-key.pem" }

func complete(dir, name string) bool {
	for _, f := range []string{CAFile, certFile(name), keyFile(name)} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			return false
		}
	}
	return true
}

// EnsureDevPKI creates a CA, a server certificate for serverName (written to
// serverDir) and a client certificate with common name clientName (written to
// clientDir), unless both directories are already complete. It returns true
// when it generated new material.
func EnsureDevPKI(serverDir, serverName string, serverHosts []string, clientDir, clientName string) (bool, error) {
	if complete(serverDir, serverName) && complete(clientDir, clientName) {
		return false, nil
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return false, err
	}
	now := time.Now()
	caTmpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: "Tickless dev internal CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(5, 0, 0),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return false, err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return false, err
	}
	srv := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: serverName},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(5, 0, 0),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	for _, h := range append([]string{serverName}, serverHosts...) {
		if ip := net.ParseIP(h); ip != nil {
			srv.IPAddresses = append(srv.IPAddresses, ip)
		} else {
			srv.DNSNames = append(srv.DNSNames, h)
		}
	}
	cli := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: clientName},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(5, 0, 0),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	for _, it := range []struct {
		dir, name string
		tmpl      *x509.Certificate
	}{{serverDir, serverName, srv}, {clientDir, clientName, cli}} {
		if err := issue(it.dir, it.name, it.tmpl, ca, caKey, caDER); err != nil {
			return false, err
		}
	}
	return true, nil
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	return n
}

func issue(dir, name string, tmpl, ca *x509.Certificate, caKey *ecdsa.PrivateKey, caDER []byte) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	for f, block := range map[string]*pem.Block{
		CAFile:         {Type: "CERTIFICATE", Bytes: caDER},
		certFile(name): {Type: "CERTIFICATE", Bytes: der},
		keyFile(name):  {Type: "EC PRIVATE KEY", Bytes: keyDER},
	} {
		if err := os.WriteFile(filepath.Join(dir, f), pem.EncodeToMemory(block), 0o600); err != nil {
			return err
		}
	}
	return nil
}

func pool(dir string) (*x509.CertPool, error) {
	b, err := os.ReadFile(filepath.Join(dir, CAFile)) //nolint:gosec // operator-configured PKI directory
	if err != nil {
		return nil, err
	}
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM(b) {
		return nil, errors.New("mtls: no CA certificate in " + CAFile)
	}
	return p, nil
}

// ServerTLS requires and verifies a client certificate issued by the CA in dir.
func ServerTLS(dir, name string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, certFile(name)), filepath.Join(dir, keyFile(name)))
	if err != nil {
		return nil, err
	}
	cas, err := pool(dir)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: cas, MinVersion: tls.VersionTLS13}, nil
}

// ClientTLS presents the client certificate `name` and trusts only the CA in dir.
func ClientTLS(dir, name, serverName string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, certFile(name)), filepath.Join(dir, keyFile(name)))
	if err != nil {
		return nil, err
	}
	cas, err := pool(dir)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: cas, ServerName: serverName, MinVersion: tls.VersionTLS13}, nil
}

// RequireClientCN rejects calls whose verified client certificate common name
// is not one of allowed (PERMISSION_DENIED). TLS already proved the CA.
func RequireClientCN(allowed ...string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		cn, err := clientCN(ctx)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}
		if !slices.Contains(allowed, cn) {
			return nil, status.Errorf(codes.PermissionDenied, "client %q may not call %s", cn, info.FullMethod)
		}
		return next(ctx, req)
	}
}

func clientCN(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", errors.New("mtls: no peer")
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(ti.State.VerifiedChains) == 0 || len(ti.State.VerifiedChains[0]) == 0 {
		return "", errors.New("mtls: no verified client certificate")
	}
	return ti.State.VerifiedChains[0][0].Subject.CommonName, nil
}

// Describe returns a short description of the PKI files for logs.
func Describe(dir, name string) string {
	return fmt.Sprintf("%s (%s, %s, %s)", dir, CAFile, certFile(name), keyFile(name))
}
