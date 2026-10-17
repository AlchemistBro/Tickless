// Package app wires the identity service.
package app

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tickless/identity/internal/password"
	"github.com/tickless/identity/internal/service"
	"github.com/tickless/identity/migrations"
	"github.com/tickless/pkg/auth"
	identityv1 "github.com/tickless/pkg/gen/tickless/identity/v1"
	"github.com/tickless/pkg/kafka"
	"github.com/tickless/pkg/otelx"
	"github.com/tickless/pkg/outbox"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/seal"
	"github.com/tickless/pkg/svc"
)

// Config is the identity configuration.
type Config struct {
	GRPCAddr, AdminAddr     string
	DatabaseURL, MigrateURL string
	KafkaBrokers            string
	// JWTSeed (base64, 32 bytes) fixes the signing key. Empty = ephemeral key
	// generated at start; access tokens then do not survive a restart.
	JWTSeed string
	// EncKey (base64, 32 bytes) encrypts TOTP seeds and API-key secrets at rest.
	EncKey string
	// DevEphemeralEncKey allows starting without EncKey using a random key
	// (secrets stored in this run become unreadable later). DEVELOPMENT ONLY.
	DevEphemeralEncKey bool
	AccessTTL          time.Duration
	AutoVerify         bool
	AdminEmail         string
	AdminPassword      string
	Argon              password.Params
}

// ConfigFromEnv reads the configuration from environment variables.
func ConfigFromEnv() Config {
	db := svc.Env("DATABASE_URL", "postgres://tickless:tickless@localhost:5432/identity?sslmode=disable")
	return Config{
		GRPCAddr: svc.Env("GRPC_ADDR", ":9103"), AdminAddr: svc.Env("ADMIN_ADDR", ":9203"),
		DatabaseURL: db, MigrateURL: svc.Env("MIGRATE_DATABASE_URL", db),
		KafkaBrokers: svc.Env("KAFKA_BROKERS", "localhost:9092"),
		JWTSeed:      svc.Env("JWT_ED25519_SEED", ""), EncKey: svc.Env("IDENTITY_ENC_KEY", ""),
		DevEphemeralEncKey: svc.EnvBool("IDENTITY_DEV_EPHEMERAL_ENC_KEY", false),
		AccessTTL:          time.Duration(svc.EnvInt("ACCESS_TOKEN_TTL_SECONDS", 600)) * time.Second,
		AutoVerify:         svc.EnvBool("IDENTITY_AUTO_VERIFY", false),
		AdminEmail:         svc.Env("IDENTITY_BOOTSTRAP_ADMIN_EMAIL", ""),
		AdminPassword:      svc.Env("IDENTITY_BOOTSTRAP_ADMIN_PASSWORD", ""),
		Argon: password.Params{MemoryKiB: uint32(svc.EnvInt("ARGON_MEMORY_KIB", int(password.Default.MemoryKiB))),
			Time: uint32(svc.EnvInt("ARGON_TIME", int(password.Default.Time))), Threads: 1},
	}
}

// Run starts the service and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	shutdown, err := otelx.Setup(ctx, "identity")
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

	var seed []byte
	if cfg.JWTSeed != "" {
		if seed, err = base64.StdEncoding.DecodeString(cfg.JWTSeed); err != nil {
			return fmt.Errorf("JWT_ED25519_SEED: %w", err)
		}
	} else {
		slog.Warn("JWT_ED25519_SEED not set: using an ephemeral signing key")
	}
	signer, err := auth.NewSigner(seed, cfg.AccessTTL)
	if err != nil {
		return err
	}
	sealer, err := newSealer(cfg)
	if err != nil {
		return err
	}
	s, err := service.New(pool, service.Options{Signer: signer, Sealer: sealer, Argon: cfg.Argon, AutoVerify: cfg.AutoVerify})
	if err != nil {
		return err
	}
	if cfg.AutoVerify {
		slog.Warn("IDENTITY_AUTO_VERIFY=true: new accounts are verified without email (development only)")
	}
	if cfg.AdminEmail != "" && cfg.AdminPassword != "" {
		if err := s.BootstrapAdmin(ctx, cfg.AdminEmail, cfg.AdminPassword); err != nil {
			return fmt.Errorf("bootstrap admin: %w", err)
		}
	}

	producer, err := kafka.NewProducer(kafka.Brokers(cfg.KafkaBrokers))
	if err != nil {
		return err
	}
	defer producer.Close()

	grpcSrv, health := svc.NewGRPCServer()
	identityv1.RegisterIdentityServiceServer(grpcSrv, &server{s: s, pub: signer.PublicKey()})
	health.SetServingStatus("", 1)
	svc.ServeAdmin(ctx, cfg.AdminAddr, func(ctx context.Context) error { return pool.Ping(ctx) })

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return (&outbox.Relay{Name: "identity", Pool: pool, Producer: producer}).Run(gctx) })
	g.Go(func() error { return svc.ServeGRPC(gctx, cfg.GRPCAddr, grpcSrv) })
	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func newSealer(cfg Config) (*seal.Sealer, error) {
	if cfg.EncKey == "" {
		if !cfg.DevEphemeralEncKey {
			return nil, errors.New("IDENTITY_ENC_KEY is required (base64, 32 bytes); set IDENTITY_DEV_EPHEMERAL_ENC_KEY=true only for throwaway development runs")
		}
		slog.Warn("using an ephemeral encryption key: stored API-key secrets become unreadable after restart")
		return seal.New(auth.RandomBytes(32))
	}
	k, err := seal.ParseKey(cfg.EncKey)
	if err != nil {
		return nil, err
	}
	return seal.New(k)
}

type server struct {
	identityv1.UnimplementedIdentityServiceServer
	s   *service.Service
	pub ed25519.PublicKey
}

func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, service.ErrInvalidEmail), errors.Is(err, service.ErrWeakPassword):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, service.ErrEmailTaken):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, service.ErrInvalidCredentials), errors.Is(err, service.ErrInvalidToken):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, service.ErrEmailNotVerified), errors.Is(err, service.ErrFrozen):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, service.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		slog.Error("identity rpc failed", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}

func pair(t service.Tokens) *identityv1.TokenPair {
	return &identityv1.TokenPair{AccessToken: t.Access, AccessExpiresUnix: t.AccessExpires.Unix(),
		RefreshToken: t.Refresh, RefreshExpiresUnix: t.RefreshExpires.Unix()}
}

func keyInfo(k service.APIKey) *identityv1.APIKeyInfo {
	return &identityv1.APIKeyInfo{KeyId: k.KeyID, Label: k.Label, CanTrade: k.CanTrade, CreatedAtUnix: k.CreatedAt.Unix(), Revoked: k.Revoked}
}

func (v *server) Signup(ctx context.Context, r *identityv1.SignupRequest) (*identityv1.SignupResponse, error) {
	id, err := v.s.Signup(ctx, r.Email, r.Password)
	if err != nil {
		return nil, mapErr(err)
	}
	return &identityv1.SignupResponse{UserId: id}, nil
}

func (v *server) VerifyEmail(ctx context.Context, r *identityv1.VerifyEmailRequest) (*identityv1.VerifyEmailResponse, error) {
	return &identityv1.VerifyEmailResponse{}, mapErr(v.s.VerifyEmail(ctx, r.Token))
}

func (v *server) Login(ctx context.Context, r *identityv1.LoginRequest) (*identityv1.LoginResponse, error) {
	t, err := v.s.Login(ctx, r.Email, r.Password)
	if err != nil {
		return nil, mapErr(err)
	}
	return &identityv1.LoginResponse{Tokens: pair(t)}, nil
}

func (v *server) Refresh(ctx context.Context, r *identityv1.RefreshRequest) (*identityv1.RefreshResponse, error) {
	t, err := v.s.Refresh(ctx, r.RefreshToken)
	if err != nil {
		return nil, mapErr(err)
	}
	return &identityv1.RefreshResponse{Tokens: pair(t)}, nil
}

func (v *server) Logout(ctx context.Context, r *identityv1.LogoutRequest) (*identityv1.LogoutResponse, error) {
	return &identityv1.LogoutResponse{}, mapErr(v.s.Logout(ctx, r.RefreshToken))
}

func (v *server) CreateAPIKey(ctx context.Context, r *identityv1.CreateAPIKeyRequest) (*identityv1.CreateAPIKeyResponse, error) {
	k, secret, err := v.s.CreateAPIKey(ctx, r.UserId, r.Label, r.CanTrade)
	if err != nil {
		return nil, mapErr(err)
	}
	return &identityv1.CreateAPIKeyResponse{Key: keyInfo(k), Secret: secret}, nil
}

func (v *server) ListAPIKeys(ctx context.Context, r *identityv1.ListAPIKeysRequest) (*identityv1.ListAPIKeysResponse, error) {
	ks, err := v.s.ListAPIKeys(ctx, r.UserId)
	if err != nil {
		return nil, mapErr(err)
	}
	out := &identityv1.ListAPIKeysResponse{}
	for _, k := range ks {
		out.Keys = append(out.Keys, keyInfo(k))
	}
	return out, nil
}

func (v *server) RevokeAPIKey(ctx context.Context, r *identityv1.RevokeAPIKeyRequest) (*identityv1.RevokeAPIKeyResponse, error) {
	return &identityv1.RevokeAPIKeyResponse{}, mapErr(v.s.RevokeAPIKey(ctx, r.UserId, r.KeyId))
}

func (v *server) LookupAPIKey(ctx context.Context, r *identityv1.LookupAPIKeyRequest) (*identityv1.LookupAPIKeyResponse, error) {
	ka, err := v.s.LookupAPIKey(ctx, r.KeyId)
	if err != nil {
		return nil, mapErr(err)
	}
	return &identityv1.LookupAPIKeyResponse{UserId: ka.UserID, Secret: ka.Secret, CanTrade: ka.CanTrade, Role: ka.Role}, nil
}

func (v *server) GetPublicKey(context.Context, *identityv1.GetPublicKeyRequest) (*identityv1.GetPublicKeyResponse, error) {
	return &identityv1.GetPublicKeyResponse{Ed25519PublicKey: v.pub}, nil
}

func (v *server) SetUserFrozen(ctx context.Context, r *identityv1.SetUserFrozenRequest) (*identityv1.SetUserFrozenResponse, error) {
	return &identityv1.SetUserFrozenResponse{}, mapErr(v.s.SetFrozen(ctx, r.UserId, r.Frozen))
}

func (v *server) ListUsers(ctx context.Context, r *identityv1.ListUsersRequest) (*identityv1.ListUsersResponse, error) {
	us, err := v.s.ListUsers(ctx, r.AfterId, int(r.Limit))
	if err != nil {
		return nil, mapErr(err)
	}
	out := &identityv1.ListUsersResponse{}
	for _, u := range us {
		out.Users = append(out.Users, &identityv1.UserInfo{UserId: u.ID, Email: u.Email, Role: u.Role,
			EmailVerified: u.Verified, Frozen: u.Frozen, CreatedAtUnix: u.CreatedAt.Unix()})
	}
	return out, nil
}

func (v *server) ListFrozen(ctx context.Context, _ *identityv1.ListFrozenRequest) (*identityv1.ListFrozenResponse, error) {
	ids, err := v.s.ListFrozen(ctx)
	if err != nil {
		return nil, mapErr(err)
	}
	return &identityv1.ListFrozenResponse{UserIds: ids}, nil
}
