//go:build integration

package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/identity/internal/password"
	"github.com/tickless/identity/internal/service"
	"github.com/tickless/identity/migrations"
	"github.com/tickless/pkg/auth"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/seal"
	"github.com/tickless/pkg/testkit"
)

type env struct {
	svc    *service.Service
	pool   *pgxpool.Pool
	signer *auth.Signer
	now    time.Time
}

func setup(t *testing.T, autoVerify bool) (*env, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	dsn := testkit.Postgres(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "."))
	pool, err := pg.Connect(ctx, dsn, 5)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	signer, _ := auth.NewSigner(nil, 10*time.Minute)
	sealer, _ := seal.New(auth.RandomBytes(32))
	e := &env{pool: pool, signer: signer, now: time.Now()}
	e.svc, err = service.New(pool, service.Options{
		Signer: signer, Sealer: sealer, Argon: password.Params{MemoryKiB: 8, Time: 1, Threads: 1},
		AutoVerify: autoVerify, Now: func() time.Time { return e.now },
	})
	require.NoError(t, err)
	return e, ctx
}

// verificationToken reads the token from the queued users.events message, as
// the notifier will (the raw token is never stored, only its hash).
func (e *env) verificationToken(t *testing.T, ctx context.Context, userID int64) string {
	t.Helper()
	var payload []byte
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT payload FROM outbox WHERE key = $1::text ORDER BY id LIMIT 1`, userID).Scan(&payload))
	// UserSignedUp.verification_token is field 4 (string): find it by decoding.
	return decodeToken(t, payload)
}

func TestSignupVerifyLogin(t *testing.T) {
	e, ctx := setup(t, false)
	id, err := e.svc.Signup(ctx, "  Alice@Example.COM ", "correct horse battery")
	require.NoError(t, err)

	_, err = e.svc.Signup(ctx, "alice@example.com", "another password!!")
	assert.ErrorIs(t, err, service.ErrEmailTaken, "case-insensitive uniqueness")

	_, err = e.svc.Login(ctx, "alice@example.com", "correct horse battery")
	assert.ErrorIs(t, err, service.ErrEmailNotVerified)

	require.NoError(t, e.svc.VerifyEmail(ctx, e.verificationToken(t, ctx, id)))
	assert.ErrorIs(t, e.svc.VerifyEmail(ctx, e.verificationToken(t, ctx, id)), service.ErrInvalidToken, "single use")

	tk, err := e.svc.Login(ctx, "ALICE@example.com", "correct horse battery")
	require.NoError(t, err)
	c, err := auth.NewVerifier(e.signer.PublicKey()).Verify(tk.Access, e.now)
	require.NoError(t, err)
	assert.Equal(t, id, c.UserID)
	assert.Equal(t, auth.RoleUser, c.Role)
	assert.True(t, c.EmailVerified)

	_, err = e.svc.Login(ctx, "alice@example.com", "wrong password!!")
	assert.ErrorIs(t, err, service.ErrInvalidCredentials)
	_, err = e.svc.Login(ctx, "nobody@example.com", "whatever password")
	assert.ErrorIs(t, err, service.ErrInvalidCredentials, "unknown user is indistinguishable")
}

func TestSignupValidation(t *testing.T) {
	e, ctx := setup(t, true)
	_, err := e.svc.Signup(ctx, "not-an-email", "correct horse battery")
	assert.ErrorIs(t, err, service.ErrInvalidEmail)
	_, err = e.svc.Signup(ctx, "Name <a@b.co>", "correct horse battery")
	assert.ErrorIs(t, err, service.ErrInvalidEmail)
	_, err = e.svc.Signup(ctx, "a@b.co", "short")
	assert.ErrorIs(t, err, service.ErrWeakPassword)
}

func TestVerificationTokenExpires(t *testing.T) {
	e, ctx := setup(t, false)
	id, _ := e.svc.Signup(ctx, "bob@example.com", "correct horse battery")
	tok := e.verificationToken(t, ctx, id)
	e.now = e.now.Add(25 * time.Hour)
	assert.ErrorIs(t, e.svc.VerifyEmail(ctx, tok), service.ErrInvalidToken)
}

func TestRefreshRotationAndReuseDetection(t *testing.T) {
	e, ctx := setup(t, true)
	_, _ = e.svc.Signup(ctx, "carol@example.com", "correct horse battery")
	t1, err := e.svc.Login(ctx, "carol@example.com", "correct horse battery")
	require.NoError(t, err)

	t2, err := e.svc.Refresh(ctx, t1.Refresh)
	require.NoError(t, err)
	assert.NotEqual(t, t1.Refresh, t2.Refresh)

	// Replaying the rotated token is theft: it fails AND kills the new session.
	_, err = e.svc.Refresh(ctx, t1.Refresh)
	assert.ErrorIs(t, err, service.ErrInvalidToken)
	_, err = e.svc.Refresh(ctx, t2.Refresh)
	assert.ErrorIs(t, err, service.ErrInvalidToken, "whole token family revoked after reuse")

	t3, err := e.svc.Login(ctx, "carol@example.com", "correct horse battery")
	require.NoError(t, err)
	require.NoError(t, e.svc.Logout(ctx, t3.Refresh))
	_, err = e.svc.Refresh(ctx, t3.Refresh)
	assert.ErrorIs(t, err, service.ErrInvalidToken, "logged out")

	t4, _ := e.svc.Login(ctx, "carol@example.com", "correct horse battery")
	e.now = e.now.Add(31 * 24 * time.Hour)
	_, err = e.svc.Refresh(ctx, t4.Refresh)
	assert.ErrorIs(t, err, service.ErrInvalidToken, "expired")
}

func TestAPIKeys(t *testing.T) {
	e, ctx := setup(t, true)
	uid, _ := e.svc.Signup(ctx, "dave@example.com", "correct horse battery")
	k, secret, err := e.svc.CreateAPIKey(ctx, uid, "bot", true)
	require.NoError(t, err)
	assert.NotEmpty(t, secret)

	var enc []byte
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT secret_enc FROM api_keys WHERE key_id=$1`, k.KeyID).Scan(&enc))
	assert.NotContains(t, string(enc), secret, "secret is encrypted at rest")

	ka, err := e.svc.LookupAPIKey(ctx, k.KeyID)
	require.NoError(t, err)
	assert.Equal(t, secret, ka.Secret)
	assert.True(t, ka.CanTrade)
	assert.Equal(t, uid, ka.UserID)

	ks, _ := e.svc.ListAPIKeys(ctx, uid)
	require.Len(t, ks, 1)

	other, _ := e.svc.Signup(ctx, "eve@example.com", "correct horse battery")
	assert.ErrorIs(t, e.svc.RevokeAPIKey(ctx, other, k.KeyID), service.ErrNotFound, "cannot revoke someone else's key")
	require.NoError(t, e.svc.RevokeAPIKey(ctx, uid, k.KeyID))
	_, err = e.svc.LookupAPIKey(ctx, k.KeyID)
	assert.ErrorIs(t, err, service.ErrNotFound)
}

func TestFreezeBlocksLoginRefreshAndKeys(t *testing.T) {
	e, ctx := setup(t, true)
	uid, _ := e.svc.Signup(ctx, "frank@example.com", "correct horse battery")
	tk, _ := e.svc.Login(ctx, "frank@example.com", "correct horse battery")
	k, _, _ := e.svc.CreateAPIKey(ctx, uid, "", false)

	require.NoError(t, e.svc.SetFrozen(ctx, uid, true))
	_, err := e.svc.Login(ctx, "frank@example.com", "correct horse battery")
	assert.ErrorIs(t, err, service.ErrFrozen)
	_, err = e.svc.Refresh(ctx, tk.Refresh)
	assert.ErrorIs(t, err, service.ErrInvalidToken, "sessions revoked on freeze")
	_, err = e.svc.LookupAPIKey(ctx, k.KeyID)
	assert.ErrorIs(t, err, service.ErrFrozen)

	require.NoError(t, e.svc.SetFrozen(ctx, uid, false))
	_, err = e.svc.Login(ctx, "frank@example.com", "correct horse battery")
	assert.NoError(t, err)
	assert.ErrorIs(t, e.svc.SetFrozen(ctx, 99999, true), service.ErrNotFound)
}

func TestBootstrapAdminAndListUsers(t *testing.T) {
	e, ctx := setup(t, true)
	require.NoError(t, e.svc.BootstrapAdmin(ctx, "admin@example.com", "admin password 123"))
	require.NoError(t, e.svc.BootstrapAdmin(ctx, "admin@example.com", "admin password 123"), "idempotent")
	tk, err := e.svc.Login(ctx, "admin@example.com", "admin password 123")
	require.NoError(t, err)
	c, _ := auth.NewVerifier(e.signer.PublicKey()).Verify(tk.Access, e.now)
	assert.Equal(t, auth.RoleAdmin, c.Role)

	_, _ = e.svc.Signup(ctx, "u1@example.com", "correct horse battery")
	us, err := e.svc.ListUsers(ctx, 0, 10)
	require.NoError(t, err)
	assert.Len(t, us, 2)
}
