// Package service implements the identity use cases: accounts, sessions,
// API keys and user administration.
package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/identity/internal/password"
	"github.com/tickless/pkg/auth"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	usersv1 "github.com/tickless/pkg/gen/tickless/users/v1"
	"github.com/tickless/pkg/outbox"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/seal"
)

// TopicUsers carries user lifecycle events.
const TopicUsers = "users.events"

// Domain errors, mapped to gRPC codes by the transport layer.
var (
	ErrInvalidEmail       = errors.New("identity: invalid email")
	ErrWeakPassword       = errors.New("identity: password must be 10-128 characters")
	ErrEmailTaken         = errors.New("identity: email already registered")
	ErrInvalidCredentials = errors.New("identity: invalid email or password")
	ErrEmailNotVerified   = errors.New("identity: email not verified")
	ErrFrozen             = errors.New("identity: account frozen")
	ErrInvalidToken       = errors.New("identity: invalid or expired token")
	ErrNotFound           = errors.New("identity: not found")
)

// Options configure a Service.
type Options struct {
	Signer     *auth.Signer
	Sealer     *seal.Sealer
	Argon      password.Params
	RefreshTTL time.Duration
	VerifyTTL  time.Duration
	// AutoVerify marks new accounts as verified immediately. DEVELOPMENT ONLY,
	// used until the notifier delivers verification mail (Phase 6).
	AutoVerify bool
	Now        func() time.Time
}

// Service is the identity use-case layer.
type Service struct {
	pool      *pgxpool.Pool
	o         Options
	dummyHash string // verified against when the user does not exist (timing equalisation)
}

// New creates a Service.
func New(pool *pgxpool.Pool, o Options) (*Service, error) {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.RefreshTTL == 0 {
		o.RefreshTTL = 30 * 24 * time.Hour
	}
	if o.VerifyTTL == 0 {
		o.VerifyTTL = 24 * time.Hour
	}
	if o.Argon == (password.Params{}) {
		o.Argon = password.Default
	}
	dummy, err := password.Hash("dummy-password-for-timing", o.Argon)
	if err != nil {
		return nil, err
	}
	return &Service{pool: pool, o: o, dummyHash: dummy}, nil
}

func normalizeEmail(e string) (string, error) {
	e = strings.ToLower(strings.TrimSpace(e))
	a, err := mail.ParseAddress(e)
	if err != nil || a.Address != e || len(e) > 254 || !strings.Contains(e, "@") {
		return "", ErrInvalidEmail
	}
	return e, nil
}

func randToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func hashToken(t string) []byte { h := sha256.Sum256([]byte(t)); return h[:] }

func eventMeta(ctx context.Context) *commonv1.EventMeta {
	return &commonv1.EventMeta{EventId: uuid.Must(uuid.NewV7()).String()}
}

// Signup registers an account and queues the verification email event.
func (s *Service) Signup(ctx context.Context, email, pw string) (int64, error) {
	email, err := normalizeEmail(email)
	if err != nil {
		return 0, err
	}
	if n := len(pw); n < 10 || n > 128 {
		return 0, ErrWeakPassword
	}
	hash, err := password.Hash(pw, s.o.Argon)
	if err != nil {
		return 0, err
	}
	token := randToken(32)
	var id int64
	err = pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		var verified *time.Time
		if s.o.AutoVerify {
			now := s.o.Now()
			verified = &now
		}
		if err := tx.QueryRow(ctx, `INSERT INTO users (email, email_verified_at) VALUES ($1, $2) RETURNING id`, email, verified).Scan(&id); err != nil {
			if pg.IsCode(err, pg.CodeUniqueViolation) {
				return ErrEmailTaken
			}
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO credentials (user_id, password_hash) VALUES ($1, $2)`, id, hash); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO email_verifications (token_hash, user_id, expires_at) VALUES ($1, $2, $3)`,
			hashToken(token), id, s.o.Now().Add(s.o.VerifyTTL)); err != nil {
			return err
		}
		msg, err := proto.Marshal(&usersv1.UserEvent{Meta: eventMeta(ctx), UserId: id,
			Event: &usersv1.UserEvent_SignedUp{SignedUp: &usersv1.UserSignedUp{Email: email, VerificationToken: token}}})
		if err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, TopicUsers, fmt.Sprint(id), msg)
	})
	return id, err
}

// VerifyEmail consumes a verification token.
func (s *Service) VerifyEmail(ctx context.Context, token string) error {
	return pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		var uid int64
		err := tx.QueryRow(ctx, `
			UPDATE email_verifications SET used_at = $2 WHERE token_hash = $1 AND used_at IS NULL AND expires_at > $2
			RETURNING user_id`, hashToken(token), s.o.Now()).Scan(&uid)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrInvalidToken
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE users SET email_verified_at = COALESCE(email_verified_at, $2) WHERE id = $1`, uid, s.o.Now())
		return err
	})
}

// Tokens is an access/refresh token pair.
type Tokens struct {
	Access         string
	AccessExpires  time.Time
	Refresh        string
	RefreshExpires time.Time
}

type userRow struct {
	id       int64
	role     string
	verified bool
	frozen   bool
}

// Login authenticates by password and issues tokens.
func (s *Service) Login(ctx context.Context, email, pw string) (Tokens, error) {
	email, err := normalizeEmail(email)
	if err != nil {
		_, _ = password.Verify(pw, s.dummyHash, s.o.Argon) // keep timing similar
		return Tokens{}, ErrInvalidCredentials
	}
	var u userRow
	var hash string
	err = s.pool.QueryRow(ctx, `
		SELECT u.id, u.role, u.email_verified_at IS NOT NULL, u.frozen, c.password_hash
		FROM users u JOIN credentials c ON c.user_id = u.id WHERE lower(u.email) = $1`, email).
		Scan(&u.id, &u.role, &u.verified, &u.frozen, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		_, _ = password.Verify(pw, s.dummyHash, s.o.Argon)
		return Tokens{}, ErrInvalidCredentials
	}
	if err != nil {
		return Tokens{}, err
	}
	rehash, err := password.Verify(pw, hash, s.o.Argon)
	if err != nil {
		return Tokens{}, ErrInvalidCredentials
	}
	if u.frozen {
		return Tokens{}, ErrFrozen
	}
	if !u.verified {
		return Tokens{}, ErrEmailNotVerified
	}
	if rehash {
		if nh, err := password.Hash(pw, s.o.Argon); err == nil {
			_, _ = s.pool.Exec(ctx, `UPDATE credentials SET password_hash = $2, updated_at = now() WHERE user_id = $1`, u.id, nh)
		}
	}
	return s.issue(ctx, nil, u)
}

// issue creates an access token and a fresh refresh token. tx may be nil.
func (s *Service) issue(ctx context.Context, tx pgx.Tx, u userRow) (Tokens, error) {
	now := s.o.Now()
	access, aexp, err := s.o.Signer.Issue(auth.Claims{UserID: u.id, Role: u.role, EmailVerified: u.verified}, now)
	if err != nil {
		return Tokens{}, err
	}
	refresh := randToken(32)
	rexp := now.Add(s.o.RefreshTTL)
	q := `INSERT INTO refresh_tokens (user_id, token_hash, expires_at) VALUES ($1, $2, $3)`
	if tx != nil {
		_, err = tx.Exec(ctx, q, u.id, hashToken(refresh), rexp)
	} else {
		_, err = s.pool.Exec(ctx, q, u.id, hashToken(refresh), rexp)
	}
	if err != nil {
		return Tokens{}, err
	}
	return Tokens{Access: access, AccessExpires: aexp, Refresh: refresh, RefreshExpires: rexp}, nil
}

// Refresh rotates a refresh token. Presenting an already used (revoked)
// token is treated as theft: all of the user's sessions are revoked.
func (s *Service) Refresh(ctx context.Context, refresh string) (Tokens, error) {
	var out Tokens
	var reuse bool
	err := pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		var uid int64
		var revoked *time.Time
		var exp time.Time
		err := tx.QueryRow(ctx, `SELECT user_id, revoked_at, expires_at FROM refresh_tokens WHERE token_hash = $1 FOR UPDATE`,
			hashToken(refresh)).Scan(&uid, &revoked, &exp)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrInvalidToken
		}
		if err != nil {
			return err
		}
		if revoked != nil {
			reuse = true
			return ErrInvalidToken
		}
		if !exp.After(s.o.Now()) {
			return ErrInvalidToken
		}
		var u userRow
		if err := tx.QueryRow(ctx, `SELECT id, role, email_verified_at IS NOT NULL, frozen FROM users WHERE id = $1`, uid).
			Scan(&u.id, &u.role, &u.verified, &u.frozen); err != nil {
			return err
		}
		if u.frozen {
			return ErrFrozen
		}
		if _, err := tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = $2 WHERE token_hash = $1`, hashToken(refresh), s.o.Now()); err != nil {
			return err
		}
		out, err = s.issue(ctx, tx, u)
		return err
	})
	if reuse {
		// Outside the failed transaction: revoke every active session of the token's owner.
		_, _ = s.pool.Exec(ctx, `
			UPDATE refresh_tokens SET revoked_at = now()
			WHERE revoked_at IS NULL AND user_id = (SELECT user_id FROM refresh_tokens WHERE token_hash = $1)`, hashToken(refresh))
	}
	return out, err
}

// Logout revokes a refresh token (idempotent).
func (s *Service) Logout(ctx context.Context, refresh string) error {
	_, err := s.pool.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = COALESCE(revoked_at, now()) WHERE token_hash = $1`, hashToken(refresh))
	return err
}

// APIKey is the public description of a key.
type APIKey struct {
	KeyID     string
	Label     string
	CanTrade  bool
	CreatedAt time.Time
	Revoked   bool
}

func apiKeyAAD(keyID string) []byte { return []byte("apikey:" + keyID) }

// CreateAPIKey issues an HMAC key; the secret is returned only once.
func (s *Service) CreateAPIKey(ctx context.Context, userID int64, label string, canTrade bool) (APIKey, string, error) {
	if len(label) > 64 {
		label = label[:64]
	}
	keyID := "tk_" + hex.EncodeToString(randBytes(8))
	secret := randToken(32)
	enc, err := s.o.Sealer.Seal([]byte(secret), apiKeyAAD(keyID))
	if err != nil {
		return APIKey{}, "", err
	}
	k := APIKey{KeyID: keyID, Label: label, CanTrade: canTrade}
	err = s.pool.QueryRow(ctx, `
		INSERT INTO api_keys (key_id, user_id, label, secret_enc, can_trade) VALUES ($1, $2, $3, $4, $5) RETURNING created_at`,
		keyID, userID, label, enc, canTrade).Scan(&k.CreatedAt)
	return k, secret, err
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

// ListAPIKeys lists a user's keys (never the secrets).
func (s *Service) ListAPIKeys(ctx context.Context, userID int64) ([]APIKey, error) {
	rows, err := s.pool.Query(ctx, `SELECT key_id, label, can_trade, created_at, revoked_at IS NOT NULL FROM api_keys WHERE user_id = $1 ORDER BY created_at`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []APIKey
	for rows.Next() {
		var k APIKey
		if err := rows.Scan(&k.KeyID, &k.Label, &k.CanTrade, &k.CreatedAt, &k.Revoked); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// RevokeAPIKey revokes one of the user's keys.
func (s *Service) RevokeAPIKey(ctx context.Context, userID int64, keyID string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now()) WHERE key_id = $1 AND user_id = $2`, keyID, userID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// KeyAuth is what the gateway needs to verify a signed request.
type KeyAuth struct {
	UserID   int64
	Secret   string
	CanTrade bool
	Role     string
}

// LookupAPIKey returns the decrypted secret of an active key of an active user.
func (s *Service) LookupAPIKey(ctx context.Context, keyID string) (KeyAuth, error) {
	var ka KeyAuth
	var enc []byte
	var revoked, frozen, verified bool
	err := s.pool.QueryRow(ctx, `
		SELECT k.user_id, k.secret_enc, k.can_trade, k.revoked_at IS NOT NULL, u.role, u.frozen, u.email_verified_at IS NOT NULL
		FROM api_keys k JOIN users u ON u.id = k.user_id WHERE k.key_id = $1`, keyID).
		Scan(&ka.UserID, &enc, &ka.CanTrade, &revoked, &ka.Role, &frozen, &verified)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && revoked) {
		return KeyAuth{}, ErrNotFound
	}
	if err != nil {
		return KeyAuth{}, err
	}
	if frozen {
		return KeyAuth{}, ErrFrozen
	}
	if !verified {
		return KeyAuth{}, ErrEmailNotVerified
	}
	sec, err := s.o.Sealer.Open(enc, apiKeyAAD(keyID))
	if err != nil {
		return KeyAuth{}, fmt.Errorf("identity: cannot decrypt api key %s: %w", keyID, err)
	}
	ka.Secret = string(sec)
	return ka, nil
}

// SetFrozen freezes or unfreezes an account, revoking sessions when freezing.
func (s *Service) SetFrozen(ctx context.Context, userID int64, frozen bool) error {
	return pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE users SET frozen = $2 WHERE id = $1`, userID, frozen)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		if frozen {
			if _, err := tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = now() WHERE user_id = $1 AND revoked_at IS NULL`, userID); err != nil {
				return err
			}
		}
		msg, err := proto.Marshal(&usersv1.UserEvent{Meta: eventMeta(ctx), UserId: userID,
			Event: &usersv1.UserEvent_Frozen{Frozen: &usersv1.UserFrozen{Frozen: frozen}}})
		if err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, TopicUsers, fmt.Sprint(userID), msg)
	})
}

// User is an administrative view of an account.
type User struct {
	ID        int64
	Email     string
	Role      string
	Verified  bool
	Frozen    bool
	CreatedAt time.Time
}

// ListUsers pages users by id.
func (s *Service) ListUsers(ctx context.Context, afterID int64, limit int) ([]User, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, email, role, email_verified_at IS NOT NULL, frozen, created_at FROM users WHERE id > $1 ORDER BY id LIMIT $2`, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Email, &u.Role, &u.Verified, &u.Frozen, &u.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// BootstrapAdmin creates a verified admin account if none exists with that
// email. Credentials come from the environment (never the repo).
func (s *Service) BootstrapAdmin(ctx context.Context, email, pw string) error {
	email, err := normalizeEmail(email)
	if err != nil {
		return err
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM users WHERE lower(email) = $1)`, email).Scan(&exists); err != nil || exists {
		return err
	}
	hash, err := password.Hash(pw, s.o.Argon)
	if err != nil {
		return err
	}
	return pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		var id int64
		if err := tx.QueryRow(ctx, `INSERT INTO users (email, role, email_verified_at) VALUES ($1, 'admin', now()) RETURNING id`, email).Scan(&id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO credentials (user_id, password_hash) VALUES ($1, $2)`, id, hash)
		return err
	})
}

// ListFrozen returns the ids of all frozen users.
func (s *Service) ListFrozen(ctx context.Context) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM users WHERE frozen ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
