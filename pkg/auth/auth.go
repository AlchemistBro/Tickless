// Package auth provides short-lived Ed25519 JWT access tokens and HMAC
// request signing for API keys.
package auth

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Roles carried in tokens.
const (
	RoleUser  = "user"
	RoleAdmin = "admin"
)

// Claims are the application claims of an access token.
type Claims struct {
	UserID        int64
	Role          string
	EmailVerified bool
}

type tokenClaims struct {
	Role          string `json:"role"`
	EmailVerified bool   `json:"ev"`
	jwt.RegisteredClaims
}

// Signer issues access tokens. Only the identity service holds one.
type Signer struct {
	priv ed25519.PrivateKey
	TTL  time.Duration
}

// NewSigner builds a signer from a 32 byte seed; a nil seed generates a fresh
// random key (tokens then do not survive a restart, refresh tokens still do).
func NewSigner(seed []byte, ttl time.Duration) (*Signer, error) {
	if seed == nil {
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		return &Signer{priv: priv, TTL: ttl}, nil
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("auth: seed must be %d bytes", ed25519.SeedSize)
	}
	return &Signer{priv: ed25519.NewKeyFromSeed(seed), TTL: ttl}, nil
}

// PublicKey returns the verification key.
func (s *Signer) PublicKey() ed25519.PublicKey { return s.priv.Public().(ed25519.PublicKey) }

// Issue creates an access token for c.
func (s *Signer) Issue(c Claims, now time.Time) (string, time.Time, error) {
	exp := now.Add(s.TTL)
	t := jwt.NewWithClaims(jwt.SigningMethodEdDSA, tokenClaims{
		Role: c.Role, EmailVerified: c.EmailVerified,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: strconv.FormatInt(c.UserID, 10), IssuedAt: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp), Issuer: "tickless-identity",
		},
	})
	str, err := t.SignedString(s.priv)
	return str, exp, err
}

// Verifier validates access tokens with the public key only.
type Verifier struct{ pub ed25519.PublicKey }

// NewVerifier returns a Verifier for pub.
func NewVerifier(pub ed25519.PublicKey) *Verifier { return &Verifier{pub: pub} }

// ErrInvalidToken is returned for any token that fails validation.
var ErrInvalidToken = errors.New("auth: invalid token")

// Verify checks signature, algorithm, issuer and expiry.
func (v *Verifier) Verify(token string, now time.Time) (Claims, error) {
	var tc tokenClaims
	_, err := jwt.ParseWithClaims(token, &tc, func(*jwt.Token) (any, error) { return v.pub, nil },
		jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithIssuer("tickless-identity"),
		jwt.WithExpirationRequired(), jwt.WithTimeFunc(func() time.Time { return now }))
	if err != nil {
		return Claims{}, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	uid, err := strconv.ParseInt(tc.Subject, 10, 64)
	if err != nil || uid <= 0 {
		return Claims{}, ErrInvalidToken
	}
	return Claims{UserID: uid, Role: tc.Role, EmailVerified: tc.EmailVerified}, nil
}

// SignRequest computes the API-key signature:
//
//	hex(HMAC-SHA256(secret, ts "\n" METHOD "\n" path-with-query "\n" hex(SHA256(body))))
func SignRequest(secret string, ts int64, method, pathQuery string, body []byte) string {
	bh := sha256.Sum256(body)
	m := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(m, "%d\n%s\n%s\n%s", ts, method, pathQuery, hex.EncodeToString(bh[:]))
	return hex.EncodeToString(m.Sum(nil))
}

// ErrBadSignature is returned when an API-key signature does not verify or is stale.
var ErrBadSignature = errors.New("auth: bad signature")

// VerifyRequest checks the signature in constant time and rejects timestamps
// further than skew from now (replay window).
func VerifyRequest(secret string, ts int64, method, pathQuery string, body []byte, sig string, now time.Time, skew time.Duration) error {
	if d := now.Sub(time.Unix(ts, 0)); d > skew || d < -skew {
		return ErrBadSignature
	}
	want := SignRequest(secret, ts, method, pathQuery, body)
	if !hmac.Equal([]byte(want), []byte(sig)) {
		return ErrBadSignature
	}
	return nil
}
