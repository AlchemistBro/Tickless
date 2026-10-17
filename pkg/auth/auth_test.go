package auth_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/pkg/auth"
)

func TestTokenRoundTrip(t *testing.T) {
	s, err := auth.NewSigner(nil, time.Minute)
	require.NoError(t, err)
	now := time.Now()
	tok, exp, err := s.Issue(auth.Claims{UserID: 42, Role: auth.RoleAdmin, EmailVerified: true}, now)
	require.NoError(t, err)
	assert.WithinDuration(t, now.Add(time.Minute), exp, time.Second)

	c, err := auth.NewVerifier(s.PublicKey()).Verify(tok, now)
	require.NoError(t, err)
	assert.Equal(t, auth.Claims{UserID: 42, Role: auth.RoleAdmin, EmailVerified: true}, c)
}

func TestTokenRejections(t *testing.T) {
	s, _ := auth.NewSigner(nil, time.Minute)
	other, _ := auth.NewSigner(nil, time.Minute)
	now := time.Now()
	tok, _, _ := s.Issue(auth.Claims{UserID: 1, Role: auth.RoleUser}, now)

	v := auth.NewVerifier(s.PublicKey())
	_, err := v.Verify(tok, now.Add(2*time.Minute))
	assert.ErrorIs(t, err, auth.ErrInvalidToken, "expired")

	_, err = auth.NewVerifier(other.PublicKey()).Verify(tok, now)
	assert.ErrorIs(t, err, auth.ErrInvalidToken, "wrong key")

	_, err = v.Verify(tok[:len(tok)-2]+"xx", now)
	assert.ErrorIs(t, err, auth.ErrInvalidToken, "tampered")

	_, err = v.Verify("not.a.jwt", now)
	assert.ErrorIs(t, err, auth.ErrInvalidToken)
	_, err = v.Verify("", now)
	assert.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestSignerFromSeedIsDeterministic(t *testing.T) {
	seed := make([]byte, 32)
	a, err := auth.NewSigner(seed, time.Minute)
	require.NoError(t, err)
	b, _ := auth.NewSigner(seed, time.Minute)
	assert.Equal(t, a.PublicKey(), b.PublicKey())
	_, err = auth.NewSigner([]byte("short"), time.Minute)
	assert.Error(t, err)
}

func TestRequestSigning(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	body := []byte(`{"market":"BTC-USDT"}`)
	sig := auth.SignRequest("s3cret", now.Unix(), "POST", "/v1/orders?x=1", body)

	assert.NoError(t, auth.VerifyRequest("s3cret", now.Unix(), "POST", "/v1/orders?x=1", body, sig, now, 30*time.Second))
	assert.ErrorIs(t, auth.VerifyRequest("other", now.Unix(), "POST", "/v1/orders?x=1", body, sig, now, 30*time.Second), auth.ErrBadSignature)
	assert.ErrorIs(t, auth.VerifyRequest("s3cret", now.Unix(), "GET", "/v1/orders?x=1", body, sig, now, 30*time.Second), auth.ErrBadSignature, "method is signed")
	assert.ErrorIs(t, auth.VerifyRequest("s3cret", now.Unix(), "POST", "/v1/orders?x=2", body, sig, now, 30*time.Second), auth.ErrBadSignature, "query is signed")
	assert.ErrorIs(t, auth.VerifyRequest("s3cret", now.Unix(), "POST", "/v1/orders?x=1", []byte("{}"), sig, now, 30*time.Second), auth.ErrBadSignature, "body is signed")
	assert.ErrorIs(t, auth.VerifyRequest("s3cret", now.Unix(), "POST", "/v1/orders?x=1", body, sig, now.Add(time.Minute), 30*time.Second), auth.ErrBadSignature, "replay window")
	assert.ErrorIs(t, auth.VerifyRequest("s3cret", now.Unix(), "POST", "/v1/orders?x=1", body, "", now, 30*time.Second), auth.ErrBadSignature)
}
