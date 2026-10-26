// Package totp implements RFC 6238 time-based one-time passwords (HMAC-SHA1,
// 6 digits, 30-second steps) as used by authenticator apps. Verification
// accepts one step either side of now to absorb clock drift, and a code is
// accepted at most once per step (the caller stores the last used step).
package totp

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // RFC 6238 mandates HMAC-SHA1; the HMAC construction is not broken by SHA-1 collisions
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// Period is the step length of RFC 6238.
const Period = 30 * time.Second

// Digits is the code length.
const Digits = 6

// Drift is how many steps either side of now a code may come from.
const Drift = 1

// ErrInvalid is returned for a code that does not match.
var ErrInvalid = errors.New("totp: invalid code")

// NewSecret returns a random 160-bit secret (the size RFC 4226 recommends).
func NewSecret() ([]byte, error) {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

// Encode renders a secret the way authenticator apps expect it (base32, no padding).
func Encode(secret []byte) string {
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(secret)
}

// Step returns the RFC 6238 counter for t.
func Step(t time.Time) int64 { return t.Unix() / int64(Period/time.Second) }

// Code computes the code for one counter value.
func Code(secret []byte, step int64) string {
	var msg [8]byte
	binary.BigEndian.PutUint64(msg[:], uint64(step)) //nolint:gosec // step is a non-negative counter
	mac := hmac.New(sha1.New, secret)
	_, _ = mac.Write(msg[:])
	sum := mac.Sum(nil)
	off := sum[len(sum)-1] & 0x0f
	bin := (uint32(sum[off])&0x7f)<<24 | uint32(sum[off+1])<<16 | uint32(sum[off+2])<<8 | uint32(sum[off+3])
	return fmt.Sprintf("%0*d", Digits, bin%1_000_000)
}

// Verify checks code against the steps around now and returns the matched
// step, which the caller must remember to refuse replays. A step at or below
// lastUsed is refused even if the code is right (replay protection).
func Verify(secret []byte, code string, now time.Time, lastUsed int64) (int64, error) {
	if len(code) != Digits {
		return 0, ErrInvalid
	}
	s := Step(now)
	for d := -Drift; d <= Drift; d++ {
		candidate := s + int64(d)
		if candidate <= lastUsed {
			continue
		}
		if hmac.Equal([]byte(Code(secret, candidate)), []byte(code)) {
			return candidate, nil
		}
	}
	return 0, ErrInvalid
}
