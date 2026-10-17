// Package password hashes and verifies passwords with argon2id and encodes
// the result as a PHC string so parameters can be raised over time.
package password

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Params are the argon2id cost parameters.
type Params struct {
	MemoryKiB uint32
	Time      uint32
	Threads   uint8
}

// Default follows the OWASP minimum recommendation for argon2id
// (19 MiB, 2 iterations, 1 lane).
var Default = Params{MemoryKiB: 19 * 1024, Time: 2, Threads: 1}

const (
	saltLen = 16
	keyLen  = 32
)

// ErrMismatch is returned by Verify for a wrong password.
var ErrMismatch = errors.New("password: mismatch")

var b64 = base64.RawStdEncoding

// Hash returns the PHC encoding of the argon2id hash of pw.
func Hash(pw string, p Params) (string, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := argon2.IDKey([]byte(pw), salt, p.Time, p.MemoryKiB, p.Threads, keyLen)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, p.MemoryKiB, p.Time, p.Threads, b64.EncodeToString(salt), b64.EncodeToString(key)), nil
}

// Verify checks pw against an encoded hash in constant time. The returned
// bool reports whether the stored parameters are weaker than p (rehash).
func Verify(pw, encoded string, p Params) (needsRehash bool, err error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false, errors.New("password: unsupported hash format")
	}
	var ver int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &ver); err != nil || ver != argon2.Version {
		return false, errors.New("password: unsupported argon2 version")
	}
	var sp Params
	var threads uint
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &sp.MemoryKiB, &sp.Time, &threads); err != nil || threads == 0 || threads > 255 {
		return false, errors.New("password: bad parameters")
	}
	sp.Threads = uint8(threads)
	// Bound attacker-influenced cost (the hash comes from our DB, but be strict anyway).
	if sp.MemoryKiB > 1<<20 || sp.Time > 16 {
		return false, errors.New("password: parameters out of range")
	}
	salt, err := b64.DecodeString(parts[4])
	if err != nil {
		return false, err
	}
	want, err := b64.DecodeString(parts[5])
	if err != nil || len(want) == 0 {
		return false, errors.New("password: bad hash")
	}
	got := argon2.IDKey([]byte(pw), salt, sp.Time, sp.MemoryKiB, sp.Threads, uint32(len(want)))
	if subtle.ConstantTimeCompare(got, want) != 1 {
		return false, ErrMismatch
	}
	return sp.MemoryKiB < p.MemoryKiB || sp.Time < p.Time, nil
}
