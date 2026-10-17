// Package seal encrypts small secrets at rest (TOTP seeds, API-key secrets)
// with AES-256-GCM. The key comes from the environment, never from the repo.
package seal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
)

// Sealer encrypts and decrypts with one key.
type Sealer struct{ aead cipher.AEAD }

// ParseKey decodes a base64 32-byte key.
func ParseKey(b64 string) ([]byte, error) {
	k, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("seal: key is not base64: %w", err)
	}
	if len(k) != 32 {
		return nil, fmt.Errorf("seal: key must be 32 bytes, got %d", len(k))
	}
	return k, nil
}

// New returns a Sealer for a 32 byte key.
func New(key []byte) (*Sealer, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}
	return &Sealer{aead: g}, nil
}

// Seal returns nonce||ciphertext. aad binds the ciphertext to its context
// (e.g. the owning user id) so rows cannot be swapped.
func (s *Sealer) Seal(plain, aad []byte) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return s.aead.Seal(nonce, nonce, plain, aad), nil
}

// Open reverses Seal.
func (s *Sealer) Open(sealed, aad []byte) ([]byte, error) {
	n := s.aead.NonceSize()
	if len(sealed) < n {
		return nil, errors.New("seal: ciphertext too short")
	}
	return s.aead.Open(nil, sealed[:n], sealed[n:], aad)
}
