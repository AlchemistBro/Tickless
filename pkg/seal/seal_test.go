package seal_test

import (
	"crypto/rand"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/pkg/seal"
)

func newSealer(t *testing.T) *seal.Sealer {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	s, err := seal.New(key)
	require.NoError(t, err)
	return s
}

func TestSealRoundTripAndBinding(t *testing.T) {
	s := newSealer(t)
	ct, err := s.Seal([]byte("JBSWY3DPEHPK3PXP"), []byte("user:7"))
	require.NoError(t, err)
	assert.NotContains(t, string(ct), "JBSWY3DP")

	pt, err := s.Open(ct, []byte("user:7"))
	require.NoError(t, err)
	assert.Equal(t, "JBSWY3DPEHPK3PXP", string(pt))

	_, err = s.Open(ct, []byte("user:8"))
	assert.Error(t, err, "aad binds ciphertext to its owner")
	ct[len(ct)-1] ^= 1
	_, err = s.Open(ct, []byte("user:7"))
	assert.Error(t, err, "tampering detected")
	_, err = s.Open([]byte{1}, nil)
	assert.Error(t, err)
}

func TestSealNonceIsRandom(t *testing.T) {
	s := newSealer(t)
	a, _ := s.Seal([]byte("x"), nil)
	b, _ := s.Seal([]byte("x"), nil)
	assert.NotEqual(t, a, b)
}

func TestParseKey(t *testing.T) {
	_, err := seal.ParseKey("not base64!!")
	assert.Error(t, err)
	_, err = seal.ParseKey(base64.StdEncoding.EncodeToString([]byte("short")))
	assert.Error(t, err)
	_, err = seal.ParseKey(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	assert.NoError(t, err)
}
