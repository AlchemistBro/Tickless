package password_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/identity/internal/password"
)

var fast = password.Params{MemoryKiB: 8, Time: 1, Threads: 1}

func TestHashVerify(t *testing.T) {
	h, err := password.Hash("correct horse battery", fast)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(h, "$argon2id$v=19$m=8,t=1,p=1$"))

	rehash, err := password.Verify("correct horse battery", h, fast)
	require.NoError(t, err)
	assert.False(t, rehash)

	_, err = password.Verify("wrong", h, fast)
	assert.ErrorIs(t, err, password.ErrMismatch)
}

func TestHashesAreSalted(t *testing.T) {
	a, _ := password.Hash("same", fast)
	b, _ := password.Hash("same", fast)
	assert.NotEqual(t, a, b)
}

func TestNeedsRehashWhenParamsWeaker(t *testing.T) {
	h, _ := password.Hash("pw", fast)
	rehash, err := password.Verify("pw", h, password.Params{MemoryKiB: 16, Time: 1, Threads: 1})
	require.NoError(t, err)
	assert.True(t, rehash)
}

func TestVerifyRejectsMalformed(t *testing.T) {
	for _, bad := range []string{
		"", "plaintext", "$argon2i$v=19$m=8,t=1,p=1$AAAA$AAAA", "$argon2id$v=18$m=8,t=1,p=1$AAAA$AAAA",
		"$argon2id$v=19$m=x,t=1,p=1$AAAA$AAAA", "$argon2id$v=19$m=8,t=1,p=0$AAAA$AAAA",
		"$argon2id$v=19$m=4294967295,t=1,p=1$AAAA$AAAA", "$argon2id$v=19$m=8,t=1,p=1$!!$AAAA",
	} {
		_, err := password.Verify("pw", bad, fast)
		assert.Error(t, err, bad)
		assert.NotErrorIs(t, err, password.ErrMismatch, bad)
	}
}
