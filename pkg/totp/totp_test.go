package totp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// RFC 6238 Appendix B vectors: secret "12345678901234567890", SHA-1.
// The RFC lists 8-digit codes; the 6-digit codes are their last six digits.
var rfcSecret = []byte("12345678901234567890")

func TestRFC6238Vectors(t *testing.T) {
	for _, v := range []struct {
		unix int64
		code string
	}{
		{59, "287082"},
		{1111111109, "081804"},
		{1111111111, "050471"},
		{1234567890, "005924"},
		{2000000000, "279037"},
		{20000000000, "353130"},
	} {
		require.Equal(t, v.code, Code(rfcSecret, Step(time.Unix(v.unix, 0))), "T=%d", v.unix)
	}
}

func TestVerifyAcceptsDriftAndRefusesReplay(t *testing.T) {
	now := time.Unix(1111111111, 0)
	s := Step(now)
	code := Code(rfcSecret, s)

	got, err := Verify(rfcSecret, code, now, 0)
	require.NoError(t, err)
	require.Equal(t, s, got)

	// The previous step's code is accepted within the drift window...
	prev := Code(rfcSecret, s-1)
	got, err = Verify(rfcSecret, prev, now, 0)
	require.NoError(t, err)
	require.Equal(t, s-1, got)

	// ...but two steps away is not.
	_, err = Verify(rfcSecret, Code(rfcSecret, s-2), now, 0)
	require.ErrorIs(t, err, ErrInvalid)

	// A code is accepted once: at or below the last used step it is refused.
	_, err = Verify(rfcSecret, code, now, s)
	require.ErrorIs(t, err, ErrInvalid, "replay of the same step")
	_, err = Verify(rfcSecret, prev, now, s)
	require.ErrorIs(t, err, ErrInvalid, "an older step after a newer one was used")
}

func TestVerifyRejectsMalformedAndWrongCodes(t *testing.T) {
	now := time.Unix(59, 0)
	for _, bad := range []string{"", "28708", "2870821", "abcdef", "000000"} {
		_, err := Verify(rfcSecret, bad, now, 0)
		require.ErrorIs(t, err, ErrInvalid, "%q", bad)
	}
}

func TestNewSecretIsRandomAndEncodes(t *testing.T) {
	a, err := NewSecret()
	require.NoError(t, err)
	b, err := NewSecret()
	require.NoError(t, err)
	require.Len(t, a, 20)
	require.NotEqual(t, a, b)
	require.Regexp(t, `^[A-Z2-7]{32}$`, Encode(a), "160 bits in base32 without padding")
}
