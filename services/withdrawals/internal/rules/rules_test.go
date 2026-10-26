package rules

import (
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/stretchr/testify/require"

	"github.com/tickless/pkg/money"
)

// Every transition in the table is accepted; every other pair is refused.
func TestStateMachineAllowsExactlyTheDeclaredEdges(t *testing.T) {
	all := []State{Requested, Screening, PendingReview, Approved, Signing, Broadcast, Confirmed, Rejected, Failed}
	for _, from := range all {
		for _, to := range all {
			err := Next(from, to)
			allowed := false
			for _, x := range transitions[from] {
				allowed = allowed || x == to
			}
			if allowed {
				require.NoError(t, err, "%s -> %s", from, to)
			} else {
				require.ErrorIs(t, err, ErrTransition, "%s -> %s", from, to)
			}
		}
	}
}

func TestFinalStatesHaveNoExit(t *testing.T) {
	for _, s := range []State{Confirmed, Rejected, Failed} {
		require.True(t, s.Final())
		require.Empty(t, transitions[s], "%s is final", s)
	}
	require.False(t, Approved.Final())
}

// Random walks over the machine never reach a state twice without a legal path,
// and never leave a final state.
func TestRandomWalksRespectTheMachine(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for run := 0; run < 2000; run++ {
		s := Requested
		for step := 0; step < 20 && !s.Final(); step++ {
			next := transitions[s]
			to := next[rng.IntN(len(next))]
			require.NoError(t, Next(s, to))
			s = to
		}
	}
}

func TestRouteSendsOnlyLargeAmountsToReview(t *testing.T) {
	threshold := money.FromInt64(1_000_000)
	require.Equal(t, Approved, Route(money.FromInt64(999_999), threshold))
	require.Equal(t, Approved, Route(money.FromInt64(1_000_000), threshold), "exactly the threshold executes automatically")
	require.Equal(t, PendingReview, Route(money.FromInt64(1_000_001), threshold))
}

func TestValidateBTCRegtestAddresses(t *testing.T) {
	p2wpkh, err := btcutil.NewAddressWitnessPubKeyHash(make([]byte, 20), &chaincfg.RegressionNetParams)
	require.NoError(t, err)
	valid := p2wpkh.EncodeAddress()
	require.True(t, strings.HasPrefix(valid, "bcrt1q"), valid)
	require.NoError(t, ValidateAddress("btc", valid))

	require.Error(t, ValidateAddress("btc", ""))
	require.Error(t, ValidateAddress("btc", valid[:len(valid)-1]+"q"), "one character changed breaks the bech32 checksum")
	require.Error(t, ValidateAddress("btc", "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"), "mainnet address refused")
	require.Error(t, ValidateAddress("btc", "2N8hwP1WmJrFF5QWABn38y63uYLhnJYJYTF"), "legacy p2sh refused")
}

func TestValidateETHAddresses(t *testing.T) {
	const eip55 = "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"
	require.NoError(t, ValidateAddress("eth", eip55))
	require.NoError(t, ValidateAddress("eth", "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed"), "all lower case has no checksum")
	require.NoError(t, ValidateAddress("eth", "0x5AAEB6053F3E94C9B9A09F33669435E7EF1BEAED"), "all upper case has no checksum either")
	require.Error(t, ValidateAddress("eth", "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAEd"), "one letter flipped breaks the EIP-55 checksum")
	require.Error(t, ValidateAddress("eth", "5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"), "missing 0x")
	require.Error(t, ValidateAddress("eth", "0x0000000000000000000000000000000000000000"), "zero address")
	require.Error(t, ValidateAddress("doge", eip55))
}

func TestDailyLimitCountsOnlyLiveRequestsAndIsExact(t *testing.T) {
	limit := money.FromInt64(100)
	counted := []money.Int128{money.FromInt64(40), money.FromInt64(50)}
	require.NoError(t, CheckDailyLimit(counted, money.FromInt64(10), limit), "90 + 10 = limit")
	require.ErrorIs(t, CheckDailyLimit(counted, money.FromInt64(11), limit), ErrDailyLimit)
	require.Error(t, CheckDailyLimit(counted, money.FromInt64(0), limit), "zero is refused")
	require.Error(t, CheckDailyLimit(nil, money.FromInt64(-1), limit), "negative is refused")

	require.True(t, CountsTowardLimit(Broadcast))
	require.True(t, CountsTowardLimit(PendingReview), "a request waiting for review holds its allowance")
	require.False(t, CountsTowardLimit(Rejected))
	require.False(t, CountsTowardLimit(Failed))
}

func TestDayIsTheUTCCalendarDay(t *testing.T) {
	// 23:30 at UTC+9 is 14:30 UTC on the same date: the same UTC day.
	jst := time.Date(2026, 10, 26, 23, 30, 0, 0, time.FixedZone("UTC+9", 9*3600))
	require.Equal(t, time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC), DayOf(jst))
	// 00:05 UTC belongs to the next day, whatever the local clock says.
	require.Equal(t, time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC), DayOf(time.Date(2026, 10, 27, 0, 5, 0, 0, time.UTC)))
	// 23:59 at UTC-5 is 04:59 UTC the next day.
	require.Equal(t, time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC), DayOf(time.Date(2026, 10, 26, 23, 59, 0, 0, time.FixedZone("UTC-5", -5*3600))))
}
