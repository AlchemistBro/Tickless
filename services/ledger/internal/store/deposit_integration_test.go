//go:build integration

package store_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tickless/ledger/internal/store"
	"github.com/tickless/pkg/money"
)

// SPEC §6 invariant 4 at the ledger: the same deposit (chain, txid, index)
// delivered many times, also concurrently, is credited exactly once.
func TestDepositIsCreditedOncePerChainTxidIndex(t *testing.T) {
	st, pool, ctx := newStore(t)
	amt := money.FromInt64(123_456_789)

	type result struct {
		id      int64
		already bool
		err     error
	}
	results := make(chan result, 20)
	for i := 0; i < 20; i++ { // more concurrent sessions than pool connections, all missing the account cache
		go func() {
			id, already, err := st.Deposit(ctx, "btc", "abc", 1, 9, "BTC", amt)
			results <- result{id, already, err}
		}()
	}
	applied := 0
	for i := 0; i < 20; i++ {
		r := <-results
		require.NoError(t, r.err)
		if !r.already {
			applied++
			require.Positive(t, r.id)
		}
	}
	require.Equal(t, 1, applied, "exactly one delivery is applied, the rest are recognised as repeats")
	var entries int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM journal_entries WHERE business_key = $1`, store.DepositKey("btc", "abc", 1)).Scan(&entries))
	require.Equal(t, 1, entries)

	bs, err := st.Balances(ctx, 9)
	require.NoError(t, err)
	require.Len(t, bs, 1)
	require.Equal(t, "BTC", bs[0].Asset)
	require.Equal(t, "123456789", bs[0].Available.String())

	// Another output of the same transaction is another deposit.
	_, already, err := st.Deposit(ctx, "btc", "abc", 2, 9, "BTC", money.FromInt64(1))
	require.NoError(t, err)
	require.False(t, already)

	var kind string
	require.NoError(t, pool.QueryRow(ctx, `SELECT kind FROM journal_entries WHERE business_key = $1`, store.DepositKey("btc", "abc", 1)).Scan(&kind))
	require.Equal(t, "deposit", kind)
	var bad int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ledger_verify()`).Scan(&bad))
	require.Zero(t, bad)
}

func TestDepositRejectsUnknownAssetsAndNonPositiveAmounts(t *testing.T) {
	st, _, ctx := newStore(t)
	_, _, err := st.Deposit(ctx, "eth", "0x1", 0, 1, "DOGE", money.FromInt64(1))
	require.ErrorIs(t, err, store.ErrInvalidAmount)
	_, _, err = st.Deposit(ctx, "eth", "0x1", 0, 1, "ETH", money.FromInt64(0))
	require.ErrorIs(t, err, store.ErrInvalidAmount)
	_, _, err = st.Deposit(ctx, "eth", "0x1", 0, 1, "ETH", money.FromInt64(-5))
	require.ErrorIs(t, err, store.ErrInvalidAmount)
}
