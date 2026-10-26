//go:build integration

package store_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tickless/ledger/internal/store"
	"github.com/tickless/pkg/money"
)

func holdBalance(t *testing.T, st *store.Store, ctx context.Context, user int64, asset string) (string, string) {
	t.Helper()
	bs, err := st.Balances(ctx, user)
	require.NoError(t, err)
	for _, b := range bs {
		if b.Asset == asset {
			return b.Available.String(), b.Held.String()
		}
	}
	return "0", "0"
}

func TestWithdrawalHoldIsIdempotentAndRefusesInsufficientFunds(t *testing.T) {
	st, pool, ctx := newStore(t)
	_, _, err := st.Deposit(ctx, "btc", "d1", 0, 5, "BTC", money.FromInt64(1000))
	require.NoError(t, err)

	already, err := st.HoldWithdrawal(ctx, 1, 5, "BTC", money.FromInt64(600))
	require.NoError(t, err)
	require.False(t, already)
	already, err = st.HoldWithdrawal(ctx, 1, 5, "BTC", money.FromInt64(600))
	require.NoError(t, err)
	require.True(t, already, "a retried hold is recognised, not applied twice")
	avail, held := holdBalance(t, st, ctx, 5, "BTC")
	require.Equal(t, "400", avail)
	require.Equal(t, "600", held)

	_, err = st.HoldWithdrawal(ctx, 1, 5, "BTC", money.FromInt64(601))
	require.Error(t, err, "the same withdrawal id with other terms is refused")

	_, err = st.HoldWithdrawal(ctx, 2, 5, "BTC", money.FromInt64(401))
	require.ErrorIs(t, err, store.ErrInsufficientFunds)
	var n int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM withdrawal_holds WHERE withdrawal_id = 2`).Scan(&n))
	require.Zero(t, n, "a refused hold leaves no trace")
	avail, held = holdBalance(t, st, ctx, 5, "BTC")
	require.Equal(t, "400", avail)
	require.Equal(t, "600", held)
}

func TestSettleMovesFundsOutAndReleaseReturnsThem(t *testing.T) {
	st, pool, ctx := newStore(t)
	_, _, err := st.Deposit(ctx, "eth", "d", -1, 6, "ETH", money.FromInt64(1000))
	require.NoError(t, err)
	_, err = st.HoldWithdrawal(ctx, 10, 6, "ETH", money.FromInt64(300))
	require.NoError(t, err)
	_, err = st.HoldWithdrawal(ctx, 11, 6, "ETH", money.FromInt64(200))
	require.NoError(t, err)

	already, err := st.SettleWithdrawal(ctx, 10)
	require.NoError(t, err)
	require.False(t, already)
	already, err = st.SettleWithdrawal(ctx, 10)
	require.NoError(t, err)
	require.True(t, already)
	_, err = st.ReleaseWithdrawal(ctx, 10)
	require.ErrorIs(t, err, store.ErrWithdrawalFinished, "a settled hold can never be released")

	_, err = st.ReleaseWithdrawal(ctx, 11)
	require.NoError(t, err)
	_, err = st.SettleWithdrawal(ctx, 11)
	require.ErrorIs(t, err, store.ErrWithdrawalFinished, "a released hold can never be settled")
	_, err = st.SettleWithdrawal(ctx, 999)
	require.ErrorIs(t, err, store.ErrWithdrawalNotFound)

	avail, held := holdBalance(t, st, ctx, 6, "ETH")
	require.Equal(t, "700", avail, "1000 deposited - 300 withdrawn")
	require.Equal(t, "0", held)

	var bad int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ledger_verify()`).Scan(&bad))
	require.Zero(t, bad)
	var ext string
	require.NoError(t, pool.QueryRow(ctx, `SELECT b.amount::text FROM balances b JOIN accounts a ON a.id = b.account_id WHERE a.user_id = 0 AND a.asset = 'ETH' AND a.kind = 'EXTERNAL'`).Scan(&ext))
	require.Equal(t, "-700", ext, "EXTERNAL mirrors deposits minus withdrawals")
}

// Settle and release racing on one hold: exactly one of them takes effect.
func TestConcurrentSettleAndReleaseHaveOneWinner(t *testing.T) {
	st, _, ctx := newStore(t)
	_, _, err := st.Deposit(ctx, "btc", "d2", 0, 7, "BTC", money.FromInt64(5000))
	require.NoError(t, err)
	for i := int64(100); i < 110; i++ {
		_, err := st.HoldWithdrawal(ctx, i, 7, "BTC", money.FromInt64(100))
		require.NoError(t, err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	settled, released := 0, 0
	for i := int64(100); i < 110; i++ {
		for _, settle := range []bool{true, false} {
			wg.Add(1)
			go func(id int64, settle bool) {
				defer wg.Done()
				var already bool
				var err error
				if settle {
					already, err = st.SettleWithdrawal(context.Background(), id)
				} else {
					already, err = st.ReleaseWithdrawal(context.Background(), id)
				}
				mu.Lock()
				defer mu.Unlock()
				if err == nil && !already {
					if settle {
						settled++
					} else {
						released++
					}
				} else if err != nil {
					require.ErrorIs(t, err, store.ErrWithdrawalFinished)
				}
			}(i, settle)
		}
	}
	wg.Wait()
	require.Equal(t, 10, settled+released, "each hold ended exactly once")
	avail, held := holdBalance(t, st, ctx, 7, "BTC")
	require.Equal(t, "0", held)
	want := money.FromInt64(4000 + int64(released)*100)
	require.Equal(t, want.String(), avail)
}
