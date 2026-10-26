//go:build integration

package store_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	withdrawalsv1 "github.com/tickless/pkg/gen/tickless/withdrawals/v1"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/testkit"
	"github.com/tickless/withdrawals/internal/rules"
	"github.com/tickless/withdrawals/internal/store"
	"github.com/tickless/withdrawals/migrations"
)

// btcDest is a valid regtest p2wpkh address (checksum computed, not copied).
var btcDest = func() string {
	a, err := btcutil.NewAddressWitnessPubKeyHash(make([]byte, 20), &chaincfg.RegressionNetParams)
	if err != nil {
		panic(err)
	}
	return a.EncodeAddress()
}()

func newDB(t *testing.T) (*store.Store, *pgxpool.Pool, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	dsn := testkit.Postgres(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "."))
	pool, err := pg.Connect(ctx, dsn, 30)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return store.New(pool), pool, ctx
}

var now = time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)

// Concurrent requests for one user and asset are serialised: the total of the
// accepted ones never exceeds the daily limit, however they interleave.
func TestConcurrentRequestsNeverExceedTheDailyLimit(t *testing.T) {
	st, _, ctx := newDB(t)
	limit := money.FromInt64(100)
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := money.FromInt64(0)
	count := 0
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := st.Request(ctx, 7, "btc", "BTC", money.FromInt64(15), btcDest, limit, now)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				count++
				accepted, _ = accepted.Add(money.FromInt64(15))
			} else {
				require.ErrorIs(t, err, rules.ErrDailyLimit)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 6, count, "six 15s fit into 100")
	require.Zero(t, accepted.Cmp(money.FromInt64(90)))
}

// The daily limit is per asset: BTC and USDT allowances are independent.
func TestLimitIsPerAssetAndPerUTCDay(t *testing.T) {
	st, _, ctx := newDB(t)
	limit := money.FromInt64(100)
	_, err := st.Request(ctx, 1, "btc", "BTC", money.FromInt64(100), btcDest, limit, now)
	require.NoError(t, err)
	_, err = st.Request(ctx, 1, "btc", "BTC", money.FromInt64(1), btcDest, limit, now)
	require.ErrorIs(t, err, rules.ErrDailyLimit)
	_, err = st.Request(ctx, 1, "eth", "USDT", money.FromInt64(100), "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed", limit, now)
	require.NoError(t, err, "another asset has its own allowance")
	_, err = st.Request(ctx, 1, "btc", "BTC", money.FromInt64(100), btcDest, limit, now.Add(24*time.Hour))
	require.NoError(t, err, "the next UTC day starts with a fresh allowance")
}

// A rejected request releases its allowance at once.
func TestRejectedWithdrawalReleasesItsAllowance(t *testing.T) {
	st, _, ctx := newDB(t)
	limit := money.FromInt64(100)
	w, err := st.Request(ctx, 2, "btc", "BTC", money.FromInt64(100), btcDest, limit, now)
	require.NoError(t, err)
	require.NoError(t, st.Transition(ctx, w.ID, rules.Requested, rules.Screening, "", "", now))
	require.NoError(t, st.Transition(ctx, w.ID, rules.Screening, rules.Rejected, "denylisted", "", now))
	_, err = st.Request(ctx, 2, "btc", "BTC", money.FromInt64(100), btcDest, limit, now)
	require.NoError(t, err)
}

// A transition based on a stale state fails; the state is not overwritten.
func TestStaleTransitionIsRefused(t *testing.T) {
	st, _, ctx := newDB(t)
	w, err := st.Request(ctx, 3, "btc", "BTC", money.FromInt64(5), btcDest, money.FromInt64(100), now)
	require.NoError(t, err)
	require.NoError(t, st.Transition(ctx, w.ID, rules.Requested, rules.Screening, "", "", now))

	// Two actors both believe the withdrawal is still REQUESTED.
	err = st.Transition(ctx, w.ID, rules.Requested, rules.Screening, "", "", now)
	require.ErrorIs(t, err, store.ErrStale)
	got, err := st.Get(ctx, w.ID)
	require.NoError(t, err)
	require.Equal(t, rules.Screening, got.State)
}

// Concurrent transitions from the same state: exactly one wins.
func TestConcurrentTransitionHasExactlyOneWinner(t *testing.T) {
	st, _, ctx := newDB(t)
	w, err := st.Request(ctx, 4, "eth", "ETH", money.FromInt64(5), "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed", money.FromInt64(100), now)
	require.NoError(t, err)
	require.NoError(t, st.Transition(ctx, w.ID, rules.Requested, rules.Screening, "", "", now))
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- st.Transition(ctx, w.ID, rules.Screening, rules.Approved, "", "", now)
		}()
	}
	wg.Wait()
	close(errs)
	wins := 0
	for err := range errs {
		if err == nil {
			wins++
		} else {
			require.ErrorIs(t, err, store.ErrStale)
		}
	}
	require.Equal(t, 1, wins)
}

// Edges that do not exist are refused before touching the database.
func TestInvalidEdgeIsRefused(t *testing.T) {
	st, _, ctx := newDB(t)
	w, err := st.Request(ctx, 5, "btc", "BTC", money.FromInt64(5), btcDest, money.FromInt64(100), now)
	require.NoError(t, err)
	err = st.Transition(ctx, w.ID, rules.Requested, rules.Confirmed, "", "", now)
	require.ErrorIs(t, err, rules.ErrTransition)
	err = st.Transition(ctx, 999999, rules.Requested, rules.Screening, "", "", now)
	require.Error(t, err)
}

// Every transition, including the request, is an outbox event keyed by user id.
func TestEveryTransitionIsAnOutboxEvent(t *testing.T) {
	st, pool, ctx := newDB(t)
	w, err := st.Request(ctx, 9, "btc", "BTC", money.FromInt64(42), btcDest, money.FromInt64(100), now)
	require.NoError(t, err)
	require.NoError(t, st.Transition(ctx, w.ID, rules.Requested, rules.Screening, "", "", now))
	require.NoError(t, st.Transition(ctx, w.ID, rules.Screening, rules.Approved, "", "", now))

	rows, err := pool.Query(ctx, `SELECT payload, key, topic FROM outbox ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()
	var states []string
	for rows.Next() {
		var b []byte
		var key, topic string
		require.NoError(t, rows.Scan(&b, &key, &topic))
		require.Equal(t, store.TopicEvents, topic)
		require.Equal(t, "9", key)
		var ev withdrawalsv1.WithdrawalEvent
		require.NoError(t, proto.Unmarshal(b, &ev))
		require.Equal(t, w.ID, ev.WithdrawalId)
		require.Equal(t, "42", ev.Amount.Units)
		states = append(states, ev.FromState+">"+ev.ToState)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"REQUESTED>REQUESTED", "REQUESTED>SCREENING", "SCREENING>APPROVED"}, states)
}

func TestRejectsBadDestinationBeforeLimitCheck(t *testing.T) {
	st, _, ctx := newDB(t)
	_, err := st.Request(ctx, 6, "btc", "BTC", money.FromInt64(5), "not-an-address", money.FromInt64(100), now)
	require.ErrorIs(t, err, rules.ErrAddress)
}
