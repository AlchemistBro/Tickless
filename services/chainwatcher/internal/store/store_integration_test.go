//go:build integration

package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/chainwatcher/internal/scan"
	"github.com/tickless/chainwatcher/internal/scan/scantest"
	"github.com/tickless/chainwatcher/internal/store"
	"github.com/tickless/chainwatcher/migrations"
	chainwatcherv1 "github.com/tickless/pkg/gen/tickless/chainwatcher/v1"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/testkit"
)

func newDB(t *testing.T) (*store.Store, *pgxpool.Pool, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	dsn := testkit.Postgres(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "."))
	pool, err := pg.Connect(ctx, dsn, 10)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return store.New(pool), pool, ctx
}

// inspector reads a chain's state straight from the tables; credits are the
// CONFIRMED events actually enqueued in the outbox.
type inspector struct {
	pool  *pgxpool.Pool
	chain string
}

func (in inspector) events(t *testing.T) []*chainwatcherv1.DepositEvent {
	t.Helper()
	rows, err := in.pool.Query(context.Background(), `SELECT payload FROM outbox WHERE topic = $1 ORDER BY id`, store.TopicEvents)
	require.NoError(t, err)
	defer rows.Close()
	var out []*chainwatcherv1.DepositEvent
	for rows.Next() {
		var b []byte
		require.NoError(t, rows.Scan(&b))
		var ev chainwatcherv1.DepositEvent
		require.NoError(t, proto.Unmarshal(b, &ev))
		if ev.Chain == in.chain {
			out = append(out, &ev)
		}
	}
	require.NoError(t, rows.Err())
	return out
}

func (in inspector) Credits(t *testing.T) map[scantest.Key]int {
	out := map[scantest.Key]int{}
	for _, ev := range in.events(t) {
		if ev.Type == chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_CONFIRMED {
			out[scantest.Key{TxID: ev.Txid, Index: ev.Index}]++
		}
	}
	return out
}

func (in inspector) Status(t *testing.T, k scantest.Key) string {
	var s string
	err := in.pool.QueryRow(context.Background(), `SELECT status FROM deposits WHERE chain = $1 AND txid = $2 AND idx = $3`, in.chain, k.TxID, k.Index).Scan(&s)
	if err != nil {
		return ""
	}
	return s
}

func (in inspector) Deposits(t *testing.T) map[scantest.Key]int {
	rows, err := in.pool.Query(context.Background(), `SELECT txid, idx, count(*) FROM deposits WHERE chain = $1 GROUP BY txid, idx`, in.chain)
	require.NoError(t, err)
	defer rows.Close()
	out := map[scantest.Key]int{}
	for rows.Next() {
		var k scantest.Key
		var n int
		require.NoError(t, rows.Scan(&k.TxID, &k.Index, &n))
		out[k] = n
	}
	require.NoError(t, rows.Err())
	return out
}

func (in inspector) MaxHeight(t *testing.T) int64 {
	var h int64
	require.NoError(t, in.pool.QueryRow(context.Background(), `SELECT COALESCE(max(height), -1) FROM blocks WHERE chain = $1`, in.chain).Scan(&h))
	return h
}

// The PostgreSQL store passes the same random histories as the reference store
// (SPEC §6 invariants 4 and 5), including failures after COMMIT. Each history
// uses its own chain name, so one database serves them all.
func TestPostgresStoreKeepsInvariants4And5OverRandomHistories(t *testing.T) {
	st, pool, ctx := newDB(t)
	n := 0
	scantest.RunRandomHistories(t, 40, 80, func(t *testing.T, watched []string) scantest.Harness {
		n++
		chain := fmt.Sprintf("chain%d", n)
		for i, a := range watched {
			require.NoError(t, st.RegisterAddress(ctx, chain, a, int64(i+1), uint32(i)))
		}
		return scantest.Harness{Store: st.Chain(chain), Inspect: inspector{pool: pool, chain: chain}}
	})
}

func TestRegisterAddressIsIdempotentAndOwned(t *testing.T) {
	st, _, ctx := newDB(t)
	addr := "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
	require.NoError(t, st.RegisterAddress(ctx, "eth", addr, 7, 3))
	require.NoError(t, st.RegisterAddress(ctx, "eth", addr, 7, 3), "same owner again is a no-op")
	require.ErrorIs(t, st.RegisterAddress(ctx, "eth", "0xabcdef0123456789abcdef0123456789abcdef01", 8, 0), store.ErrAddressTaken,
		"Ethereum addresses are case-insensitive: the lower-case form is the same address")
	require.NoError(t, st.RegisterAddress(ctx, "btc", "bcrt1qexample", 8, 0), "another chain is another namespace")
}

func TestDepositLifecycleEventsAndQueries(t *testing.T) {
	st, pool, ctx := newDB(t)
	addr := "0xAbCdEf0123456789aBcDeF0123456789AbCdEf01"
	require.NoError(t, st.RegisterAddress(ctx, "eth", addr, 42, 0))
	cs := st.Chain("eth")
	amount := money.FromInt64(1_500_000)

	// The chain reports addresses in lower case; matching is case-insensitive.
	require.NoError(t, cs.ApplyBlock(ctx, scan.Block{Height: 10, Hash: "h10", Parent: "h9", Transfers: []scan.Transfer{
		{TxID: "0xt1", Index: 4, Address: "0xabcdef0123456789abcdef0123456789abcdef01", Asset: "USDT", Amount: amount},
		{TxID: "0xt2", Index: 0, Address: "0x0000000000000000000000000000000000000001", Asset: "ETH", Amount: amount}, // not ours
		{TxID: "0xt3", Index: 0, Address: addr, Asset: "ETH", Amount: money.FromInt64(0)},                             // zero value
	}}))
	require.NoError(t, cs.Confirm(ctx, 11, 12))
	ds, err := st.ListDeposits(ctx, 42, 0, 10)
	require.NoError(t, err)
	require.Len(t, ds, 1)
	require.Equal(t, store.StatusPending, ds[0].Status)
	require.EqualValues(t, 2, ds[0].Confirmations)
	require.Equal(t, "USDT", ds[0].Asset)
	require.Zero(t, ds[0].Amount.Cmp(amount))

	require.NoError(t, cs.Confirm(ctx, 21, 12))
	ds, err = st.ListDeposits(ctx, 42, 0, 10)
	require.NoError(t, err)
	require.Equal(t, store.StatusConfirmed, ds[0].Status)
	require.NotNil(t, ds[0].ConfirmedAt)

	evs := inspector{pool: pool, chain: "eth"}.events(t)
	require.Len(t, evs, 2)
	require.Equal(t, chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_DETECTED, evs[0].Type)
	c := evs[1]
	require.Equal(t, chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_CONFIRMED, c.Type)
	require.Equal(t, int64(42), c.UserId)
	require.Equal(t, "0xt1", c.Txid)
	require.EqualValues(t, 4, c.Index)
	require.Equal(t, "1500000", c.Amount.Units)
	require.EqualValues(t, 12, c.Confirmations)
	require.NotEmpty(t, c.Meta.EventId)

	var key string
	require.NoError(t, pool.QueryRow(ctx, `SELECT key FROM outbox ORDER BY id DESC LIMIT 1`).Scan(&key))
	require.Equal(t, "42", key, "deposits.events is keyed by user id")

	// A CONFIRMED deposit is final, even for a direct UPDATE.
	_, err = pool.Exec(ctx, `UPDATE deposits SET status = 'REORGED' WHERE txid = '0xt1'`)
	require.Error(t, err)
	require.True(t, pg.IsCode(err, "23514"), "%v", err)

	// A block cannot be applied twice.
	require.Error(t, cs.ApplyBlock(ctx, scan.Block{Height: 10, Hash: "h10", Parent: "h9"}))
}
