//go:build integration

package btc_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/chainwatcher/internal/btc"
	"github.com/tickless/chainwatcher/internal/scan"
	"github.com/tickless/chainwatcher/internal/store"
	"github.com/tickless/chainwatcher/migrations"
	chainwatcherv1 "github.com/tickless/pkg/gen/tickless/chainwatcher/v1"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/testkit"
)

type node struct {
	t      *testing.T
	ctx    context.Context
	rpc    *btc.Client // node-level calls
	miner  *btc.Client // wallet "miner"
	minerA string      // miner's address
	forkA  string      // coinbase address of alternative branches
}

func (n *node) call(c *btc.Client, out any, method string, params ...any) {
	n.t.Helper()
	require.NoError(n.t, c.Call(n.ctx, method, out, params...), method)
}

func (n *node) height() int64 {
	var h int64
	n.call(n.rpc, &h, "getblockcount")
	return h
}

// mine mines blocks including the mempool.
func (n *node) mine(k int) {
	n.call(n.rpc, nil, "generatetoaddress", k, n.minerA)
}

// mineEmpty mines k blocks that ignore the mempool (an alternative branch
// that does not contain pending transactions). Its coinbase pays another
// address: otherwise a block with the same parent, coinbase and second could
// hash exactly like an invalidated block and be rejected.
func (n *node) mineEmpty(k int) {
	for i := 0; i < k; i++ {
		n.call(n.rpc, nil, "generateblock", n.forkA, []string{})
	}
}

func startNode(t *testing.T) *node {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	url, user, pass := testkit.Bitcoind(t)
	n := &node{t: t, ctx: ctx, rpc: btc.NewClient(url, user, pass), miner: btc.NewClient(url+"/wallet/miner", user, pass)}
	n.call(n.rpc, nil, "createwallet", "miner")
	n.call(n.rpc, nil, "createwallet", "user")
	n.call(n.miner, &n.minerA, "getnewaddress", "", "bech32")
	n.call(n.miner, &n.forkA, "getnewaddress", "", "bech32")
	n.mine(101) // coinbase outputs mature after 100 blocks
	return n
}

func newStore(t *testing.T, ctx context.Context) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := testkit.Postgres(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "."))
	pool, err := pg.Connect(ctx, dsn, 10)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return store.New(pool), pool
}

func events(t *testing.T, pool *pgxpool.Pool, txid string) []chainwatcherv1.DepositEventType {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT payload FROM outbox ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()
	var out []chainwatcherv1.DepositEventType
	for rows.Next() {
		var b []byte
		require.NoError(t, rows.Scan(&b))
		var ev chainwatcherv1.DepositEvent
		require.NoError(t, proto.Unmarshal(b, &ev))
		if ev.Txid == txid {
			out = append(out, ev.Type)
		}
	}
	return out
}

func sync(t *testing.T, s *scan.Scanner) {
	t.Helper()
	for i := 0; i < 50; i++ {
		n, err := s.Step(context.Background())
		require.NoError(t, err)
		if n == 0 {
			return
		}
	}
	t.Fatal("scanner did not catch up")
}

// SPEC §6 invariant 5 and the phase 5 acceptance criterion: a forced 3-block
// reorg on regtest removes a not-yet-confirmed deposit; it is never credited
// and the system recovers by itself when the transaction is mined again.
func TestForcedThreeBlockReorgOnRegtestNeverCreditsTheRemovedDeposit(t *testing.T) {
	n := startNode(t)
	st, pool := newStore(t, n.ctx)

	userW := btc.NewClient(n.rpc.URL+"/wallet/user", n.rpc.User, n.rpc.Password)
	var depositAddr string
	n.call(userW, &depositAddr, "getnewaddress", "", "bech32")
	require.NoError(t, st.RegisterAddress(n.ctx, "btc", depositAddr, 1, 0))

	s := &scan.Scanner{Chain: btc.Chain{C: n.rpc}, Store: st.Chain("btc"), Name: "btc", Start: 1, Required: 2, MaxBatch: 500}
	sync(t, s)

	n.mine(2)
	var txid string
	n.call(n.miner, &txid, "sendtoaddress", depositAddr, "1.23456789")
	n.mine(1) // deposit block D = tip, 1 confirmation
	depositHeight := n.height()
	sync(t, s)
	ds, err := st.ListDeposits(n.ctx, 1, 0, 10)
	require.NoError(t, err)
	require.Len(t, ds, 1)
	require.Equal(t, store.StatusPending, ds[0].Status)
	require.EqualValues(t, 1, ds[0].Confirmations)
	require.Equal(t, "123456789", ds[0].Amount.String(), "exact satoshis")

	// Forced 3-block reorg: invalidate D-2 (removes D-2, D-1, D) and build a
	// longer branch of empty blocks that does not contain the transaction.
	var forkHash string
	n.call(n.rpc, &forkHash, "getblockhash", depositHeight-2)
	n.call(n.rpc, nil, "invalidateblock", forkHash)
	n.mineEmpty(4)
	require.Equal(t, depositHeight+1, n.height())
	sync(t, s)

	ds, err = st.ListDeposits(n.ctx, 1, 0, 10)
	require.NoError(t, err)
	require.Equal(t, store.StatusReorged, ds[0].Status)

	n.mineEmpty(5) // far past the threshold on the new branch
	sync(t, s)
	ds, err = st.ListDeposits(n.ctx, 1, 0, 10)
	require.NoError(t, err)
	require.Equal(t, store.StatusReorged, ds[0].Status)
	require.NotContains(t, events(t, pool, txid), chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_CONFIRMED,
		"a reorged deposit must never be credited")

	// Recovery: the sender's wallet broadcasts the transaction again (this
	// bitcoind does not return transactions disconnected by invalidateblock to
	// its mempool), and a normal block mines it on the new branch.
	var wtx struct {
		Hex string `json:"hex"`
	}
	n.call(n.miner, &wtx, "gettransaction", txid)
	n.call(n.rpc, nil, "sendrawtransaction", wtx.Hex)
	var mempool []string
	n.call(n.rpc, &mempool, "getrawmempool")
	require.Contains(t, mempool, txid)
	n.mine(1)
	sync(t, s)
	ds, err = st.ListDeposits(n.ctx, 1, 0, 10)
	require.NoError(t, err)
	require.Len(t, ds, 1, "the same (txid, vout) is the same deposit")
	require.Equal(t, store.StatusPending, ds[0].Status)
	n.mine(1)
	sync(t, s)
	ds, err = st.ListDeposits(n.ctx, 1, 0, 10)
	require.NoError(t, err)
	require.Equal(t, store.StatusConfirmed, ds[0].Status)

	n.mine(10)
	sync(t, s)
	require.Equal(t, []chainwatcherv1.DepositEventType{
		chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_DETECTED,
		chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_REORGED,
		chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_DETECTED,
		chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_CONFIRMED,
	}, events(t, pool, txid), "credited exactly once, after recovery")
}

// SPEC §6 invariant 4: a watcher restarted in the middle of the chain (a new
// scanner over the same store, re-reading blocks it already processed) never
// creates or credits a deposit twice.
func TestRestartedWatcherNeverDuplicatesDeposits(t *testing.T) {
	n := startNode(t)
	st, pool := newStore(t, n.ctx)
	userW := btc.NewClient(n.rpc.URL+"/wallet/user", n.rpc.User, n.rpc.Password)
	var addr string
	n.call(userW, &addr, "getnewaddress", "", "bech32")
	require.NoError(t, st.RegisterAddress(n.ctx, "btc", addr, 1, 0))

	var txids []string
	for i := 0; i < 3; i++ {
		var txid string
		n.call(n.miner, &txid, "sendtoaddress", addr, "0.5")
		txids = append(txids, txid)
		n.mine(1)
	}
	n.mine(3)
	for round := 0; round < 3; round++ { // three "restarts", each a fresh scanner
		s := &scan.Scanner{Chain: btc.Chain{C: n.rpc}, Store: st.Chain("btc"), Name: "btc", Start: 1, Required: 2, MaxBatch: 7}
		sync(t, s)
	}
	ds, err := st.ListDeposits(n.ctx, 1, 0, 50)
	require.NoError(t, err)
	require.Len(t, ds, 3)
	for _, txid := range txids {
		require.Equal(t, []chainwatcherv1.DepositEventType{
			chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_DETECTED,
			chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_CONFIRMED,
		}, events(t, pool, txid))
	}
}
