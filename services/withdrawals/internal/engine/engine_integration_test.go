//go:build integration

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/stretchr/testify/require"

	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/testkit"
	"github.com/tickless/withdrawals/internal/engine"
	"github.com/tickless/withdrawals/internal/rules"
	"github.com/tickless/withdrawals/internal/store"
	"github.com/tickless/withdrawals/migrations"
)

// ---------------------------------------------------------------- fakes

type hold struct {
	user   int64
	asset  string
	amount int64
	status string // ACTIVE SETTLED RELEASED
}

type fakeLedger struct {
	mu         sync.Mutex
	avail      map[string]int64 // "user/asset"
	held       map[string]int64
	holds      map[int64]*hold
	failSettle int
}

func newLedger() *fakeLedger {
	return &fakeLedger{avail: map[string]int64{}, held: map[string]int64{}, holds: map[int64]*hold{}}
}

func key(u int64, a string) string { return fmt.Sprintf("%d/%s", u, a) }

func (l *fakeLedger) fund(u int64, a string, v int64) {
	l.mu.Lock()
	l.avail[key(u, a)] += v
	l.mu.Unlock()
}

func (l *fakeLedger) HoldWithdrawal(_ context.Context, id, user int64, asset string, amount money.Int128) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.holds[id]; ok {
		return nil
	}
	v, _ := amount.Int64()
	if l.avail[key(user, asset)] < v {
		return engine.ErrInsufficientFunds
	}
	l.avail[key(user, asset)] -= v
	l.held[key(user, asset)] += v
	l.holds[id] = &hold{user, asset, v, "ACTIVE"}
	return nil
}

func (l *fakeLedger) finish(id int64, to string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	h, ok := l.holds[id]
	if !ok {
		return engine.ErrNoHold
	}
	if h.status == to {
		return nil
	}
	if h.status != "ACTIVE" {
		return fmt.Errorf("hold %d already %s", id, h.status)
	}
	h.status = to
	l.held[key(h.user, h.asset)] -= h.amount
	if to == "RELEASED" {
		l.avail[key(h.user, h.asset)] += h.amount
	}
	return nil
}

func (l *fakeLedger) SettleWithdrawal(_ context.Context, id int64) error {
	l.mu.Lock()
	if l.failSettle > 0 {
		l.failSettle--
		l.mu.Unlock()
		return errors.New("ledger unavailable")
	}
	l.mu.Unlock()
	return l.finish(id, "SETTLED")
}

func (l *fakeLedger) ReleaseWithdrawal(_ context.Context, id int64) error {
	return l.finish(id, "RELEASED")
}

func (l *fakeLedger) balance(u int64, a string) (int64, int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.avail[key(u, a)], l.held[key(u, a)]
}

type fakeSigner struct {
	mu     sync.Mutex
	retry  int
	refuse bool
	calls  int
}

func (s *fakeSigner) Sign(_ context.Context, w store.Withdrawal) (engine.Signed, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.refuse {
		return engine.Signed{}, errors.New("destination rejected by the signer")
	}
	if s.retry > 0 {
		s.retry--
		return engine.Signed{}, fmt.Errorf("%w: hot wallet change unconfirmed", engine.ErrRetry)
	}
	return engine.Signed{TxID: fmt.Sprintf("tx-%d", w.ID), Raw: []byte(fmt.Sprintf("raw-%d", w.ID)), NetworkFee: "300", FeeAsset: "BTC"}, nil
}

type fakeChain struct {
	mu        sync.Mutex
	sent      map[string]int
	confs     map[string]int64
	reverted  map[string]bool
	unknown   map[string]bool
	failSends int
}

func newChain() *fakeChain {
	return &fakeChain{sent: map[string]int{}, confs: map[string]int64{}, reverted: map[string]bool{}, unknown: map[string]bool{}}
}

func (c *fakeChain) Broadcast(_ context.Context, _ []byte, txid string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failSends > 0 {
		c.failSends--
		return errors.New("node unreachable")
	}
	c.sent[txid]++
	return nil
}

func (c *fakeChain) Confirmations(_ context.Context, txid string) (int64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unknown[txid] {
		return -1, false, nil
	}
	return c.confs[txid], c.reverted[txid], nil
}

type fakeVerifier struct{}

func (fakeVerifier) VerifyTOTP(_ context.Context, user int64, code string) error {
	if user == 99 {
		return engine.ErrTOTPRequired
	}
	if code != "123456" {
		return engine.ErrTOTPInvalid
	}
	return nil
}

type fakeQueue struct {
	mu        sync.Mutex
	screening []int64
	reviews   []int64
}

func (q *fakeQueue) PublishScreening(_ context.Context, id int64) error {
	q.mu.Lock()
	q.screening = append(q.screening, id)
	q.mu.Unlock()
	return nil
}

func (q *fakeQueue) PublishReview(_ context.Context, w store.Withdrawal) error {
	q.mu.Lock()
	q.reviews = append(q.reviews, w.ID)
	q.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------- harness

type rig struct {
	e      *engine.Engine
	ledger *fakeLedger
	signer *fakeSigner
	chain  *fakeChain
	queue  *fakeQueue
	now    time.Time
	ctx    context.Context
}

var dest = func() string {
	a, _ := btcutil.NewAddressWitnessPubKeyHash(make([]byte, 20), &chaincfg.RegressionNetParams)
	return a.EncodeAddress()
}()

var denied = func() string {
	h := make([]byte, 20)
	h[0] = 1
	a, _ := btcutil.NewAddressWitnessPubKeyHash(h, &chaincfg.RegressionNetParams)
	return a.EncodeAddress()
}()

func setup(t *testing.T) *rig {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	dsn := testkit.Postgres(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "."))
	pool, err := pg.Connect(ctx, dsn, 20)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	dl := filepath.Join(t.TempDir(), "denylist.txt")
	require.NoError(t, os.WriteFile(dl, []byte("# test list\n"+denied+"\n"), 0o600))
	deny, err := rules.LoadDenylist(dl)
	require.NoError(t, err)
	r := &rig{ledger: newLedger(), signer: &fakeSigner{}, chain: newChain(), queue: &fakeQueue{}, ctx: ctx,
		now: time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)}
	r.e = &engine.Engine{St: store.New(pool), Ledger: r.ledger, Signer: r.signer, Chains: map[string]engine.Chain{"btc": r.chain},
		Verifier: fakeVerifier{}, Queue: r.queue, Cfg: engine.Config{
			DailyLimit:    map[string]money.Int128{"BTC": money.FromInt64(10_000_000)},
			ReviewAbove:   map[string]money.Int128{"BTC": money.FromInt64(1_000_000)},
			Confirmations: map[string]int64{"btc": 2}, Denylist: deny, RetryAfter: 30 * time.Second,
			Now: func() time.Time { return r.now }}}
	return r
}

func (r *rig) state(t *testing.T, id int64) rules.State {
	t.Helper()
	w, err := r.e.St.Get(r.ctx, id)
	require.NoError(t, err)
	return w.State
}

func (r *rig) request(t *testing.T, user int64, amount int64, to string) store.Withdrawal {
	t.Helper()
	w, err := r.e.Request(r.ctx, user, "btc", "BTC", money.FromInt64(amount), to, "123456")
	require.NoError(t, err)
	return w
}

// ---------------------------------------------------------------- tests

func TestSmallWithdrawalIsPaidAutomaticallyAndSettledAtTheThreshold(t *testing.T) {
	r := setup(t)
	r.ledger.fund(1, "BTC", 2_000_000)
	w := r.request(t, 1, 500_000, dest)
	require.Equal(t, rules.Requested, w.State)
	require.Equal(t, []int64{w.ID}, r.queue.screening)
	avail, held := r.ledger.balance(1, "BTC")
	require.Equal(t, int64(1_500_000), avail)
	require.Equal(t, int64(500_000), held, "held at request time")

	require.NoError(t, r.e.Screen(r.ctx, w.ID))
	require.Equal(t, rules.Approved, r.state(t, w.ID), "at or below the threshold: no review")
	n, err := r.e.ExecuteOnce(r.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, rules.Broadcast, r.state(t, w.ID))
	require.Equal(t, 1, r.chain.sent[fmt.Sprintf("tx-%d", w.ID)])
	_, held = r.ledger.balance(1, "BTC")
	require.Equal(t, int64(500_000), held, "still held while unconfirmed")

	r.chain.confs[fmt.Sprintf("tx-%d", w.ID)] = 1
	_, err = r.e.ConfirmOnce(r.ctx)
	require.NoError(t, err)
	got, _ := r.e.St.Get(r.ctx, w.ID)
	require.Equal(t, rules.Broadcast, got.State)
	require.EqualValues(t, 1, got.Confirmations)
	require.Equal(t, "300", got.NetworkFee)

	r.chain.confs[fmt.Sprintf("tx-%d", w.ID)] = 2
	n, err = r.e.ConfirmOnce(r.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, rules.Confirmed, r.state(t, w.ID))
	avail, held = r.ledger.balance(1, "BTC")
	require.Equal(t, int64(1_500_000), avail)
	require.Zero(t, held, "settled out of the exchange")
	require.Equal(t, "SETTLED", r.ledger.holds[w.ID].status)
}

func TestLargeWithdrawalWaitsForAnAdmin(t *testing.T) {
	r := setup(t)
	r.ledger.fund(2, "BTC", 9_000_000)
	w := r.request(t, 2, 3_000_000, dest)
	require.NoError(t, r.e.Screen(r.ctx, w.ID))
	require.Equal(t, rules.PendingReview, r.state(t, w.ID))
	require.Equal(t, []int64{w.ID}, r.queue.reviews, "a review task was queued")

	n, err := r.e.ExecuteOnce(r.ctx)
	require.NoError(t, err)
	require.Zero(t, n, "nothing is executed before approval")
	require.Zero(t, r.signer.calls, "the signer is never asked before approval")

	_, err = r.e.Review(r.ctx, w.ID, true, 1000, "")
	require.NoError(t, err)
	n, err = r.e.ExecuteOnce(r.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	w2 := r.request(t, 2, 2_000_000, dest)
	require.NoError(t, r.e.Screen(r.ctx, w2.ID))
	_, err = r.e.Review(r.ctx, w2.ID, false, 1000, "suspicious")
	require.NoError(t, err)
	require.Equal(t, rules.Rejected, r.state(t, w2.ID))
	avail, held := r.ledger.balance(2, "BTC")
	require.Equal(t, int64(6_000_000), avail, "the rejected one was released")
	require.Equal(t, int64(3_000_000), held, "the approved one is still held until confirmed")
}

func TestTwoAdminsDecidingAtOnceHaveOneWinner(t *testing.T) {
	r := setup(t)
	r.ledger.fund(3, "BTC", 9_000_000)
	w := r.request(t, 3, 2_000_000, dest)
	require.NoError(t, r.e.Screen(r.ctx, w.ID))
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, approve := range []bool{true, false} {
		wg.Add(1)
		go func(a bool) {
			defer wg.Done()
			_, err := r.e.Review(r.ctx, w.ID, a, 1000, "")
			results <- err
		}(approve)
	}
	wg.Wait()
	close(results)
	ok := 0
	for err := range results {
		if err == nil {
			ok++
		} else {
			require.ErrorIs(t, err, store.ErrStale)
		}
	}
	require.Equal(t, 1, ok)
	final := r.state(t, w.ID)
	_, held := r.ledger.balance(3, "BTC")
	if final == rules.Approved {
		require.Equal(t, int64(2_000_000), held, "approved: the funds must stay held")
	} else {
		require.Equal(t, rules.Rejected, final)
		require.Zero(t, held, "rejected: released")
	}
}

func TestDenylistedDestinationIsRejectedAndReleased(t *testing.T) {
	r := setup(t)
	r.ledger.fund(4, "BTC", 1_000_000)
	w := r.request(t, 4, 400_000, denied)
	require.NoError(t, r.e.Screen(r.ctx, w.ID))
	got, err := r.e.St.Get(r.ctx, w.ID)
	require.NoError(t, err)
	require.Equal(t, rules.Rejected, got.State)
	require.Contains(t, got.Reason, "denylist")
	avail, held := r.ledger.balance(4, "BTC")
	require.Equal(t, int64(1_000_000), avail)
	require.Zero(t, held)
}

func TestRequestRefusals(t *testing.T) {
	r := setup(t)
	r.ledger.fund(5, "BTC", 100_000)
	_, err := r.e.Request(r.ctx, 5, "btc", "BTC", money.FromInt64(1000), dest, "000000")
	require.ErrorIs(t, err, engine.ErrTOTPInvalid)
	_, err = r.e.Request(r.ctx, 99, "btc", "BTC", money.FromInt64(1000), dest, "123456")
	require.ErrorIs(t, err, engine.ErrTOTPRequired)
	_, err = r.e.Request(r.ctx, 5, "eth", "BTC", money.FromInt64(1000), dest, "123456")
	require.ErrorIs(t, err, engine.ErrUnsupported)
	ws, err := r.e.St.ListForUser(r.ctx, 5)
	require.NoError(t, err)
	require.Empty(t, ws, "refused requests leave no withdrawal")

	_, err = r.e.Request(r.ctx, 5, "btc", "BTC", money.FromInt64(200_000), dest, "123456")
	require.ErrorIs(t, err, engine.ErrInsufficientFunds)
	ws, err = r.e.St.ListForUser(r.ctx, 5)
	require.NoError(t, err)
	require.Len(t, ws, 1)
	require.Equal(t, rules.Failed, ws[0].State, "recorded as FAILED, nothing held")
	avail, held := r.ledger.balance(5, "BTC")
	require.Equal(t, int64(100_000), avail)
	require.Zero(t, held)
}

func TestSignerRetryAndRefusal(t *testing.T) {
	r := setup(t)
	r.ledger.fund(6, "BTC", 5_000_000)
	w := r.request(t, 6, 100_000, dest)
	require.NoError(t, r.e.Screen(r.ctx, w.ID))
	r.signer.retry = 1
	n, err := r.e.ExecuteOnce(r.ctx)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Equal(t, rules.Signing, r.state(t, w.ID))
	n, err = r.e.ExecuteOnce(r.ctx)
	require.NoError(t, err)
	require.Zero(t, n, "backoff: not retried before RetryAfter")
	r.now = r.now.Add(31 * time.Second)
	n, err = r.e.ExecuteOnce(r.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	w2 := r.request(t, 6, 100_000, dest)
	require.NoError(t, r.e.Screen(r.ctx, w2.ID))
	r.signer.refuse = true
	_, err = r.e.ExecuteOnce(r.ctx)
	require.NoError(t, err)
	require.Equal(t, rules.Failed, r.state(t, w2.ID))
	require.Equal(t, "RELEASED", r.ledger.holds[w2.ID].status)
}

func TestRevertedTransactionReleasesAndNeverSettles(t *testing.T) {
	r := setup(t)
	r.ledger.fund(7, "BTC", 1_000_000)
	w := r.request(t, 7, 100_000, dest)
	require.NoError(t, r.e.Screen(r.ctx, w.ID))
	_, err := r.e.ExecuteOnce(r.ctx)
	require.NoError(t, err)
	r.chain.reverted[fmt.Sprintf("tx-%d", w.ID)] = true
	_, err = r.e.ConfirmOnce(r.ctx)
	require.NoError(t, err)
	require.Equal(t, rules.Failed, r.state(t, w.ID))
	require.Equal(t, "RELEASED", r.ledger.holds[w.ID].status)
	avail, _ := r.ledger.balance(7, "BTC")
	require.Equal(t, int64(1_000_000), avail)
}

func TestFinalizerRepairsALedgerFailureAfterConfirmation(t *testing.T) {
	r := setup(t)
	r.ledger.fund(8, "BTC", 1_000_000)
	w := r.request(t, 8, 100_000, dest)
	require.NoError(t, r.e.Screen(r.ctx, w.ID))
	_, err := r.e.ExecuteOnce(r.ctx)
	require.NoError(t, err)
	r.chain.confs[fmt.Sprintf("tx-%d", w.ID)] = 5
	r.ledger.failSettle = 1
	_, err = r.e.ConfirmOnce(r.ctx)
	require.NoError(t, err)
	require.Equal(t, rules.Confirmed, r.state(t, w.ID))
	require.Equal(t, "ACTIVE", r.ledger.holds[w.ID].status, "the settle failed")
	require.NoError(t, r.e.FinalizeOnce(r.ctx))
	require.Equal(t, "SETTLED", r.ledger.holds[w.ID].status, "the finaliser applied it")
	require.NoError(t, r.e.FinalizeOnce(r.ctx))
	require.Equal(t, "SETTLED", r.ledger.holds[w.ID].status)
}

func TestDroppedTransactionIsRebroadcast(t *testing.T) {
	r := setup(t)
	r.ledger.fund(9, "BTC", 1_000_000)
	w := r.request(t, 9, 100_000, dest)
	require.NoError(t, r.e.Screen(r.ctx, w.ID))
	_, err := r.e.ExecuteOnce(r.ctx)
	require.NoError(t, err)
	tx := fmt.Sprintf("tx-%d", w.ID)
	r.chain.unknown[tx] = true
	_, err = r.e.ConfirmOnce(r.ctx)
	require.NoError(t, err)
	require.Equal(t, 2, r.chain.sent[tx], "the same transaction is sent again")
	require.Equal(t, rules.Broadcast, r.state(t, w.ID))
}

// Screening tasks are delivered at least once: concurrent duplicates decide once.
func TestDuplicateScreeningDecidesOnce(t *testing.T) {
	r := setup(t)
	r.ledger.fund(10, "BTC", 9_000_000)
	w := r.request(t, 10, 2_000_000, dest)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, r.e.Screen(r.ctx, w.ID))
		}()
	}
	wg.Wait()
	require.Equal(t, rules.PendingReview, r.state(t, w.ID))
	require.Len(t, r.queue.reviews, 1, "one review task, not eight")
	_, held := r.ledger.balance(10, "BTC")
	require.Equal(t, int64(2_000_000), held, "held once")
}

func TestSweeperRequeuesStuckScreening(t *testing.T) {
	r := setup(t)
	r.ledger.fund(11, "BTC", 1_000_000)
	w := r.request(t, 11, 100_000, dest)
	r.queue.screening = nil
	require.NoError(t, r.e.SweepOnce(r.ctx, time.Minute))
	require.Empty(t, r.queue.screening, "not yet stuck")
	r.now = r.now.Add(2 * time.Minute)
	require.NoError(t, r.e.SweepOnce(r.ctx, time.Minute))
	require.Equal(t, []int64{w.ID}, r.queue.screening)
}
