package scan

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tickless/pkg/money"
)

// ---------------------------------------------------------------- fake chain

type fakeChain struct {
	blocks []Block // index == height; blocks[0] is genesis
	seq    int
}

func newFakeChain() *fakeChain {
	c := &fakeChain{}
	c.blocks = []Block{{Height: 0, Hash: "genesis"}}
	return c
}

func (c *fakeChain) TipHeight(context.Context) (int64, error) { return int64(len(c.blocks) - 1), nil }

func (c *fakeChain) BlockAt(_ context.Context, h int64) (Block, error) {
	if h < 0 || h >= int64(len(c.blocks)) {
		return Block{}, ErrNoBlock
	}
	return c.blocks[h], nil
}

func (c *fakeChain) mine(ts ...Transfer) {
	c.seq++
	tip := c.blocks[len(c.blocks)-1]
	c.blocks = append(c.blocks, Block{Height: tip.Height + 1, Hash: fmt.Sprintf("b%d", c.seq), Parent: tip.Hash, Transfers: ts})
}

// reorg replaces the top depth blocks with depth+extra new ones. When remine
// is true the removed transfers are included again in the first new block
// (a transaction re-mined on the winning branch); otherwise they vanish.
func (c *fakeChain) reorg(depth, extra int, remine bool) {
	if depth > len(c.blocks)-1 {
		depth = len(c.blocks) - 1
	}
	var removed []Transfer
	for _, b := range c.blocks[len(c.blocks)-depth:] {
		removed = append(removed, b.Transfers...)
	}
	c.blocks = c.blocks[:len(c.blocks)-depth]
	for i := 0; i < depth+extra; i++ {
		if i == 0 && remine {
			c.mine(removed...)
		} else {
			c.mine()
		}
	}
}

// shorten removes the top n blocks without a replacement (invalidateblock).
func (c *fakeChain) shorten(n int) {
	if n > len(c.blocks)-1 {
		n = len(c.blocks) - 1
	}
	c.blocks = c.blocks[:len(c.blocks)-n]
}

// ---------------------------------------------------------------- memory store

type depKey struct {
	txid  string
	index int64
}

type dep struct {
	status        string // PENDING | CONFIRMED | REORGED
	height        int64
	confirmations int64
	amount        money.Int128
}

// memStore mirrors the PostgreSQL store's contract. Every method is atomic:
// an injected failure happens before any mutation.
type memStore struct {
	watched  map[string]bool
	blocks   map[int64]string
	deps     map[depKey]*dep
	credits  map[depKey]int // CONFIRMED events emitted per deposit
	failRate float64
	rng      *rand.Rand
}

var errInjected = errors.New("injected store failure")

func newMemStore(rng *rand.Rand, watched ...string) *memStore {
	s := &memStore{watched: map[string]bool{}, blocks: map[int64]string{}, deps: map[depKey]*dep{}, credits: map[depKey]int{}, rng: rng}
	for _, a := range watched {
		s.watched[a] = true
	}
	return s
}

func (s *memStore) fail() error {
	if s.failRate > 0 && s.rng.Float64() < s.failRate {
		return errInjected
	}
	return nil
}

func (s *memStore) Head(context.Context) (int64, string, bool, error) {
	if err := s.fail(); err != nil {
		return 0, "", false, err
	}
	best, ok := int64(-1), false
	for h := range s.blocks {
		if h > best {
			best, ok = h, true
		}
	}
	return best, s.blocks[best], ok, nil
}

func (s *memStore) HashAt(_ context.Context, h int64) (string, bool, error) {
	if err := s.fail(); err != nil {
		return "", false, err
	}
	v, ok := s.blocks[h]
	return v, ok, nil
}

func (s *memStore) ApplyBlock(_ context.Context, b Block) error {
	if err := s.fail(); err != nil {
		return err
	}
	if _, dup := s.blocks[b.Height]; dup {
		return fmt.Errorf("block %d already applied", b.Height) // a scanner bug, never expected
	}
	s.blocks[b.Height] = b.Hash
	for _, t := range b.Transfers {
		if !s.watched[t.Address] {
			continue
		}
		k := depKey{t.TxID, t.Index}
		d, ok := s.deps[k]
		switch {
		case !ok:
			s.deps[k] = &dep{status: "PENDING", height: b.Height, amount: t.Amount}
		case d.status == "REORGED":
			d.status, d.height, d.confirmations = "PENDING", b.Height, 0
		}
	}
	return nil
}

func (s *memStore) Rollback(_ context.Context, ancestor int64) (int, error) {
	if err := s.fail(); err != nil {
		return 0, err
	}
	for h := range s.blocks {
		if h > ancestor {
			delete(s.blocks, h)
		}
	}
	lost := 0
	for _, d := range s.deps {
		if d.height <= ancestor {
			continue
		}
		switch d.status {
		case "PENDING":
			d.status, d.confirmations = "REORGED", 0
		case "CONFIRMED":
			lost++
		}
	}
	return lost, nil
}

func (s *memStore) Confirm(_ context.Context, head, required int64) error {
	if err := s.fail(); err != nil {
		return err
	}
	for k, d := range s.deps {
		if d.status != "PENDING" {
			continue
		}
		d.confirmations = head - d.height + 1
		if d.confirmations >= required {
			d.status = "CONFIRMED"
			s.credits[k]++
		}
	}
	return nil
}

// ---------------------------------------------------------------- tests

func step(t *testing.T, s *Scanner) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		n, err := s.Step(context.Background())
		if err != nil {
			require.ErrorIs(t, err, errInjected)
			continue
		}
		if n == 0 {
			return
		}
	}
	t.Fatal("scanner did not converge")
}

func TestReorgDropsUnconfirmedDepositWithoutCredit(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	c := newFakeChain()
	st := newMemStore(rng, "alice")
	s := &Scanner{Chain: c, Store: st, Name: "test", Start: 1, Required: 2}

	c.mine()
	c.mine(Transfer{TxID: "tx1", Index: 0, Address: "alice", Asset: "BTC", Amount: money.FromInt64(5)})
	step(t, s)
	require.Equal(t, "PENDING", st.deps[depKey{"tx1", 0}].status)
	require.EqualValues(t, 1, st.deps[depKey{"tx1", 0}].confirmations)

	c.reorg(3, 0, false) // tx1 is not on the winning branch
	step(t, s)
	require.Equal(t, "REORGED", st.deps[depKey{"tx1", 0}].status)

	for i := 0; i < 5; i++ {
		c.mine()
	}
	step(t, s)
	require.Equal(t, "REORGED", st.deps[depKey{"tx1", 0}].status)
	require.Zero(t, st.credits[depKey{"tx1", 0}], "a reorged deposit must never be credited")
}

func TestRemindedDepositIsCreditedOnceInItsNewBlock(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	c := newFakeChain()
	st := newMemStore(rng, "alice")
	s := &Scanner{Chain: c, Store: st, Name: "test", Start: 1, Required: 3}

	c.mine(Transfer{TxID: "tx1", Index: 1, Address: "alice", Asset: "BTC", Amount: money.FromInt64(7)})
	c.mine()
	step(t, s)
	c.reorg(2, 0, true) // same height, other hashes; the tx is mined again on the new branch
	step(t, s)
	d := st.deps[depKey{"tx1", 1}]
	require.Equal(t, "PENDING", d.status)
	require.EqualValues(t, 1, d.height, "re-mined in the first block of the new branch")

	c.mine()
	step(t, s)
	require.Equal(t, "CONFIRMED", d.status)
	require.Equal(t, 1, st.credits[depKey{"tx1", 1}])
	require.Len(t, st.deps, 1)
}

func TestShorterChainIsDetectedAsReorg(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	c := newFakeChain()
	st := newMemStore(rng, "bob")
	s := &Scanner{Chain: c, Store: st, Name: "test", Start: 1, Required: 6}
	for i := 0; i < 4; i++ {
		c.mine()
	}
	c.mine(Transfer{TxID: "tx9", Address: "bob", Asset: "ETH", Amount: money.FromInt64(1)})
	step(t, s)
	c.shorten(3) // the tip goes DOWN; nothing new to extend
	step(t, s)
	require.Equal(t, "REORGED", st.deps[depKey{"tx9", 0}].status)
	head, _, _, err := st.Head(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 2, head)
}

func TestDeepReorgOfConfirmedDepositIsReportedNotHidden(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 8))
	c := newFakeChain()
	st := newMemStore(rng, "alice")
	var lost int
	s := &Scanner{Chain: c, Store: st, Name: "test", Start: 1, Required: 2, OnDeepReorg: func(n int) { lost += n }}
	c.mine(Transfer{TxID: "tx1", Address: "alice", Asset: "BTC", Amount: money.FromInt64(1)})
	c.mine()
	c.mine()
	step(t, s)
	require.Equal(t, "CONFIRMED", st.deps[depKey{"tx1", 0}].status)

	c.reorg(3, 1, false)
	step(t, s)
	require.Equal(t, 1, lost, "a reorg deeper than the threshold must be reported")
	require.Equal(t, 1, st.credits[depKey{"tx1", 0}], "and never credited twice")
}

// TestRandomChainHistoriesKeepInvariants4And5 drives the scanner through random
// histories: blocks with transfers to watched and unwatched addresses, reorgs
// shallower than the threshold (with and without re-mining), shorter chains,
// injected store failures and scanner restarts. After every step:
//   - a deposit (txid, index) exists at most once and is credited at most once;
//   - a credited deposit is on the current best chain (reorgs stay below the
//     threshold, so a credit can never be invalidated);
//
// and at the end, after enough blocks, exactly the watched transfers on the
// final best chain are credited.
func TestRandomChainHistoriesKeepInvariants4And5(t *testing.T) {
	for seed := uint64(0); seed < 300; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 99))
			required := int64(1 + rng.IntN(6))
			c := newFakeChain()
			st := newMemStore(rng, "a1", "a2", "a3")
			st.failRate = 0.15
			newScanner := func() *Scanner {
				return &Scanner{Chain: c, Store: st, Name: "test", Start: 1, Required: required, MaxBatch: 1 + rng.IntN(4),
					OnDeepReorg: func(int) { t.Fatal("reorgs below the threshold must never remove a credit") }}
			}
			s := newScanner()
			tx := 0
			addrs := []string{"a1", "a2", "a3", "stranger"}
			// peak is the highest head the scanner ever processed. A deposit can
			// be confirmed up to height peak-required+1, so a "shallow" reorg is
			// one that keeps every block at or below that height; consecutive
			// shorten+reorg operations are bounded by it as a whole.
			peak := int64(0)
			maxDepth := func() int {
				d := int64(len(c.blocks)-1) - (peak - required + 1)
				return int(min(d, required-1))
			}
			for op := 0; op < 120; op++ {
				switch r := rng.IntN(100); {
				case r < 60:
					var ts []Transfer
					for i := rng.IntN(3); i > 0; i-- {
						tx++
						ts = append(ts, Transfer{TxID: fmt.Sprintf("t%d", tx), Index: int64(rng.IntN(3)), Address: addrs[rng.IntN(len(addrs))], Asset: "BTC", Amount: money.FromInt64(int64(1 + rng.IntN(1000)))})
					}
					c.mine(ts...)
				case r < 75 && maxDepth() >= 1:
					c.reorg(1+rng.IntN(maxDepth()), rng.IntN(3), rng.IntN(2) == 0)
				case r < 80 && maxDepth() >= 1:
					c.shorten(1 + rng.IntN(maxDepth()))
				case r < 85:
					s = newScanner() // restart: the scanner holds no state of its own
				}
				step(t, s)
				checkInvariants(t, c, st)
				for h := range st.blocks { // read state directly: no injected failure here
					peak = max(peak, h)
				}
			}
			st.failRate = 0
			for i := int64(0); i < required; i++ {
				c.mine()
			}
			step(t, s)
			checkInvariants(t, c, st)

			want := map[depKey]bool{}
			tipH := int64(len(c.blocks) - 1)
			for _, b := range c.blocks {
				for _, tr := range b.Transfers {
					if st.watched[tr.Address] && tipH-b.Height+1 >= required {
						want[depKey{tr.TxID, tr.Index}] = true
					}
				}
			}
			got := map[depKey]bool{}
			for k, n := range st.credits {
				require.Equal(t, 1, n, "%v credited %d times", k, n)
				got[k] = true
			}
			require.Equal(t, sortedKeys(want), sortedKeys(got))
		})
	}
}

func checkInvariants(t *testing.T, c *fakeChain, st *memStore) {
	t.Helper()
	onChain := map[depKey]bool{}
	for _, b := range c.blocks {
		for _, tr := range b.Transfers {
			onChain[depKey{tr.TxID, tr.Index}] = true
		}
	}
	for k, n := range st.credits {
		require.LessOrEqual(t, n, 1, "deposit %v credited %d times", k, n)
		require.True(t, onChain[k], "credited deposit %v is not on the best chain", k)
		require.Equal(t, "CONFIRMED", st.deps[k].status)
	}
}

func sortedKeys(m map[depKey]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, fmt.Sprintf("%s:%d", k.txid, k.index))
	}
	sort.Strings(out)
	return out
}
