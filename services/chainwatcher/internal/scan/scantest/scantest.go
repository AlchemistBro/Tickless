// Package scantest is test support for the scanner: a fake chain that can
// reorganise, an in-memory reference Store, a failure-injecting Store wrapper
// and the random-history scenario that every Store implementation must pass
// (SPEC §6 invariants 4 and 5). It is imported by tests only.
package scantest

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tickless/chainwatcher/internal/scan"
	"github.com/tickless/pkg/money"
)

// ---------------------------------------------------------------- fake chain

// Chain is an in-memory best chain; Blocks[i].Height == i, Blocks[0] is genesis.
type Chain struct {
	Blocks []scan.Block
	seq    int
	tag    string
}

// NewChain returns a chain holding only a genesis block. tag makes block
// hashes unique across chains sharing one store.
func NewChain(tag string) *Chain {
	return &Chain{Blocks: []scan.Block{{Height: 0, Hash: tag + "-genesis"}}, tag: tag}
}

// TipHeight implements scan.Chain.
func (c *Chain) TipHeight(context.Context) (int64, error) { return int64(len(c.Blocks) - 1), nil }

// BlockAt implements scan.Chain.
func (c *Chain) BlockAt(_ context.Context, h int64) (scan.Block, error) {
	if h < 0 || h >= int64(len(c.Blocks)) {
		return scan.Block{}, scan.ErrNoBlock
	}
	return c.Blocks[h], nil
}

// Mine appends a block with the given transfers.
func (c *Chain) Mine(ts ...scan.Transfer) {
	c.seq++
	tip := c.Blocks[len(c.Blocks)-1]
	c.Blocks = append(c.Blocks, scan.Block{Height: tip.Height + 1, Hash: fmt.Sprintf("%s-b%d", c.tag, c.seq), Parent: tip.Hash, Transfers: ts})
}

// Reorg replaces the top depth blocks with depth+extra new ones. With remine
// the removed transfers are included again in the first new block.
func (c *Chain) Reorg(depth, extra int, remine bool) {
	depth = min(depth, len(c.Blocks)-1)
	var removed []scan.Transfer
	for _, b := range c.Blocks[len(c.Blocks)-depth:] {
		removed = append(removed, b.Transfers...)
	}
	c.Blocks = c.Blocks[:len(c.Blocks)-depth]
	for i := 0; i < depth+extra; i++ {
		if i == 0 && remine {
			c.Mine(removed...)
		} else {
			c.Mine()
		}
	}
}

// Shorten removes the top n blocks without replacement (invalidateblock).
func (c *Chain) Shorten(n int) {
	n = min(n, len(c.Blocks)-1)
	c.Blocks = c.Blocks[:len(c.Blocks)-n]
}

// ---------------------------------------------------------------- inspection

// Key identifies a deposit.
type Key struct {
	TxID  string
	Index int64
}

// Inspector reads a Store's state for assertions.
type Inspector interface {
	// Credits returns how many CONFIRMED (credit) events were emitted per deposit.
	Credits(t *testing.T) map[Key]int
	// Status returns a deposit's status ("" if unknown).
	Status(t *testing.T, k Key) string
	// Deposits returns how many deposit rows exist per key.
	Deposits(t *testing.T) map[Key]int
	// MaxHeight returns the highest stored block height (-1 if none).
	MaxHeight(t *testing.T) int64
}

// Harness is a Store under test.
type Harness struct {
	Store   scan.Store
	Inspect Inspector
}

// Factory builds a fresh, empty Store that watches the given addresses.
type Factory func(t *testing.T, watched []string) Harness

// ---------------------------------------------------------------- flaky store

// ErrInjected is the failure injected by Flaky.
var ErrInjected = errors.New("injected store failure")

// Flaky wraps a Store and fails a fraction of calls, either BEFORE the call
// (nothing happened) or AFTER it committed (the caller cannot know it
// succeeded, like a connection lost after COMMIT).
type Flaky struct {
	Inner scan.Store
	Rate  float64
	Rng   *rand.Rand
}

func (f *Flaky) before() bool { return f.Rate > 0 && f.Rng.Float64() < f.Rate/2 }
func (f *Flaky) after() bool  { return f.Rate > 0 && f.Rng.Float64() < f.Rate/2 }

// Head implements scan.Store.
func (f *Flaky) Head(ctx context.Context) (int64, string, bool, error) {
	if f.before() {
		return 0, "", false, ErrInjected
	}
	return f.Inner.Head(ctx)
}

// HashAt implements scan.Store.
func (f *Flaky) HashAt(ctx context.Context, h int64) (string, bool, error) {
	if f.before() {
		return "", false, ErrInjected
	}
	return f.Inner.HashAt(ctx, h)
}

// ApplyBlock implements scan.Store.
func (f *Flaky) ApplyBlock(ctx context.Context, b scan.Block) error {
	if f.before() {
		return ErrInjected
	}
	if err := f.Inner.ApplyBlock(ctx, b); err != nil {
		return err
	}
	if f.after() {
		return ErrInjected
	}
	return nil
}

// Rollback implements scan.Store.
func (f *Flaky) Rollback(ctx context.Context, ancestor int64) (int, error) {
	if f.before() {
		return 0, ErrInjected
	}
	n, err := f.Inner.Rollback(ctx, ancestor)
	if err == nil && f.after() {
		return 0, ErrInjected
	}
	return n, err
}

// Confirm implements scan.Store.
func (f *Flaky) Confirm(ctx context.Context, head, required int64) error {
	if f.before() {
		return ErrInjected
	}
	if err := f.Inner.Confirm(ctx, head, required); err != nil {
		return err
	}
	if f.after() {
		return ErrInjected
	}
	return nil
}

// ---------------------------------------------------------------- scenario

// Sync steps the scanner until it reports no new blocks, retrying injected
// failures.
func Sync(t *testing.T, s *scan.Scanner) {
	t.Helper()
	for i := 0; i < 2000; i++ {
		n, err := s.Step(context.Background())
		if err != nil {
			require.ErrorIs(t, err, ErrInjected)
			continue
		}
		if n == 0 {
			return
		}
	}
	t.Fatal("scanner did not converge")
}

// RunRandomHistories drives a scanner over random chain histories against
// stores built by f: blocks with transfers to watched and unwatched addresses,
// reorgs and shorter chains that stay below the confirmation threshold (with
// and without re-mining), store failures before and after commit, and scanner
// restarts. After every step a deposit exists once, is credited at most once,
// and every credited deposit is on the best chain; at the end exactly the
// watched transfers buried at least `required` deep are credited.
func RunRandomHistories(t *testing.T, seeds, ops int, f Factory) {
	watched := []string{"a1", "a2", "a3"}
	for seed := 0; seed < seeds; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(seed), 99)) //nolint:gosec // seeded on purpose: a failing history must be reproducible from its seed
			required := int64(1 + rng.IntN(6))
			c := NewChain(fmt.Sprintf("s%d", seed))
			h := f(t, watched)
			flaky := &Flaky{Inner: h.Store, Rate: 0.3, Rng: rng}
			newScanner := func() *scan.Scanner {
				return &scan.Scanner{Chain: c, Store: flaky, Name: "test", Start: 1, Required: required, MaxBatch: 1 + rng.IntN(4),
					OnDeepReorg: func(int) { t.Fatal("reorgs below the threshold must never remove a credit") }}
			}
			s := newScanner()
			addrs := append([]string{"stranger"}, watched...)
			tx, peak := 0, int64(0)
			// A deposit may be confirmed up to height peak-required+1, so every
			// reorg (and shorten+reorg combination) keeps that height.
			maxDepth := func() int {
				return int(min(int64(len(c.Blocks)-1)-(peak-required+1), required-1))
			}
			for op := 0; op < ops; op++ {
				switch r := rng.IntN(100); {
				case r < 60:
					var ts []scan.Transfer
					for i := rng.IntN(3); i > 0; i-- {
						tx++
						ts = append(ts, scan.Transfer{TxID: fmt.Sprintf("t%d", tx), Index: int64(rng.IntN(3)), Address: addrs[rng.IntN(len(addrs))],
							Asset: "BTC", Amount: money.FromInt64(int64(1 + rng.IntN(1000)))})
					}
					c.Mine(ts...)
				case r < 75 && maxDepth() >= 1:
					c.Reorg(1+rng.IntN(maxDepth()), rng.IntN(3), rng.IntN(2) == 0)
				case r < 80 && maxDepth() >= 1:
					c.Shorten(1 + rng.IntN(maxDepth()))
				case r < 85:
					s = newScanner() // restart: the scanner keeps no state of its own
				}
				Sync(t, s)
				checkInvariants(t, c, h.Inspect)
				peak = max(peak, h.Inspect.MaxHeight(t))
			}
			for i := int64(0); i < required; i++ {
				c.Mine()
			}
			Sync(t, s)
			checkInvariants(t, c, h.Inspect)

			want := map[string]bool{}
			tip := int64(len(c.Blocks) - 1)
			isWatched := map[string]bool{}
			for _, a := range watched {
				isWatched[a] = true
			}
			for _, b := range c.Blocks {
				for _, tr := range b.Transfers {
					if isWatched[tr.Address] && tip-b.Height+1 >= required {
						want[fmt.Sprintf("%s:%d", tr.TxID, tr.Index)] = true
					}
				}
			}
			got := map[string]bool{}
			for k, n := range h.Inspect.Credits(t) {
				require.Equal(t, 1, n, "%v credited %d times", k, n)
				got[fmt.Sprintf("%s:%d", k.TxID, k.Index)] = true
			}
			require.Equal(t, sorted(want), sorted(got))
		})
	}
}

func checkInvariants(t *testing.T, c *Chain, in Inspector) {
	t.Helper()
	onChain := map[Key]bool{}
	for _, b := range c.Blocks {
		for _, tr := range b.Transfers {
			onChain[Key{tr.TxID, tr.Index}] = true
		}
	}
	for k, n := range in.Deposits(t) {
		require.Equal(t, 1, n, "deposit %v stored %d times", k, n)
	}
	for k, n := range in.Credits(t) {
		require.LessOrEqual(t, n, 1, "deposit %v credited %d times", k, n)
		require.True(t, onChain[k], "credited deposit %v is not on the best chain", k)
		require.Equal(t, "CONFIRMED", in.Status(t, k))
	}
}

func sorted(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
