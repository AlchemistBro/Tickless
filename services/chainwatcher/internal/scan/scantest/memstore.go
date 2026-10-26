package scantest

import (
	"context"
	"fmt"
	"testing"

	"github.com/tickless/chainwatcher/internal/scan"
	"github.com/tickless/pkg/money"
)

type memDep struct {
	status        string // PENDING | CONFIRMED | REORGED
	height        int64
	confirmations int64
	amount        money.Int128
}

// MemStore is the in-memory reference implementation of scan.Store. Every
// method is atomic (it cannot fail half-way).
type MemStore struct {
	watched map[string]bool
	blocks  map[int64]string
	deps    map[Key]*memDep
	credits map[Key]int
}

// NewMemStore returns an empty store watching the given addresses.
func NewMemStore(watched ...string) *MemStore {
	s := &MemStore{watched: map[string]bool{}, blocks: map[int64]string{}, deps: map[Key]*memDep{}, credits: map[Key]int{}}
	for _, a := range watched {
		s.watched[a] = true
	}
	return s
}

// MemFactory builds MemStores for RunRandomHistories.
func MemFactory(_ *testing.T, watched []string) Harness {
	s := NewMemStore(watched...)
	return Harness{Store: s, Inspect: s}
}

// Head implements scan.Store.
func (s *MemStore) Head(context.Context) (int64, string, bool, error) {
	h := s.maxHeight()
	if h < 0 {
		return 0, "", false, nil
	}
	return h, s.blocks[h], true, nil
}

func (s *MemStore) maxHeight() int64 {
	best := int64(-1)
	for h := range s.blocks {
		best = max(best, h)
	}
	return best
}

// HashAt implements scan.Store.
func (s *MemStore) HashAt(_ context.Context, h int64) (string, bool, error) {
	v, ok := s.blocks[h]
	return v, ok, nil
}

// ApplyBlock implements scan.Store.
func (s *MemStore) ApplyBlock(_ context.Context, b scan.Block) error {
	if _, dup := s.blocks[b.Height]; dup {
		return fmt.Errorf("block %d already applied", b.Height)
	}
	s.blocks[b.Height] = b.Hash
	for _, t := range b.Transfers {
		if !s.watched[t.Address] {
			continue
		}
		k := Key{t.TxID, t.Index}
		d, ok := s.deps[k]
		switch {
		case !ok:
			s.deps[k] = &memDep{status: "PENDING", height: b.Height, amount: t.Amount}
		case d.status == "REORGED":
			d.status, d.height, d.confirmations = "PENDING", b.Height, 0
		}
	}
	return nil
}

// Rollback implements scan.Store.
func (s *MemStore) Rollback(_ context.Context, ancestor int64) (int, error) {
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

// Confirm implements scan.Store.
func (s *MemStore) Confirm(_ context.Context, head, required int64) error {
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

// Credits implements Inspector.
func (s *MemStore) Credits(*testing.T) map[Key]int {
	out := make(map[Key]int, len(s.credits))
	for k, v := range s.credits {
		out[k] = v
	}
	return out
}

// Status implements Inspector.
func (s *MemStore) Status(_ *testing.T, k Key) string {
	if d, ok := s.deps[k]; ok {
		return d.status
	}
	return ""
}

// Confirmations returns a deposit's current confirmation count.
func (s *MemStore) Confirmations(k Key) int64 {
	if d, ok := s.deps[k]; ok {
		return d.confirmations
	}
	return 0
}

// Height returns the block height a deposit is currently recorded in.
func (s *MemStore) Height(k Key) int64 {
	if d, ok := s.deps[k]; ok {
		return d.height
	}
	return 0
}

// Deposits implements Inspector: the map makes duplicates impossible by
// construction, so every known key counts once.
func (s *MemStore) Deposits(*testing.T) map[Key]int {
	out := make(map[Key]int, len(s.deps))
	for k := range s.deps {
		out[k] = 1
	}
	return out
}

// MaxHeight implements Inspector.
func (s *MemStore) MaxHeight(*testing.T) int64 { return s.maxHeight() }
