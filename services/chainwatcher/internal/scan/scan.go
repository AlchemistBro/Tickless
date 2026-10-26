// Package scan is the chain-agnostic block scanner of chainwatcher (ADR 0010).
//
// It follows one chain block by block. Every processed block's hash and parent
// hash are stored; before extending, the scanner checks that its stored head is
// still on the node's best chain and that the next block's parent is that head.
// On a mismatch it walks back to the common ancestor, rolls the store back to
// it (pending deposits above it become REORGED) and rescans the new branch.
//
// A deposit is credited only through Confirm, which runs after the head has
// been validated, so a deposit whose block left the chain before the threshold
// is never confirmed (SPEC §6 invariant 5). The store applies a block and its
// deposits atomically and keys deposits by (chain, txid, index), so a restart
// mid-block or a rescan never creates a second deposit (invariant 4).
package scan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/tickless/pkg/money"
)

// Block is a block of the node's best chain with the transfers it contains.
type Block struct {
	Height    int64
	Hash      string
	Parent    string
	Transfers []Transfer
}

// Transfer is a payment to an address inside a block. The scanner passes every
// transfer to the store, which keeps only those to watched deposit addresses.
type Transfer struct {
	TxID    string
	Index   int64 // BTC vout, ERC-20 log index, 0 for native ETH
	Address string
	Asset   string
	Amount  money.Int128
}

// Chain reads the node's current best chain.
type Chain interface {
	TipHeight(ctx context.Context) (int64, error)
	// BlockAt returns the block at height on the best chain. ErrNoBlock if
	// height is above the tip.
	BlockAt(ctx context.Context, height int64) (Block, error)
}

// ErrNoBlock is returned by Chain.BlockAt for a height above the tip.
var ErrNoBlock = errors.New("scan: no block at height")

// Store persists scanner progress and deposits. Implementations must make
// each method atomic.
type Store interface {
	// Head returns the highest processed block, ok=false before the first one.
	Head(ctx context.Context) (height int64, hash string, ok bool, err error)
	// HashAt returns the stored hash of a processed height.
	HashAt(ctx context.Context, height int64) (hash string, ok bool, err error)
	// ApplyBlock records b and the deposits it contains (transfers to watched
	// addresses) as PENDING; a deposit already known by (txid, index) and
	// currently REORGED becomes PENDING again in its new block.
	ApplyBlock(ctx context.Context, b Block) error
	// Rollback forgets every block above ancestor and marks the PENDING
	// deposits in them REORGED. It returns how many CONFIRMED deposits were in
	// the removed blocks: a reorg deeper than the confirmation threshold, which
	// cannot be undone automatically.
	Rollback(ctx context.Context, ancestor int64) (confirmedLost int, err error)
	// Confirm updates confirmation counts of PENDING deposits against head and
	// marks those with at least required confirmations CONFIRMED (emitting the
	// credit event in the same transaction).
	Confirm(ctx context.Context, head, required int64) error
}

// Scanner follows one chain.
type Scanner struct {
	Chain    Chain
	Store    Store
	Name     string // chain name for logs, e.g. "btc"
	Start    int64  // first height to scan when the store is empty
	Required int64  // confirmations needed to credit (>= 1)
	MaxBatch int    // blocks processed per Step (default 100)
	// OnDeepReorg is called when a reorg removed already-confirmed deposits.
	OnDeepReorg func(lost int)
}

// Step processes up to MaxBatch new blocks and updates confirmations. It
// returns the number of blocks applied. Errors are transient: the next Step
// resumes from the store, which never holds a half-applied block.
func (s *Scanner) Step(ctx context.Context) (int, error) {
	if s.Required < 1 {
		return 0, fmt.Errorf("scan %s: required confirmations must be >= 1", s.Name)
	}
	batch := s.MaxBatch
	if batch <= 0 {
		batch = 100
	}
	tip, err := s.Chain.TipHeight(ctx)
	if err != nil {
		return 0, err
	}
	head, headHash, ok, err := s.Store.Head(ctx)
	if err != nil {
		return 0, err
	}
	if ok {
		// The stored head must still be on the best chain (the chain may also
		// have become shorter, e.g. after invalidateblock).
		onChain, err := s.onChain(ctx, head, headHash, tip)
		if err != nil {
			return 0, err
		}
		if !onChain {
			if head, headHash, ok, err = s.reorg(ctx, min(head, tip)); err != nil {
				return 0, err
			}
		}
	}
	applied := 0
	for applied < batch {
		next := s.Start
		if ok {
			next = head + 1
		}
		if next > tip {
			break
		}
		b, err := s.Chain.BlockAt(ctx, next)
		if errors.Is(err, ErrNoBlock) {
			break // tip moved down under us; the next Step handles it
		}
		if err != nil {
			return applied, err
		}
		if ok && b.Parent != headHash {
			if head, headHash, ok, err = s.reorg(ctx, head); err != nil {
				return applied, err
			}
			continue
		}
		if err := s.Store.ApplyBlock(ctx, b); err != nil {
			return applied, err
		}
		head, headHash, ok = b.Height, b.Hash, true
		applied++
	}
	if ok {
		if err := s.Store.Confirm(ctx, head, s.Required); err != nil {
			return applied, err
		}
	}
	return applied, nil
}

func (s *Scanner) onChain(ctx context.Context, height int64, hash string, tip int64) (bool, error) {
	if height > tip {
		return false, nil
	}
	b, err := s.Chain.BlockAt(ctx, height)
	if errors.Is(err, ErrNoBlock) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return b.Hash == hash, nil
}

// reorg walks back from height to the highest stored block that is still on
// the best chain, rolls the store back to it and returns the new head.
func (s *Scanner) reorg(ctx context.Context, from int64) (int64, string, bool, error) {
	h := from
	for ; h >= s.Start; h-- {
		stored, ok, err := s.Store.HashAt(ctx, h)
		if err != nil {
			return 0, "", false, err
		}
		if !ok {
			continue
		}
		b, err := s.Chain.BlockAt(ctx, h)
		if errors.Is(err, ErrNoBlock) {
			continue
		}
		if err != nil {
			return 0, "", false, err
		}
		if b.Hash == stored {
			break
		}
	}
	// h is the common ancestor, or s.Start-1 when no stored block survived.
	lost, err := s.Store.Rollback(ctx, h)
	if err != nil {
		return 0, "", false, err
	}
	slog.WarnContext(ctx, "chain reorganisation", "chain", s.Name, "from", from, "ancestor", h)
	if lost > 0 {
		slog.ErrorContext(ctx, "reorg removed CONFIRMED deposits; operator action required", "chain", s.Name, "deposits", lost)
		if s.OnDeepReorg != nil {
			s.OnDeepReorg(lost)
		}
	}
	if h < s.Start {
		return 0, "", false, nil
	}
	hash, _, err := s.Store.HashAt(ctx, h)
	if err != nil {
		return 0, "", false, err
	}
	return h, hash, true, nil
}
