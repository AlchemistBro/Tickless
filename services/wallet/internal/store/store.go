// Package store keeps the wallet's deposit addresses: one per (user, chain),
// derived at an index allocated in the same transaction that stores it.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tickless/pkg/pg"
)

// Address is a stored deposit address.
type Address struct {
	UserID     int64
	Chain      string
	Index      uint32
	Address    string
	Registered bool
}

// DeriveFunc derives the address at index.
type DeriveFunc func(index uint32) (string, error)

// Store is the wallet database.
type Store struct{ pool *pgxpool.Pool }

// New returns a Store over pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) get(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, user int64, chain string) (Address, bool, error) {
	a := Address{UserID: user, Chain: chain}
	var idx int64
	err := q.QueryRow(ctx, `SELECT derivation_index, address, registered FROM deposit_addresses WHERE user_id = $1 AND chain = $2`,
		user, chain).Scan(&idx, &a.Address, &a.Registered)
	if errors.Is(err, pgx.ErrNoRows) {
		return Address{}, false, nil
	}
	if err != nil {
		return Address{}, false, err
	}
	a.Index = uint32(idx) //nolint:gosec // the column CHECK keeps it below 2^31
	return a, true, nil
}

// GetOrCreate returns the user's address on chain, deriving and storing it on
// first use. Concurrent first calls for the same user agree on one address.
func (s *Store) GetOrCreate(ctx context.Context, user int64, chain string, derive DeriveFunc) (Address, error) {
	if a, ok, err := s.get(ctx, s.pool, user, chain); err != nil || ok {
		return a, err
	}
	var out Address
	err := pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		var idx int64
		if err := tx.QueryRow(ctx, `
			INSERT INTO chain_counters (chain, next_index) VALUES ($1, 1)
			ON CONFLICT (chain) DO UPDATE SET next_index = chain_counters.next_index + 1
			RETURNING next_index - 1`, chain).Scan(&idx); err != nil {
			return fmt.Errorf("allocate index: %w", err)
		}
		index := uint32(idx) //nolint:gosec // CHECK keeps next_index below 2^31
		addr, err := derive(index)
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO deposit_addresses (user_id, chain, derivation_index, address) VALUES ($1, $2, $3, $4)
			ON CONFLICT (user_id, chain) DO NOTHING`, user, chain, idx, addr)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			out = Address{UserID: user, Chain: chain, Index: index, Address: addr}
		}
		return nil
	})
	if err != nil {
		return Address{}, err
	}
	if out.Address != "" {
		return out, nil
	}
	// A concurrent call created it first (our allocated index stays unused).
	a, ok, err := s.get(ctx, s.pool, user, chain)
	if err == nil && !ok {
		err = errors.New("wallet: address vanished after a concurrent insert")
	}
	return a, err
}

// MarkRegistered records that chainwatcher knows the address.
func (s *Store) MarkRegistered(ctx context.Context, user int64, chain string) error {
	_, err := s.pool.Exec(ctx, `UPDATE deposit_addresses SET registered = true WHERE user_id = $1 AND chain = $2`, user, chain)
	return err
}
