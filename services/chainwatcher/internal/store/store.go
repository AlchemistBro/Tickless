// Package store is chainwatcher's PostgreSQL state: the deposit address
// registry, the processed blocks of each chain and the deposits found in them.
// ChainStore implements scan.Store; every method is one transaction, and every
// deposit status change enqueues its DepositEvent in the same transaction
// (transactional outbox, ADR 0005).
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/chainwatcher/internal/scan"
	chainwatcherv1 "github.com/tickless/pkg/gen/tickless/chainwatcher/v1"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/outbox"
	"github.com/tickless/pkg/pg"
)

// TopicEvents is where deposit events are published (key: user id).
const TopicEvents = "deposits.events"

// Deposit statuses.
const (
	StatusPending   = "PENDING"
	StatusConfirmed = "CONFIRMED"
	StatusReorged   = "REORGED"
)

// ErrAddressTaken is returned when an address is registered to another user.
var ErrAddressTaken = errors.New("chainwatcher: address belongs to another user")

// AddressKey is the case-normalised address used for matching transfers.
func AddressKey(address string) string { return strings.ToLower(strings.TrimSpace(address)) }

// Store is the chainwatcher database.
type Store struct{ pool *pgxpool.Pool }

// New returns a Store over pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// RegisterAddress adds a deposit address to the watch list. Idempotent for
// the same owner.
func (s *Store) RegisterAddress(ctx context.Context, chain, address string, user int64, index uint32) error {
	key := AddressKey(address)
	if chain == "" || key == "" || user <= 0 {
		return fmt.Errorf("chainwatcher: chain, address and user are required")
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO deposit_addresses (chain, address, address_key, user_id, derivation_index)
		VALUES ($1, $2, $3, $4, $5) ON CONFLICT (chain, address_key) DO NOTHING`,
		chain, address, key, user, int64(index)); err != nil {
		return err
	}
	var owner int64
	if err := s.pool.QueryRow(ctx, `SELECT user_id FROM deposit_addresses WHERE chain = $1 AND address_key = $2`, chain, key).Scan(&owner); err != nil {
		return err
	}
	if owner != user {
		return ErrAddressTaken
	}
	return nil
}

// Deposit is a deposit row.
type Deposit struct {
	ID            int64
	Chain         string
	TxID          string
	Index         int64
	Address       string
	UserID        int64
	Asset         string
	Amount        money.Int128
	Status        string
	BlockHeight   int64
	BlockHash     string
	Confirmations int64
	DetectedAt    time.Time
	ConfirmedAt   *time.Time
}

const depositCols = `id, chain, txid, idx, address, user_id, asset, amount::text, status, block_height, block_hash, confirmations, detected_at, confirmed_at`

func scanDeposit(row pgx.Row) (Deposit, error) {
	var d Deposit
	var amount string
	if err := row.Scan(&d.ID, &d.Chain, &d.TxID, &d.Index, &d.Address, &d.UserID, &d.Asset, &amount, &d.Status,
		&d.BlockHeight, &d.BlockHash, &d.Confirmations, &d.DetectedAt, &d.ConfirmedAt); err != nil {
		return Deposit{}, err
	}
	v, err := money.ParseInt128(amount)
	if err != nil {
		return Deposit{}, fmt.Errorf("deposit %d amount: %w", d.ID, err)
	}
	d.Amount = v
	return d, nil
}

func collect(rows pgx.Rows) ([]Deposit, error) {
	defer rows.Close()
	var out []Deposit
	for rows.Next() {
		d, err := scanDeposit(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ListDeposits returns a user's deposits, newest first.
func (s *Store) ListDeposits(ctx context.Context, user, beforeID int64, limit int) ([]Deposit, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if beforeID <= 0 {
		beforeID = 1<<63 - 1
	}
	rows, err := s.pool.Query(ctx, `SELECT `+depositCols+` FROM deposits WHERE user_id = $1 AND id < $2 ORDER BY id DESC LIMIT $3`,
		user, beforeID, limit)
	if err != nil {
		return nil, err
	}
	return collect(rows)
}

// Chain returns the scan.Store of one chain.
func (s *Store) Chain(name string) *ChainStore { return &ChainStore{pool: s.pool, chain: name} }

// ChainStore implements scan.Store for one chain.
type ChainStore struct {
	pool  *pgxpool.Pool
	chain string
}

var _ scan.Store = (*ChainStore)(nil)

// Head implements scan.Store.
func (c *ChainStore) Head(ctx context.Context) (int64, string, bool, error) {
	var h int64
	var hash string
	err := c.pool.QueryRow(ctx, `SELECT height, hash FROM blocks WHERE chain = $1 ORDER BY height DESC LIMIT 1`, c.chain).Scan(&h, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", false, nil
	}
	if err != nil {
		return 0, "", false, err
	}
	return h, hash, true, nil
}

// HashAt implements scan.Store.
func (c *ChainStore) HashAt(ctx context.Context, height int64) (string, bool, error) {
	var hash string
	err := c.pool.QueryRow(ctx, `SELECT hash FROM blocks WHERE chain = $1 AND height = $2`, c.chain, height).Scan(&hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return hash, err == nil, err
}

// ApplyBlock implements scan.Store.
func (c *ChainStore) ApplyBlock(ctx context.Context, b scan.Block) error {
	return pg.InTx(ctx, c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO blocks (chain, height, hash, parent_hash) VALUES ($1, $2, $3, $4)`,
			c.chain, b.Height, b.Hash, b.Parent); err != nil {
			return fmt.Errorf("record block %d: %w", b.Height, err)
		}
		if len(b.Transfers) == 0 {
			return nil
		}
		keys := make([]string, 0, len(b.Transfers))
		for _, t := range b.Transfers {
			keys = append(keys, AddressKey(t.Address))
		}
		owners := map[string]int64{}
		rows, err := tx.Query(ctx, `SELECT address_key, user_id FROM deposit_addresses WHERE chain = $1 AND address_key = ANY($2)`, c.chain, keys)
		if err != nil {
			return err
		}
		for rows.Next() {
			var k string
			var u int64
			if err := rows.Scan(&k, &u); err != nil {
				rows.Close()
				return err
			}
			owners[k] = u
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, t := range b.Transfers {
			user, ok := owners[AddressKey(t.Address)]
			if !ok || t.Amount.Sign() <= 0 {
				continue
			}
			// New deposit, or a REORGED one re-mined in this block. A PENDING or
			// CONFIRMED deposit with the same identity is left untouched.
			row := tx.QueryRow(ctx, `
				INSERT INTO deposits (chain, txid, idx, address, user_id, asset, amount, status, block_height, block_hash)
				VALUES ($1, $2, $3, $4, $5, $6, $7::numeric, 'PENDING', $8, $9)
				ON CONFLICT (chain, txid, idx) DO UPDATE
				   SET status = 'PENDING', block_height = EXCLUDED.block_height, block_hash = EXCLUDED.block_hash, confirmations = 0
				 WHERE deposits.status = 'REORGED'
				RETURNING `+depositCols,
				c.chain, t.TxID, t.Index, t.Address, user, t.Asset, t.Amount.String(), b.Height, b.Hash)
			d, err := scanDeposit(row)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			if err := enqueue(ctx, tx, chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_DETECTED, d); err != nil {
				return err
			}
		}
		return nil
	})
}

// Rollback implements scan.Store.
func (c *ChainStore) Rollback(ctx context.Context, ancestor int64) (int, error) {
	var lost int
	err := pg.InTx(ctx, c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM blocks WHERE chain = $1 AND height > $2`, c.chain, ancestor); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			UPDATE deposits SET status = 'REORGED', confirmations = 0
			 WHERE chain = $1 AND status = 'PENDING' AND block_height > $2
			RETURNING `+depositCols, c.chain, ancestor)
		if err != nil {
			return err
		}
		reorged, err := collect(rows)
		if err != nil {
			return err
		}
		for _, d := range reorged {
			if err := enqueue(ctx, tx, chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_REORGED, d); err != nil {
				return err
			}
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM deposits WHERE chain = $1 AND status = 'CONFIRMED' AND block_height > $2`,
			c.chain, ancestor).Scan(&lost)
	})
	return lost, err
}

// Confirm implements scan.Store.
func (c *ChainStore) Confirm(ctx context.Context, head, required int64) error {
	return pg.InTx(ctx, c.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE deposits SET confirmations = $2 - block_height + 1
			 WHERE chain = $1 AND status = 'PENDING' AND confirmations <> $2 - block_height + 1`, c.chain, head); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			UPDATE deposits SET status = 'CONFIRMED', confirmed_at = now()
			 WHERE chain = $1 AND status = 'PENDING' AND confirmations >= $2
			RETURNING `+depositCols, c.chain, required)
		if err != nil {
			return err
		}
		confirmed, err := collect(rows)
		if err != nil {
			return err
		}
		for _, d := range confirmed {
			if err := enqueue(ctx, tx, chainwatcherv1.DepositEventType_DEPOSIT_EVENT_TYPE_CONFIRMED, d); err != nil {
				return err
			}
		}
		return nil
	})
}

// Event builds the DepositEvent of d.
func Event(typ chainwatcherv1.DepositEventType, d Deposit) *chainwatcherv1.DepositEvent {
	return &chainwatcherv1.DepositEvent{
		Meta:          &commonv1.EventMeta{EventId: uuid.Must(uuid.NewV7()).String(), ProducedAtUnixNano: time.Now().UnixNano()},
		Type:          typ,
		UserId:        d.UserID,
		Chain:         d.Chain,
		Txid:          d.TxID,
		Index:         d.Index,
		Asset:         d.Asset,
		Amount:        &commonv1.Amount{Units: d.Amount.String()},
		Address:       d.Address,
		BlockHeight:   d.BlockHeight,
		BlockHash:     d.BlockHash,
		Confirmations: d.Confirmations,
	}
}

func enqueue(ctx context.Context, tx pgx.Tx, typ chainwatcherv1.DepositEventType, d Deposit) error {
	payload, err := proto.Marshal(Event(typ, d))
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, tx, TopicEvents, fmt.Sprint(d.UserID), payload)
}
