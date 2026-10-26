// Package store persists withdrawals. Every state change is a guarded UPDATE
// (WHERE status = expected) in one transaction with its outbox event, so an
// update based on a stale state fails instead of silently overwriting it.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	withdrawalsv1 "github.com/tickless/pkg/gen/tickless/withdrawals/v1"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/outbox"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/withdrawals/internal/rules"
)

// TopicEvents carries every withdrawal state transition.
const TopicEvents = "withdrawals.events"

// Errors.
var (
	ErrNotFound = errors.New("withdrawals: not found")
	// ErrStale means the withdrawal is not in the state the caller expected:
	// another actor moved it first.
	ErrStale = errors.New("withdrawals: state changed concurrently")
)

// Withdrawal is one withdrawal request.
type Withdrawal struct {
	ID          int64
	UserID      int64
	Chain       string
	Asset       string
	Amount      money.Int128
	Destination string
	State       rules.State
	Reason      string
	TxID        string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Store is the withdrawals database.
type Store struct{ pool *pgxpool.Pool }

// New returns a Store over pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Request validates the destination, checks the user's daily limit for the
// asset (amounts of different assets are never added together) and records a
// REQUESTED withdrawal. The limit check and the insert run under a per-user
// transaction-scoped advisory lock, so concurrent requests cannot both fit.
func (s *Store) Request(ctx context.Context, userID int64, chain, asset string, amount money.Int128, destination string, dailyLimit money.Int128, now time.Time) (Withdrawal, error) {
	if err := rules.ValidateAddress(chain, destination); err != nil {
		return Withdrawal{}, err
	}
	day := rules.DayOf(now)
	var w Withdrawal
	err := pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, userID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT amount::text, status FROM withdrawals WHERE user_id = $1 AND day = $2 AND asset = $3`, userID, day, asset)
		if err != nil {
			return err
		}
		var counted []money.Int128
		for rows.Next() {
			var amt, st string
			if err := rows.Scan(&amt, &st); err != nil {
				rows.Close()
				return err
			}
			if !rules.CountsTowardLimit(rules.State(st)) {
				continue
			}
			v, err := money.ParseInt128(amt)
			if err != nil {
				rows.Close()
				return err
			}
			counted = append(counted, v)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if err := rules.CheckDailyLimit(counted, amount, dailyLimit); err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `
			INSERT INTO withdrawals (user_id, chain, asset, amount, destination, status, day, created_at, updated_at)
			VALUES ($1, $2, $3, $4::numeric, $5, $6, $7, $8, $8)
			RETURNING id`, userID, chain, asset, amount.String(), destination, string(rules.Requested), day, now).Scan(&w.ID)
		if err != nil {
			return err
		}
		w = Withdrawal{ID: w.ID, UserID: userID, Chain: chain, Asset: asset, Amount: amount, Destination: destination,
			State: rules.Requested, CreatedAt: now, UpdatedAt: now}
		return enqueue(ctx, tx, w, rules.Requested, "requested")
	})
	return w, err
}

// Transition moves withdrawal id from `from` to `to`. It fails with ErrStale
// when the stored state is not `from`, and with rules.ErrTransition when the
// edge does not exist.
func (s *Store) Transition(ctx context.Context, id int64, from, to rules.State, reason, txid string, now time.Time) error {
	if err := rules.Next(from, to); err != nil {
		return err
	}
	return pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		var w Withdrawal
		var amt string
		err := tx.QueryRow(ctx, `
			UPDATE withdrawals SET status = $3, reason = $4, txid = COALESCE(NULLIF($5, ''), txid), updated_at = $6
			 WHERE id = $1 AND status = $2
			RETURNING id, user_id, chain, asset, amount::text, destination, reason, COALESCE(txid, ''), created_at, updated_at`,
			id, string(from), string(to), reason, txid, now).
			Scan(&w.ID, &w.UserID, &w.Chain, &w.Asset, &amt, &w.Destination, &w.Reason, &w.TxID, &w.CreatedAt, &w.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM withdrawals WHERE id = $1)`, id).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return ErrNotFound
			}
			return ErrStale
		}
		if err != nil {
			return err
		}
		v, err := money.ParseInt128(amt)
		if err != nil {
			return err
		}
		w.Amount = v
		w.State = to
		return enqueue(ctx, tx, w, from, reason)
	})
}

// Get returns one withdrawal.
func (s *Store) Get(ctx context.Context, id int64) (Withdrawal, error) {
	w, err := scan(s.pool.QueryRow(ctx, `SELECT id, user_id, chain, asset, amount::text, destination, status, reason, COALESCE(txid, ''), created_at, updated_at FROM withdrawals WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Withdrawal{}, ErrNotFound
	}
	return w, err
}

// ListForUser returns a user's withdrawals, newest first.
func (s *Store) ListForUser(ctx context.Context, userID int64) ([]Withdrawal, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, user_id, chain, asset, amount::text, destination, status, reason, COALESCE(txid, ''), created_at, updated_at FROM withdrawals WHERE user_id = $1 ORDER BY id DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Withdrawal
	for rows.Next() {
		w, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scan(r rowScanner) (Withdrawal, error) {
	var w Withdrawal
	var amt, st string
	if err := r.Scan(&w.ID, &w.UserID, &w.Chain, &w.Asset, &amt, &w.Destination, &st, &w.Reason, &w.TxID, &w.CreatedAt, &w.UpdatedAt); err != nil {
		return Withdrawal{}, err
	}
	v, err := money.ParseInt128(amt)
	if err != nil {
		return Withdrawal{}, fmt.Errorf("withdrawal %d amount: %w", w.ID, err)
	}
	w.Amount, w.State = v, rules.State(st)
	return w, nil
}

func enqueue(ctx context.Context, tx pgx.Tx, w Withdrawal, from rules.State, reason string) error {
	msg, err := proto.Marshal(&withdrawalsv1.WithdrawalEvent{
		Meta:         &commonv1.EventMeta{EventId: uuid.Must(uuid.NewV7()).String(), ProducedAtUnixNano: time.Now().UnixNano()},
		WithdrawalId: w.ID, UserId: w.UserID, Chain: w.Chain, Asset: w.Asset,
		Amount: &commonv1.Amount{Units: w.Amount.String()}, Destination: w.Destination,
		FromState: string(from), ToState: string(w.State), Reason: reason,
	})
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, tx, TopicEvents, fmt.Sprint(w.UserID), msg)
}
