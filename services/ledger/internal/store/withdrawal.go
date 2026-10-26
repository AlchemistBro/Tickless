package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/tickless/pkg/money"
)

// Withdrawal hold errors.
var (
	ErrWithdrawalNotFound = errors.New("ledger: no hold for this withdrawal")
	// ErrWithdrawalFinished means the hold already ended the other way
	// (settle after release or release after settle).
	ErrWithdrawalFinished = errors.New("ledger: withdrawal hold already finished differently")
)

// HoldWithdrawal moves amount of asset from the user's available to held
// balance. Idempotent by withdrawalID; ErrInsufficientFunds if the available
// balance is too small (nothing is changed then).
func (s *Store) HoldWithdrawal(ctx context.Context, withdrawalID, user int64, asset string, amount money.Int128) (already bool, err error) {
	if amount.Sign() <= 0 {
		return false, ErrInvalidAmount
	}
	if _, ok := money.LookupAsset(asset); !ok {
		return false, fmt.Errorf("%w: unknown asset %q", ErrInvalidAmount, asset)
	}
	err = s.inSession(ctx, "withdrawal_hold", func(ss *session) error {
		tag, err := ss.tx.Exec(ctx, `
			INSERT INTO withdrawal_holds (withdrawal_id, user_id, asset, amount, status)
			VALUES ($1, $2, $3, $4::numeric, 'ACTIVE') ON CONFLICT (withdrawal_id) DO NOTHING`,
			withdrawalID, user, asset, amount.String())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			var u int64
			var a, amt string
			if err := ss.tx.QueryRow(ctx, `SELECT user_id, asset, amount::text FROM withdrawal_holds WHERE withdrawal_id = $1`, withdrawalID).
				Scan(&u, &a, &amt); err != nil {
				return err
			}
			if u != user || a != asset || amt != amount.String() {
				return fmt.Errorf("ledger: withdrawal %d already held with different terms", withdrawalID)
			}
			already = true
			return nil
		}
		neg, err := amount.Neg()
		if err != nil {
			return err
		}
		_, _, err = ss.post(ctx, fmt.Sprintf("withdrawal-hold:%d", withdrawalID), "withdrawal_hold", []posting{
			{available(user, asset), neg}, {held(user, asset), amount},
		})
		return err
	})
	return already, err
}

// SettleWithdrawal moves the held amount to EXTERNAL: the funds left the exchange.
func (s *Store) SettleWithdrawal(ctx context.Context, withdrawalID int64) (already bool, err error) {
	return s.finishWithdrawal(ctx, withdrawalID, "SETTLED", "withdrawal")
}

// ReleaseWithdrawal returns the held amount to available.
func (s *Store) ReleaseWithdrawal(ctx context.Context, withdrawalID int64) (already bool, err error) {
	return s.finishWithdrawal(ctx, withdrawalID, "RELEASED", "withdrawal_release")
}

func (s *Store) finishWithdrawal(ctx context.Context, withdrawalID int64, to, kind string) (already bool, err error) {
	err = s.inSession(ctx, kind, func(ss *session) error {
		var user int64
		var asset, amt string
		err := ss.tx.QueryRow(ctx, `
			UPDATE withdrawal_holds SET status = $2, finished_at = now()
			 WHERE withdrawal_id = $1 AND status = 'ACTIVE'
			RETURNING user_id, asset, amount::text`, withdrawalID, to).Scan(&user, &asset, &amt)
		if errors.Is(err, pgx.ErrNoRows) {
			var st string
			err := ss.tx.QueryRow(ctx, `SELECT status FROM withdrawal_holds WHERE withdrawal_id = $1`, withdrawalID).Scan(&st)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				return ErrWithdrawalNotFound
			case err != nil:
				return err
			case st == to:
				already = true
				return nil
			default:
				return fmt.Errorf("%w: withdrawal %d is %s", ErrWithdrawalFinished, withdrawalID, st)
			}
		}
		if err != nil {
			return err
		}
		amount, err := money.ParseInt128(amt)
		if err != nil {
			return err
		}
		neg, err := amount.Neg()
		if err != nil {
			return err
		}
		other := available(user, asset)
		key := fmt.Sprintf("withdrawal-release:%d", withdrawalID)
		if to == "SETTLED" {
			other = acctKey{SystemUser, asset, KindExternal}
			key = fmt.Sprintf("withdrawal-settle:%d", withdrawalID)
		}
		_, _, err = ss.post(ctx, key, kind, []posting{{held(user, asset), neg}, {other, amount}})
		return err
	})
	return already, err
}
