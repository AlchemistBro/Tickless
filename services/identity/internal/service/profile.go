package service

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// Profile is a user's contact data and settings, for internal services
// (notifier, withdrawals).
type Profile struct {
	ID               int64
	Email            string
	Role             string
	Frozen           bool
	Verified         bool
	TOTPEnabled      bool
	NotifyOrderFills bool
}

// GetProfile returns one user's profile.
func (s *Service) GetProfile(ctx context.Context, userID int64) (Profile, error) {
	p := Profile{ID: userID}
	err := s.pool.QueryRow(ctx, `
		SELECT u.email, u.role, u.frozen, u.email_verified_at IS NOT NULL,
		       EXISTS (SELECT 1 FROM totp_secrets t WHERE t.user_id = u.id AND t.confirmed_at IS NOT NULL),
		       u.notify_order_fills
		FROM users u WHERE u.id = $1`, userID).
		Scan(&p.Email, &p.Role, &p.Frozen, &p.Verified, &p.TOTPEnabled, &p.NotifyOrderFills)
	if errors.Is(err, pgx.ErrNoRows) {
		return Profile{}, ErrNotFound
	}
	return p, err
}

// SetNotifyOrderFills switches the opt-in "order filled" emails.
func (s *Service) SetNotifyOrderFills(ctx context.Context, userID int64, on bool) error {
	tag, err := s.pool.Exec(ctx, `UPDATE users SET notify_order_fills = $2 WHERE id = $1`, userID, on)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
