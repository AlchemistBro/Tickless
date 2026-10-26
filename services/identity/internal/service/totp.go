package service

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/tickless/pkg/totp"
)

// Two-factor errors.
var (
	ErrTOTPNotSetUp  = errors.New("identity: two-factor authentication is not set up")
	ErrTOTPConfirmed = errors.New("identity: two-factor authentication is already confirmed")
	ErrTOTPInvalid   = errors.New("identity: invalid two-factor code")
)

func totpAAD(userID int64) []byte { return []byte("totp:" + strconv.FormatInt(userID, 10)) }

// EnrollTOTP creates (or replaces, until confirmed) the user's TOTP secret and
// returns it in base32 for the authenticator app. The secret is sealed at rest.
func (s *Service) EnrollTOTP(ctx context.Context, userID int64) (string, error) {
	secret, err := totp.NewSecret()
	if err != nil {
		return "", err
	}
	enc, err := s.o.Sealer.Seal(secret, totpAAD(userID))
	if err != nil {
		return "", err
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO totp_secrets (user_id, secret_enc, last_used_step) VALUES ($1, $2, -1)
		ON CONFLICT (user_id) DO UPDATE SET secret_enc = EXCLUDED.secret_enc, last_used_step = -1
		  WHERE totp_secrets.confirmed_at IS NULL`, userID, enc)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "", ErrTOTPConfirmed
	}
	return totp.Encode(secret), nil
}

type totpRow struct {
	secret    []byte
	confirmed bool
	lastUsed  int64
}

func (s *Service) loadTOTP(ctx context.Context, userID int64) (totpRow, error) {
	var enc []byte
	var r totpRow
	err := s.pool.QueryRow(ctx, `SELECT secret_enc, confirmed_at IS NOT NULL, last_used_step FROM totp_secrets WHERE user_id = $1`, userID).
		Scan(&enc, &r.confirmed, &r.lastUsed)
	if errors.Is(err, pgx.ErrNoRows) {
		return totpRow{}, ErrTOTPNotSetUp
	}
	if err != nil {
		return totpRow{}, err
	}
	r.secret, err = s.o.Sealer.Open(enc, totpAAD(userID))
	if err != nil {
		return totpRow{}, fmt.Errorf("identity: cannot decrypt totp secret of user %d: %w", userID, err)
	}
	return r, nil
}

// ConfirmTOTP proves the user has the authenticator app by accepting one code
// and activates two-factor authentication.
func (s *Service) ConfirmTOTP(ctx context.Context, userID int64, code string) error {
	r, err := s.loadTOTP(ctx, userID)
	if err != nil {
		return err
	}
	if r.confirmed {
		return ErrTOTPConfirmed
	}
	step, err := totp.Verify(r.secret, code, s.o.Now(), -1)
	if err != nil {
		return ErrTOTPInvalid
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE totp_secrets SET confirmed_at = now(), last_used_step = $2
		 WHERE user_id = $1 AND confirmed_at IS NULL`, userID, step)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrTOTPConfirmed // a concurrent confirmation won
	}
	return nil
}

// VerifyTOTP checks a second-factor code for a sensitive action (withdrawals).
// Each code is accepted once: the step is recorded with a guarded update, so
// concurrent requests carrying the same code cannot both succeed.
func (s *Service) VerifyTOTP(ctx context.Context, userID int64, code string) error {
	r, err := s.loadTOTP(ctx, userID)
	if err != nil {
		return err
	}
	if !r.confirmed {
		return ErrTOTPNotSetUp
	}
	step, err := totp.Verify(r.secret, code, s.o.Now(), r.lastUsed)
	if err != nil {
		return ErrTOTPInvalid
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE totp_secrets SET last_used_step = $2
		 WHERE user_id = $1 AND confirmed_at IS NOT NULL AND last_used_step < $2`, userID, step)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrTOTPInvalid // replayed by a concurrent request
	}
	return nil
}
