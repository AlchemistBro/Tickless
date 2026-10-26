package rules

import (
	"errors"
	"fmt"
	"time"

	"github.com/tickless/pkg/money"
)

// ErrDailyLimit is returned when a request would exceed the user's daily cap.
var ErrDailyLimit = errors.New("withdrawals: daily limit exceeded")

// DayOf returns the accounting day of t: the UTC calendar day. Limits reset at
// 00:00 UTC, never in a local timezone, so a user cannot shift the window.
func DayOf(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

// CheckDailyLimit allows a new withdrawal when the amounts already counted for
// the same UTC day (requests that have not FAILED or REJECTED) plus the new one
// stay within limit. Amounts are exact integers; a zero or negative amount is refused.
func CheckDailyLimit(counted []money.Int128, amount, limit money.Int128) error {
	if amount.Sign() <= 0 {
		return fmt.Errorf("withdrawals: amount must be positive")
	}
	total := amount
	for _, c := range counted {
		var err error
		if total, err = total.Add(c); err != nil {
			return err
		}
	}
	if total.Cmp(limit) > 0 {
		return ErrDailyLimit
	}
	return nil
}

// CountsTowardLimit reports whether a request in state s consumes the daily
// limit. A rejected or failed request releases its allowance immediately.
func CountsTowardLimit(s State) bool { return s != Rejected && s != Failed }
