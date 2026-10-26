// Package rules holds the pure decision logic of withdrawals: the state
// machine, destination address validation, the daily limit and the review
// threshold. It has no database or network access, so every rule is tested in
// isolation and the store only persists decisions made here.
package rules

import (
	"errors"
	"fmt"

	"github.com/tickless/pkg/money"
)

// State of a withdrawal.
type State string

// Withdrawal states (SPEC §1 withdrawals state machine).
const (
	Requested     State = "REQUESTED"      // accepted, funds held in the ledger
	Screening     State = "SCREENING"      // destination being checked against the denylist
	PendingReview State = "PENDING_REVIEW" // above the threshold: waits for an admin
	Approved      State = "APPROVED"       // may be signed and broadcast
	Signing       State = "SIGNING"        // handed to the signer
	Broadcast     State = "BROADCAST"      // in the mempool; confirmations pending
	Confirmed     State = "CONFIRMED"      // final: settled to EXTERNAL
	Rejected      State = "REJECTED"       // final: screening or an admin said no; hold released
	Failed        State = "FAILED"         // final: could not be broadcast; hold released
)

// Final reports whether no further transition is possible.
func (s State) Final() bool { return s == Confirmed || s == Rejected || s == Failed }

// transitions lists every allowed edge. Anything else is a bug or a stale
// request and must be refused by the store (the guard is the expected from-state).
var transitions = map[State][]State{
	Requested:     {Screening},
	Screening:     {PendingReview, Approved, Rejected},
	PendingReview: {Approved, Rejected},
	Approved:      {Signing, Failed},
	Signing:       {Broadcast, Failed},
	Broadcast:     {Confirmed, Failed},
}

// ErrTransition is returned for a transition that is not allowed.
var ErrTransition = errors.New("withdrawals: invalid state transition")

// Next validates that from -> to is allowed.
func Next(from, to State) error {
	for _, t := range transitions[from] {
		if t == to {
			return nil
		}
	}
	return fmt.Errorf("%w: %s -> %s", ErrTransition, from, to)
}

// Route decides the first step after screening passed: small withdrawals are
// executed automatically, larger ones wait for an admin (SPEC §1).
func Route(amount, threshold money.Int128) State {
	if amount.Cmp(threshold) > 0 {
		return PendingReview
	}
	return Approved
}
