// Package engine drives withdrawals through their state machine (ADR 0011):
//
//	request   TOTP check, daily limit, ledger hold, queue screening
//	screen    denylist; above the threshold -> PENDING_REVIEW (review task), else APPROVED
//	review    an admin approves or rejects a PENDING_REVIEW withdrawal
//	execute   APPROVED -> SIGNING -> signer (mTLS, idempotent) -> broadcast -> BROADCAST
//	confirm   BROADCAST -> CONFIRMED at the chain's threshold, or FAILED if reverted
//	finalize  apply the ledger effect of final states (settle / release) exactly once
//
// Every state change is a guarded transition, so concurrent workers or admins
// cannot both win. The ledger effect of a final state is applied after the
// transition (the transition decides) and is idempotent; the finaliser repairs
// a crash between the two. Funds stay HELD until the transaction is confirmed,
// so a reverted transaction only needs a release.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/tickless/pkg/money"
	"github.com/tickless/withdrawals/internal/rules"
	"github.com/tickless/withdrawals/internal/store"
)

// Errors returned to API callers.
var (
	ErrInsufficientFunds = errors.New("withdrawals: insufficient available balance")
	ErrTOTPRequired      = errors.New("withdrawals: two-factor authentication must be enabled")
	ErrTOTPInvalid       = errors.New("withdrawals: invalid two-factor code")
	ErrUnsupported       = errors.New("withdrawals: unsupported chain/asset")
	ErrNotYours          = errors.New("withdrawals: not found")
)

// Ledger is the part of the ledger the engine uses. HoldWithdrawal returns
// ErrInsufficientFunds when the available balance is too small.
type Ledger interface {
	HoldWithdrawal(ctx context.Context, id, user int64, asset string, amount money.Int128) error
	SettleWithdrawal(ctx context.Context, id int64) error
	ReleaseWithdrawal(ctx context.Context, id int64) error
}

// ErrRetry marks a signer/chain failure worth retrying later (e.g. the hot
// wallet's change is not confirmed yet, the node is down).
var ErrRetry = errors.New("withdrawals: temporary failure")

// Signed is a signed transaction.
type Signed struct {
	TxID       string
	Raw        []byte
	NetworkFee string
	FeeAsset   string
}

// Signer signs a withdrawal's payment. Idempotent by withdrawal id.
type Signer interface {
	Sign(ctx context.Context, w store.Withdrawal) (Signed, error)
}

// Chain broadcasts and tracks transactions of one chain.
type Chain interface {
	// Broadcast submits raw; nil when accepted or already known.
	Broadcast(ctx context.Context, raw []byte, txid string) error
	// Confirmations returns the confirmation count (0 = in the mempool), -1 if
	// the node does not know the transaction, and reverted=true for a mined
	// transaction that failed (Ethereum receipt status 0).
	Confirmations(ctx context.Context, txid string) (confs int64, reverted bool, err error)
}

// Verifier checks second-factor codes (identity).
type Verifier interface {
	VerifyTOTP(ctx context.Context, user int64, code string) error
}

// Queue publishes work for asynchronous steps (RabbitMQ).
type Queue interface {
	PublishScreening(ctx context.Context, id int64) error
	PublishReview(ctx context.Context, w store.Withdrawal) error
}

// Config holds limits and thresholds per asset, confirmations per chain.
type Config struct {
	DailyLimit    map[string]money.Int128
	ReviewAbove   map[string]money.Int128
	Confirmations map[string]int64
	Denylist      *rules.Denylist
	RetryAfter    time.Duration // backoff for signing/broadcast retries
	Now           func() time.Time
}

// Supported lists the payable (chain, asset) pairs.
var Supported = map[string]string{"BTC": "btc", "ETH": "eth", "USDT": "eth"}

// Engine is the withdrawal workflow.
type Engine struct {
	St       *store.Store
	Ledger   Ledger
	Signer   Signer
	Chains   map[string]Chain
	Verifier Verifier
	Queue    Queue
	Cfg      Config
}

func (e *Engine) now() time.Time {
	if e.Cfg.Now != nil {
		return e.Cfg.Now()
	}
	return time.Now()
}

// Request accepts a withdrawal: valid pair, valid TOTP, within the daily
// limit, funds held. A transient ledger error leaves the request REQUESTED;
// the screening step retries the (idempotent) hold.
func (e *Engine) Request(ctx context.Context, user int64, chain, asset string, amount money.Int128, dest, code string) (store.Withdrawal, error) {
	if Supported[asset] != chain {
		return store.Withdrawal{}, ErrUnsupported
	}
	if err := e.Verifier.VerifyTOTP(ctx, user, code); err != nil {
		return store.Withdrawal{}, err
	}
	limit, ok := e.Cfg.DailyLimit[asset]
	if !ok {
		return store.Withdrawal{}, ErrUnsupported
	}
	w, err := e.St.Request(ctx, user, chain, asset, amount, dest, limit, e.now())
	if err != nil {
		return store.Withdrawal{}, err
	}
	switch err := e.Ledger.HoldWithdrawal(ctx, w.ID, user, asset, amount); {
	case errors.Is(err, ErrInsufficientFunds):
		if terr := e.St.Transition(ctx, w.ID, rules.Requested, rules.Failed, "insufficient funds", "", e.now()); terr != nil {
			return store.Withdrawal{}, terr
		}
		return store.Withdrawal{}, ErrInsufficientFunds
	case err != nil:
		slog.WarnContext(ctx, "ledger hold deferred to screening", "withdrawal", w.ID, "err", err)
	}
	if err := e.Queue.PublishScreening(ctx, w.ID); err != nil {
		slog.WarnContext(ctx, "screening publish failed; the sweeper will retry", "withdrawal", w.ID, "err", err)
	}
	return e.St.Get(ctx, w.ID)
}

// Screen runs the screening step for one withdrawal. It is idempotent: a
// redelivered task for a withdrawal past screening does nothing.
func (e *Engine) Screen(ctx context.Context, id int64) error {
	w, err := e.St.Get(ctx, id)
	if err != nil {
		return err
	}
	if w.State == rules.Requested {
		switch err := e.Ledger.HoldWithdrawal(ctx, w.ID, w.UserID, w.Asset, w.Amount); {
		case errors.Is(err, ErrInsufficientFunds):
			return ignoreStale(e.St.Transition(ctx, id, rules.Requested, rules.Failed, "insufficient funds", "", e.now()))
		case err != nil:
			return err
		}
		if err := ignoreStale(e.St.Transition(ctx, id, rules.Requested, rules.Screening, "", "", e.now())); err != nil {
			return err
		}
		if w, err = e.St.Get(ctx, id); err != nil {
			return err
		}
	}
	if w.State != rules.Screening {
		return nil
	}
	if e.Cfg.Denylist != nil && e.Cfg.Denylist.Listed(w.Destination) {
		if err := e.St.Transition(ctx, id, rules.Screening, rules.Rejected, "destination is on the denylist", "", e.now()); err != nil {
			return ignoreStale(err)
		}
		e.finalize(ctx, id)
		return nil
	}
	threshold, ok := e.Cfg.ReviewAbove[w.Asset]
	next := rules.Approved
	if !ok || rules.Route(w.Amount, threshold) == rules.PendingReview {
		next = rules.PendingReview
	}
	if err := e.St.Transition(ctx, id, rules.Screening, next, "", "", e.now()); err != nil {
		return ignoreStale(err)
	}
	if next == rules.PendingReview {
		w.State = next
		if err := e.Queue.PublishReview(ctx, w); err != nil {
			slog.WarnContext(ctx, "review task publish failed", "withdrawal", id, "err", err)
		}
	}
	return nil
}

func ignoreStale(err error) error {
	if errors.Is(err, store.ErrStale) {
		return nil // another worker already moved it: nothing left to do here
	}
	return err
}

// Review applies an admin decision to a PENDING_REVIEW withdrawal.
func (e *Engine) Review(ctx context.Context, id int64, approve bool, admin int64, reason string) (store.Withdrawal, error) {
	to := rules.Approved
	if !approve {
		to = rules.Rejected
		if reason == "" {
			reason = "rejected by an administrator"
		}
	}
	if err := e.St.Transition(ctx, id, rules.PendingReview, to, reason, "", e.now()); err != nil {
		return store.Withdrawal{}, err
	}
	if err := e.St.MarkReviewed(ctx, id, admin); err != nil {
		return store.Withdrawal{}, err
	}
	if !approve {
		e.finalize(ctx, id) // release now; the finaliser retries on failure
	}
	return e.St.Get(ctx, id)
}

// ExecuteOnce signs and broadcasts approved withdrawals (and resumes
// interrupted ones). It returns how many it moved to BROADCAST.
func (e *Engine) ExecuteOnce(ctx context.Context) (int, error) {
	approved, err := e.St.ListByState(ctx, rules.Approved, e.now(), 50)
	if err != nil {
		return 0, err
	}
	for _, w := range approved {
		if err := ignoreStale(e.St.Transition(ctx, w.ID, rules.Approved, rules.Signing, "", "", e.now())); err != nil {
			return 0, err
		}
	}
	signing, err := e.St.ListByState(ctx, rules.Signing, e.now(), 50)
	if err != nil {
		return 0, err
	}
	moved := 0
	for _, w := range signing {
		ok, err := e.execute(ctx, w)
		if err != nil {
			return moved, err
		}
		if ok {
			moved++
		}
	}
	return moved, nil
}

func (e *Engine) backoff(ctx context.Context, w store.Withdrawal, why error) {
	slog.WarnContext(ctx, "withdrawal step will be retried", "withdrawal", w.ID, "state", w.State, "err", why)
	if err := e.St.Touch(ctx, w.ID, e.now().Add(e.Cfg.RetryAfter), why.Error()); err != nil {
		slog.ErrorContext(ctx, "cannot record retry", "withdrawal", w.ID, "err", err)
	}
}

func (e *Engine) execute(ctx context.Context, w store.Withdrawal) (bool, error) {
	ch, ok := e.Chains[w.Chain]
	if !ok {
		return false, fmt.Errorf("no chain %q configured", w.Chain)
	}
	signed, err := e.Signer.Sign(ctx, w)
	if errors.Is(err, ErrRetry) {
		e.backoff(ctx, w, err)
		return false, nil
	}
	if err != nil {
		if terr := ignoreStale(e.St.Transition(ctx, w.ID, rules.Signing, rules.Failed, "signing refused: "+err.Error(), "", e.now())); terr != nil {
			return false, terr
		}
		e.finalize(ctx, w.ID)
		return false, nil
	}
	if err := e.St.SetExecution(ctx, w.ID, signed.NetworkFee, signed.FeeAsset, 0); err != nil {
		return false, err
	}
	if err := ch.Broadcast(ctx, signed.Raw, signed.TxID); err != nil {
		e.backoff(ctx, w, err)
		return false, nil
	}
	if err := e.St.Transition(ctx, w.ID, rules.Signing, rules.Broadcast, "", signed.TxID, e.now()); err != nil {
		return false, ignoreStale(err)
	}
	slog.InfoContext(ctx, "withdrawal broadcast", "withdrawal", w.ID, "chain", w.Chain, "txid", signed.TxID)
	return true, nil
}

// ConfirmOnce tracks broadcast transactions. It returns how many reached the
// confirmation threshold.
func (e *Engine) ConfirmOnce(ctx context.Context) (int, error) {
	rows, err := e.St.ListByState(ctx, rules.Broadcast, e.now(), 200)
	if err != nil {
		return 0, err
	}
	done := 0
	for _, w := range rows {
		ch := e.Chains[w.Chain]
		confs, reverted, err := ch.Confirmations(ctx, w.TxID)
		if err != nil {
			slog.WarnContext(ctx, "confirmation check failed", "withdrawal", w.ID, "err", err)
			continue
		}
		switch {
		case reverted:
			if err := ignoreStale(e.St.Transition(ctx, w.ID, rules.Broadcast, rules.Failed, "transaction reverted on chain", "", e.now())); err != nil {
				return done, err
			}
			e.finalize(ctx, w.ID)
		case confs < 0:
			// Dropped (e.g. a reorg evicted it): broadcast the same signed
			// transaction again; the signer returns it from its journal.
			if signed, err := e.Signer.Sign(ctx, w); err == nil {
				if err := ch.Broadcast(ctx, signed.Raw, signed.TxID); err != nil {
					slog.WarnContext(ctx, "rebroadcast failed", "withdrawal", w.ID, "err", err)
				}
			}
		case confs >= e.Cfg.Confirmations[w.Chain]:
			if err := e.St.SetExecution(ctx, w.ID, "", "", confs); err != nil {
				return done, err
			}
			if err := ignoreStale(e.St.Transition(ctx, w.ID, rules.Broadcast, rules.Confirmed, "", "", e.now())); err != nil {
				return done, err
			}
			e.finalize(ctx, w.ID)
			done++
		default:
			if err := e.St.SetExecution(ctx, w.ID, "", "", confs); err != nil {
				return done, err
			}
		}
	}
	return done, nil
}

// finalize applies the ledger effect of a final state once and records it.
func (e *Engine) finalize(ctx context.Context, id int64) {
	w, err := e.St.Get(ctx, id)
	if err != nil {
		slog.WarnContext(ctx, "finalize: load", "withdrawal", id, "err", err)
		return
	}
	var lerr error
	switch w.State {
	case rules.Confirmed:
		lerr = e.Ledger.SettleWithdrawal(ctx, id)
	case rules.Rejected, rules.Failed:
		lerr = e.Ledger.ReleaseWithdrawal(ctx, id)
		if errors.Is(lerr, ErrNoHold) {
			lerr = nil // the hold was never placed (refused for insufficient funds)
		}
	default:
		return
	}
	if lerr != nil {
		slog.WarnContext(ctx, "finalize: ledger", "withdrawal", id, "state", w.State, "err", lerr)
		return
	}
	if err := e.St.MarkFinalized(ctx, id); err != nil {
		slog.WarnContext(ctx, "finalize: mark", "withdrawal", id, "err", err)
	}
}

// ErrNoHold is returned by Ledger.ReleaseWithdrawal when no hold exists.
var ErrNoHold = errors.New("withdrawals: no ledger hold")

// FinalizeOnce repairs final withdrawals whose ledger effect is missing.
func (e *Engine) FinalizeOnce(ctx context.Context) error {
	rows, err := e.St.ListUnfinalized(ctx, 100)
	if err != nil {
		return err
	}
	for _, w := range rows {
		e.finalize(ctx, w.ID)
	}
	return nil
}

// SweepOnce re-queues screening for withdrawals stuck before a decision
// (a lost or failed screening message).
func (e *Engine) SweepOnce(ctx context.Context, olderThan time.Duration) error {
	for _, st := range []rules.State{rules.Requested, rules.Screening} {
		rows, err := e.St.ListByState(ctx, st, e.now().Add(-olderThan), 100)
		if err != nil {
			return err
		}
		for _, w := range rows {
			if err := e.Queue.PublishScreening(ctx, w.ID); err != nil {
				return err
			}
		}
	}
	return nil
}
