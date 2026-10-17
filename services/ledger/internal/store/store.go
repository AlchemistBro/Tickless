// Package store implements the double-entry ledger on PostgreSQL.
//
// Every state change is a journal entry (a set of signed postings that sums to
// zero per asset) identified by a business key, so applying the same business
// event twice is a no-op. Balances are denormalised and updated in the same
// transaction; the database rejects negative balances and unbalanced entries
// (see migrations/0001_init.sql).
package store

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	ledgerv1 "github.com/tickless/pkg/gen/tickless/ledger/v1"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/outbox"
	"github.com/tickless/pkg/pg"
)

// Account kinds.
const (
	KindAvailable = "AVAILABLE"
	KindHeld      = "HELD"
	KindFee       = "FEE"
	KindExternal  = "EXTERNAL"
)

// SystemUser owns the FEE and EXTERNAL accounts.
const SystemUser int64 = 0

// TopicEvents is where balance changes are published.
const TopicEvents = "ledger.events"

// Errors.
var (
	ErrInsufficientFunds = errors.New("ledger: insufficient funds")
	ErrInvalidAmount     = errors.New("ledger: amount must be positive")
	ErrUnbalanced        = errors.New("ledger: journal entry does not sum to zero")
)

type acctKey struct {
	User  int64
	Asset string
	Kind  string
}

type posting struct {
	Key    acctKey
	Amount money.Int128
}

// Store is the ledger repository.
type Store struct {
	pool *pgxpool.Pool
	ids  sync.Map // acctKey -> int64 (accounts are immutable once created)
}

// New returns a Store on pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// accountID resolves (and lazily creates) an account outside any transaction,
// so a rolled back business transaction never invalidates the cache.
func (s *Store) accountID(ctx context.Context, k acctKey) (int64, error) {
	if v, ok := s.ids.Load(k); ok {
		return v.(int64), nil
	}
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO accounts (user_id, asset, kind) VALUES ($1, $2, $3)
		ON CONFLICT (user_id, asset, kind) DO UPDATE SET kind = EXCLUDED.kind RETURNING id`,
		k.User, k.Asset, k.Kind).Scan(&id)
	if err != nil {
		return 0, err
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO balances (account_id, kind) VALUES ($1, $2) ON CONFLICT DO NOTHING`, id, k.Kind); err != nil {
		return 0, err
	}
	s.ids.Store(k, id)
	return id, nil
}

type userAsset struct {
	User  int64
	Asset string
}

// session is one database transaction plus the set of balances it touched.
type session struct {
	s       *Store
	tx      pgx.Tx
	touched map[userAsset]struct{}
}

// inSession runs f in a transaction, then publishes BalanceChanged events for
// every touched user balance via the outbox (same transaction) and commits.
func (s *Store) inSession(ctx context.Context, reason string, f func(ss *session) error) error {
	return pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		ss := &session{s: s, tx: tx, touched: map[userAsset]struct{}{}}
		if err := f(ss); err != nil {
			return err
		}
		return ss.flushEvents(ctx, reason)
	})
}

// post applies one balanced journal entry. applied=false means the business
// key was already applied and nothing was changed.
func (ss *session) post(ctx context.Context, businessKey, kind string, ps []posting) (entryID int64, applied bool, err error) {
	type line struct {
		acct  int64
		asset string
		amt   money.Int128
		key   acctKey
	}
	sums := map[string]*big.Int{}
	byAcct := map[int64]*line{}
	for _, p := range ps {
		id, err := ss.s.accountID(ctx, p.Key)
		if err != nil {
			return 0, false, err
		}
		if sums[p.Key.Asset] == nil {
			sums[p.Key.Asset] = new(big.Int)
		}
		sums[p.Key.Asset].Add(sums[p.Key.Asset], p.Amount.Big())
		if l := byAcct[id]; l != nil {
			if l.amt, err = l.amt.Add(p.Amount); err != nil {
				return 0, false, err
			}
		} else {
			byAcct[id] = &line{acct: id, asset: p.Key.Asset, amt: p.Amount, key: p.Key}
		}
	}
	for asset, sum := range sums {
		if sum.Sign() != 0 {
			return 0, false, fmt.Errorf("%w: asset %s sum %s", ErrUnbalanced, asset, sum)
		}
	}
	lines := make([]*line, 0, len(byAcct))
	for _, l := range byAcct {
		if !l.amt.IsZero() {
			lines = append(lines, l)
		}
	}
	slices.SortFunc(lines, func(a, b *line) int { return int(a.acct - b.acct) }) // fixed lock order => no deadlocks
	if len(lines) == 0 {
		return 0, false, nil
	}

	err = ss.tx.QueryRow(ctx,
		`INSERT INTO journal_entries (business_key, kind) VALUES ($1, $2) ON CONFLICT (business_key) DO NOTHING RETURNING id`,
		businessKey, kind).Scan(&entryID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	b := &pgx.Batch{}
	for _, l := range lines {
		b.Queue(`INSERT INTO postings (entry_id, account_id, asset, amount) VALUES ($1, $2, $3, $4::numeric)`,
			entryID, l.acct, l.asset, l.amt.String())
		b.Queue(`UPDATE balances SET amount = amount + $2::numeric, updated_at = now() WHERE account_id = $1`,
			l.acct, l.amt.String())
	}
	br := ss.tx.SendBatch(ctx, b)
	for range lines {
		for range 2 {
			if _, err := br.Exec(); err != nil {
				_ = br.Close()
				return 0, false, mapDBErr(err)
			}
		}
	}
	if err := br.Close(); err != nil {
		return 0, false, mapDBErr(err)
	}
	for _, l := range lines {
		if l.key.User != SystemUser && (l.key.Kind == KindAvailable || l.key.Kind == KindHeld) {
			ss.touched[userAsset{l.key.User, l.key.Asset}] = struct{}{}
		}
	}
	return entryID, true, nil
}

func mapDBErr(err error) error {
	if pg.IsCode(err, pg.CodeCheckViolation) {
		return fmt.Errorf("%w: %w", ErrInsufficientFunds, err)
	}
	return err
}

// flushEvents enqueues one BalanceChanged per touched (user, asset) with the
// final balances of this transaction.
func (ss *session) flushEvents(ctx context.Context, reason string) error {
	for ua := range ss.touched {
		var avail, held string
		rows, err := ss.tx.Query(ctx, `
			SELECT a.kind, b.amount::text FROM accounts a JOIN balances b ON b.account_id = a.id
			WHERE a.user_id = $1 AND a.asset = $2 AND a.kind IN ('AVAILABLE', 'HELD')`, ua.User, ua.Asset)
		if err != nil {
			return err
		}
		for rows.Next() {
			var kind, amt string
			if err := rows.Scan(&kind, &amt); err != nil {
				rows.Close()
				return err
			}
			if kind == KindAvailable {
				avail = amt
			} else {
				held = amt
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if avail == "" {
			avail = "0"
		}
		if held == "" {
			held = "0"
		}
		msg, err := proto.Marshal(&ledgerv1.BalanceChanged{
			Meta:      &commonv1.EventMeta{EventId: uuid.Must(uuid.NewV7()).String()},
			UserId:    ua.User,
			Asset:     ua.Asset,
			Available: &commonv1.Amount{Units: avail},
			Held:      &commonv1.Amount{Units: held},
			Reason:    reason,
		})
		if err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, ss.tx, TopicEvents, fmt.Sprint(ua.User), msg); err != nil {
			return err
		}
	}
	return nil
}

func available(user int64, asset string) acctKey { return acctKey{user, asset, KindAvailable} }
func held(user int64, asset string) acctKey      { return acctKey{user, asset, KindHeld} }

func parseUnits(s string) (money.Int128, error) { return money.ParseInt128(s) }

// PlaceHold reserves amount of asset for an order. Idempotent by orderID.
func (s *Store) PlaceHold(ctx context.Context, orderID uint64, user int64, asset string, amount money.Int128) (already bool, err error) {
	if amount.Sign() <= 0 {
		return false, ErrInvalidAmount
	}
	err = s.inSession(ctx, "hold", func(ss *session) error {
		tag, err := ss.tx.Exec(ctx, `
			INSERT INTO holds (order_id, user_id, asset, amount, remaining, status)
			VALUES ($1, $2, $3, $4::numeric, $4::numeric, 'ACTIVE') ON CONFLICT (order_id) DO NOTHING`,
			int64(orderID), user, asset, amount.String())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			already = true
			return nil
		}
		neg, err := amount.Neg()
		if err != nil {
			return err
		}
		_, _, err = ss.post(ctx, fmt.Sprintf("hold:%d", orderID), "hold", []posting{
			{available(user, asset), neg}, {held(user, asset), amount},
		})
		return err
	})
	return already, err
}

// release returns the remainder of an order's hold to available (idempotent).
func (ss *session) release(ctx context.Context, orderID uint64) (money.Int128, error) {
	var user int64
	var asset, remaining, status string
	err := ss.tx.QueryRow(ctx, `SELECT user_id, asset, remaining::text, status FROM holds WHERE order_id = $1 FOR UPDATE`,
		int64(orderID)).Scan(&user, &asset, &remaining, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return money.Int128{}, nil // order never held anything (e.g. rejected before the hold)
	}
	if err != nil {
		return money.Int128{}, err
	}
	if status == "RELEASED" {
		return money.Int128{}, nil
	}
	rem, err := parseUnits(remaining)
	if err != nil {
		return money.Int128{}, err
	}
	if !rem.IsZero() {
		neg, _ := rem.Neg()
		if _, _, err := ss.post(ctx, fmt.Sprintf("release:%d", orderID), "release", []posting{
			{held(user, asset), neg}, {available(user, asset), rem},
		}); err != nil {
			return money.Int128{}, err
		}
	}
	_, err = ss.tx.Exec(ctx, `UPDATE holds SET remaining = 0, status = 'RELEASED' WHERE order_id = $1`, int64(orderID))
	return rem, err
}

// ReleaseHold returns what remains of an order's hold. Idempotent.
func (s *Store) ReleaseHold(ctx context.Context, orderID uint64) (released money.Int128, err error) {
	err = s.inSession(ctx, "release", func(ss *session) error {
		released, err = ss.release(ctx, orderID)
		return err
	})
	return released, err
}

// Credit moves amount between the EXTERNAL account and a user's available
// balance (negative amount = debit). Idempotent by businessKey.
func (s *Store) Credit(ctx context.Context, businessKey string, user int64, asset string, amount money.Int128) (entryID int64, already bool, err error) {
	if amount.IsZero() {
		return 0, false, ErrInvalidAmount
	}
	err = s.inSession(ctx, "credit", func(ss *session) error {
		neg, err := amount.Neg()
		if err != nil {
			return err
		}
		var applied bool
		entryID, applied, err = ss.post(ctx, "credit:"+businessKey, "credit", []posting{
			{available(user, asset), amount}, {acctKey{SystemUser, asset, KindExternal}, neg},
		})
		already = !applied
		return err
	})
	return entryID, already, err
}

// Balance of one asset.
type Balance struct {
	Asset     string
	Available money.Int128
	Held      money.Int128
}

// Balances returns the user's balances per asset.
func (s *Store) Balances(ctx context.Context, user int64) ([]Balance, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT a.asset, a.kind, b.amount::text FROM accounts a JOIN balances b ON b.account_id = a.id
		WHERE a.user_id = $1 AND a.kind IN ('AVAILABLE', 'HELD') ORDER BY a.asset, a.kind`, user)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	idx := map[string]*Balance{}
	var out []*Balance
	for rows.Next() {
		var asset, kind, amt string
		if err := rows.Scan(&asset, &kind, &amt); err != nil {
			return nil, err
		}
		v, err := parseUnits(amt)
		if err != nil {
			return nil, err
		}
		b := idx[asset]
		if b == nil {
			b = &Balance{Asset: asset}
			idx[asset] = b
			out = append(out, b)
		}
		if kind == KindAvailable {
			b.Available = v
		} else {
			b.Held = v
		}
	}
	res := make([]Balance, len(out))
	for i, b := range out {
		res[i] = *b
	}
	return res, rows.Err()
}

// Entry is one journal entry as seen by a user (only the user's own postings).
type Entry struct {
	ID          int64
	BusinessKey string
	Kind        string
	CreatedAt   int64 // unix nanos
	Postings    []EntryPosting
}

// EntryPosting is a posting on one of the user's accounts.
type EntryPosting struct {
	Asset, Kind string
	Amount      money.Int128
}

// Entries lists the user's ledger history, newest first.
func (s *Store) Entries(ctx context.Context, user int64, asset string, beforeID int64, limit int) ([]Entry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		WITH page AS (
		    SELECT DISTINCT p.entry_id FROM postings p JOIN accounts a ON a.id = p.account_id
		    WHERE a.user_id = $1 AND ($2 = '' OR p.asset = $2) AND ($3::bigint = 0 OR p.entry_id < $3::bigint)
		    ORDER BY p.entry_id DESC LIMIT $4)
		SELECT e.id, e.business_key, e.kind, (EXTRACT(EPOCH FROM e.created_at) * 1e9)::bigint, p.asset, a.kind, p.amount::text
		FROM page JOIN journal_entries e ON e.id = page.entry_id
		JOIN postings p ON p.entry_id = e.id JOIN accounts a ON a.id = p.account_id AND a.user_id = $1
		ORDER BY e.id DESC, p.id`, user, asset, beforeID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var id, ts int64
		var key, kind, passet, pkind, amt string
		if err := rows.Scan(&id, &key, &kind, &ts, &passet, &pkind, &amt); err != nil {
			return nil, err
		}
		v, err := parseUnits(amt)
		if err != nil {
			return nil, err
		}
		if n := len(out); n == 0 || out[n-1].ID != id {
			out = append(out, Entry{ID: id, BusinessKey: key, Kind: kind, CreatedAt: ts})
		}
		e := &out[len(out)-1]
		e.Postings = append(e.Postings, EntryPosting{Asset: passet, Kind: pkind, Amount: v})
	}
	return out, rows.Err()
}

// Verify checks the ledger invariants from the database's point of view:
// balances equal the sum of postings, and per asset the sum over all accounts
// is zero. It returns a descriptive error on the first violation.
func (s *Store) Verify(ctx context.Context) error {
	var bad int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM ledger_verify()`).Scan(&bad); err != nil {
		return err
	}
	if bad > 0 {
		return fmt.Errorf("ledger: %d accounts whose balance differs from the sum of their postings", bad)
	}
	rows, err := s.pool.Query(ctx, `SELECT asset, sum(amount)::text FROM balances b JOIN accounts a ON a.id = b.account_id GROUP BY asset HAVING sum(amount) <> 0`)
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		var asset, sum string
		_ = rows.Scan(&asset, &sum)
		return fmt.Errorf("ledger: asset %s does not sum to zero (%s)", asset, sum)
	}
	return rows.Err()
}
