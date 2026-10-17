//go:build integration

package store_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tickless/ledger/internal/store"
	"github.com/tickless/ledger/migrations"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/market"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/testkit"
)

var btcUsdt = market.Spec{
	Symbol: "BTC-USDT", Base: "BTC", Quote: "USDT", BaseDecimals: 8, QuoteDecimals: 6,
	TickSize: 10_000, LotSize: 1_000, MakerFeeBps: 10, TakerFeeBps: 20,
}

type markets struct{}

func (markets) Spec(s string) (market.Spec, bool) { return btcUsdt, s == "BTC-USDT" }

func newStore(t *testing.T) (*store.Store, *pgxpool.Pool, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	dsn := testkit.Postgres(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "."))
	pool, err := pg.Connect(ctx, dsn, 10)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return store.New(pool), pool, ctx
}

func usdt(s string) money.Int128 { v, _ := money.USDT.Parse(s); return v }
func btc(s string) money.Int128  { v, _ := money.BTC.Parse(s); return v }

func balance(t *testing.T, s *store.Store, ctx context.Context, user int64, asset string) (avail, held string) {
	t.Helper()
	bs, err := s.Balances(ctx, user)
	require.NoError(t, err)
	for _, b := range bs {
		if b.Asset == asset {
			a := money.Asset{Decimals: 8}
			if asset == "USDT" {
				a = money.USDT
			}
			return a.Format(b.Available), a.Format(b.Held)
		}
	}
	return "0", "0"
}

func tradeEv(seq, tradeID uint64, takerSide commonv1.Side, takerOrder, takerUser, makerOrder, makerUser uint64, price, qty, takerLeft, makerLeft int64) *matchingv1.Event {
	return &matchingv1.Event{
		Market: "BTC-USDT", Seq: seq, Type: matchingv1.EventType_EVENT_TYPE_TRADE, Side: takerSide,
		OrderId: takerOrder, UserId: takerUser, MakerOrderId: makerOrder, MakerUserId: makerUser,
		Price: price, Qty: qty, TakerLeaves: takerLeft, MakerLeaves: makerLeft, TradeId: tradeID,
	}
}

func apply(t *testing.T, s *store.Store, ctx context.Context, evs ...*matchingv1.Event) error {
	var in []store.Incoming
	for _, e := range evs {
		in = append(in, store.Incoming{Ctx: ctx, Event: e})
	}
	return s.ApplyEvents(ctx, markets{}, in)
}

func TestHoldReleaseIdempotentAndInsufficientFunds(t *testing.T) {
	s, _, ctx := newStore(t)
	_, _, err := s.Credit(ctx, "seed-1", 1, "USDT", usdt("100"))
	require.NoError(t, err)

	already, err := s.PlaceHold(ctx, 1, 1, "USDT", usdt("60"))
	require.NoError(t, err)
	assert.False(t, already)
	a, h := balance(t, s, ctx, 1, "USDT")
	assert.Equal(t, []string{"40", "60"}, []string{a, h})

	already, err = s.PlaceHold(ctx, 1, 1, "USDT", usdt("60")) // same order id
	require.NoError(t, err)
	assert.True(t, already)
	a, h = balance(t, s, ctx, 1, "USDT")
	assert.Equal(t, []string{"40", "60"}, []string{a, h}, "hold is idempotent by order id")

	_, err = s.PlaceHold(ctx, 2, 1, "USDT", usdt("50")) // only 40 available
	assert.ErrorIs(t, err, store.ErrInsufficientFunds)
	a, h = balance(t, s, ctx, 1, "USDT")
	assert.Equal(t, []string{"40", "60"}, []string{a, h}, "failed hold leaves no trace")
	_, err = s.PlaceHold(ctx, 2, 1, "USDT", usdt("40")) // order 2 can still be placed: no hold row leaked
	require.NoError(t, err)

	rel, err := s.ReleaseHold(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, usdt("60"), rel)
	rel, err = s.ReleaseHold(ctx, 1)
	require.NoError(t, err)
	assert.True(t, rel.IsZero(), "second release is a no-op")
	rel, err = s.ReleaseHold(ctx, 999)
	require.NoError(t, err)
	assert.True(t, rel.IsZero(), "unknown order is a no-op")
	require.NoError(t, s.Verify(ctx))

	_, err = s.PlaceHold(ctx, 3, 1, "USDT", usdt("0"))
	assert.ErrorIs(t, err, store.ErrInvalidAmount)
}

func TestCreditIdempotentByBusinessKey(t *testing.T) {
	s, _, ctx := newStore(t)
	id1, already, err := s.Credit(ctx, "dep-1", 7, "BTC", btc("1"))
	require.NoError(t, err)
	assert.False(t, already)
	id2, already, err := s.Credit(ctx, "dep-1", 7, "BTC", btc("1"))
	require.NoError(t, err)
	assert.True(t, already)
	_ = id1
	_ = id2
	a, _ := balance(t, s, ctx, 7, "BTC")
	assert.Equal(t, "1", a, "credited exactly once")
	// A debit that would overdraw is rejected by the database.
	_, _, err = s.Credit(ctx, "wd-1", 7, "BTC", btc("-2"))
	assert.ErrorIs(t, err, store.ErrInsufficientFunds)
	require.NoError(t, s.Verify(ctx))
}

func TestTradeSettlementFeesAndHoldRelease(t *testing.T) {
	s, pool, ctx := newStore(t)
	// user 1 (buyer, taker) has 30000 USDT; user 2 (seller, maker) has 1 BTC.
	_, _, _ = s.Credit(ctx, "s1", 1, "USDT", usdt("30000"))
	_, _, _ = s.Credit(ctx, "s2", 2, "BTC", btc("1"))
	// Taker buy 0.5 BTC limit 50000 holds 25000 USDT; maker sell 0.5 BTC @49000 holds 0.5 BTC.
	_, err := s.PlaceHold(ctx, 10, 1, "USDT", usdt("25000"))
	require.NoError(t, err)
	_, err = s.PlaceHold(ctx, 11, 2, "BTC", btc("0.5"))
	require.NoError(t, err)

	// Executes at the maker price 49000.00 => 4,900,000 ticks; 0.5 BTC => 50,000 lots; both fully filled.
	require.NoError(t, apply(t, s, ctx, tradeEv(1, 1, commonv1.Side_SIDE_BUY, 10, 1, 11, 2, 4_900_000, 50_000, 0, 0)))

	// Buyer: pays 24500 USDT, receives 0.5 BTC minus 20 bps taker fee (0.001 BTC), excess hold 500 released.
	a, h := balance(t, s, ctx, 1, "USDT")
	assert.Equal(t, []string{"5500", "0"}, []string{a, h})
	a, h = balance(t, s, ctx, 1, "BTC")
	assert.Equal(t, []string{"0.499", "0"}, []string{a, h})
	// Seller: BTC 0.5 left, receives 24500 USDT minus 10 bps maker fee (24.5 USDT).
	a, h = balance(t, s, ctx, 2, "BTC")
	assert.Equal(t, []string{"0.5", "0"}, []string{a, h})
	a, h = balance(t, s, ctx, 2, "USDT")
	assert.Equal(t, []string{"24475.5", "0"}, []string{a, h})
	// Exchange fee income, rounded toward the exchange.
	var feeBTC, feeUSDT string
	require.NoError(t, pool.QueryRow(ctx, `SELECT amount::text FROM balances b JOIN accounts a ON a.id=b.account_id WHERE a.kind='FEE' AND a.asset='BTC'`).Scan(&feeBTC))
	require.NoError(t, pool.QueryRow(ctx, `SELECT amount::text FROM balances b JOIN accounts a ON a.id=b.account_id WHERE a.kind='FEE' AND a.asset='USDT'`).Scan(&feeUSDT))
	assert.Equal(t, "100000", feeBTC, "0.001 BTC in satoshi")
	assert.Equal(t, "24500000", feeUSDT, "24.5 USDT in micro-USDT")
	require.NoError(t, s.Verify(ctx))
}

func TestApplyEventsIsIdempotentUnderDuplicateDelivery(t *testing.T) {
	s, _, ctx := newStore(t)
	_, _, _ = s.Credit(ctx, "s1", 1, "USDT", usdt("30000"))
	_, _, _ = s.Credit(ctx, "s2", 2, "BTC", btc("1"))
	_, _ = s.PlaceHold(ctx, 10, 1, "USDT", usdt("25000"))
	_, _ = s.PlaceHold(ctx, 11, 2, "BTC", btc("1"))

	tr := tradeEv(1, 1, commonv1.Side_SIDE_BUY, 10, 1, 11, 2, 4_900_000, 20_000, 0, 80_000) // 0.2 BTC, maker has 0.8 left
	cancel := &matchingv1.Event{Market: "BTC-USDT", Seq: 2, Type: matchingv1.EventType_EVENT_TYPE_CANCELLED, OrderId: 11, UserId: 2, Qty: 80_000}

	// Deliver the same events many times, singly and in duplicated batches.
	for i := 0; i < 3; i++ {
		require.NoError(t, apply(t, s, ctx, tr, tr, cancel))
		require.NoError(t, apply(t, s, ctx, cancel))
		require.NoError(t, apply(t, s, ctx, tr))
	}
	a, h := balance(t, s, ctx, 2, "BTC")
	assert.Equal(t, []string{"0.8", "0"}, []string{a, h}, "traded 0.2 once, remaining hold released once")
	a, h = balance(t, s, ctx, 1, "USDT")
	// 30000 - 25000 hold = 5000; trade costs 0.2*49000 = 9800; buyer is fully filled
	// (TakerLeaves=0) so the unused 15200 of the hold returns: 5000 + 15200.
	assert.Equal(t, []string{"20200", "0"}, []string{a, h})
	require.NoError(t, s.Verify(ctx))
}

func TestRejectedAndCancelledReleaseButDuplicateAndUnknownDoNot(t *testing.T) {
	s, _, ctx := newStore(t)
	_, _, _ = s.Credit(ctx, "s1", 1, "USDT", usdt("1000"))
	_, _ = s.PlaceHold(ctx, 1, 1, "USDT", usdt("300"))
	_, _ = s.PlaceHold(ctx, 2, 1, "USDT", usdt("200"))

	dup := &matchingv1.Event{Market: "BTC-USDT", Seq: 1, Type: matchingv1.EventType_EVENT_TYPE_REJECTED, Reason: matchingv1.Reason_REASON_DUPLICATE_ORDER, OrderId: 1, UserId: 1}
	unk := &matchingv1.Event{Market: "BTC-USDT", Seq: 2, Type: matchingv1.EventType_EVENT_TYPE_REJECTED, Reason: matchingv1.Reason_REASON_UNKNOWN_ORDER, OrderId: 1, UserId: 1}
	require.NoError(t, apply(t, s, ctx, dup, unk))
	a, h := balance(t, s, ctx, 1, "USDT")
	assert.Equal(t, []string{"500", "500"}, []string{a, h}, "duplicate/unknown rejects must not release the live order hold")

	rej := &matchingv1.Event{Market: "BTC-USDT", Seq: 3, Type: matchingv1.EventType_EVENT_TYPE_REJECTED, Reason: matchingv1.Reason_REASON_INVALID_QTY, OrderId: 1, UserId: 1}
	can := &matchingv1.Event{Market: "BTC-USDT", Seq: 4, Type: matchingv1.EventType_EVENT_TYPE_CANCELLED, OrderId: 2, UserId: 1, Qty: 1}
	require.NoError(t, apply(t, s, ctx, rej, can))
	a, h = balance(t, s, ctx, 1, "USDT")
	assert.Equal(t, []string{"1000", "0"}, []string{a, h})
	require.NoError(t, s.Verify(ctx))
}

func TestUnknownMarketIsRetryableError(t *testing.T) {
	s, _, ctx := newStore(t)
	ev := tradeEv(1, 1, commonv1.Side_SIDE_BUY, 1, 1, 2, 2, 100, 1, 0, 0)
	ev.Market = "NOPE-USDT"
	assert.ErrorIs(t, apply(t, s, ctx, ev), store.ErrUnknownMarket)
}

func TestEntriesHistory(t *testing.T) {
	s, _, ctx := newStore(t)
	_, _, _ = s.Credit(ctx, "a", 5, "USDT", usdt("10"))
	_, _ = s.PlaceHold(ctx, 1, 5, "USDT", usdt("4"))
	es, err := s.Entries(ctx, 5, "", 0, 10)
	require.NoError(t, err)
	require.Len(t, es, 2)
	assert.Equal(t, "hold:1", es[0].BusinessKey, "newest first")
	assert.Len(t, es[0].Postings, 2)
	assert.Len(t, es[1].Postings, 1, "only the user's own side of a credit is visible")
	older, err := s.Entries(ctx, 5, "", es[0].ID, 10)
	require.NoError(t, err)
	assert.Len(t, older, 1)
}

// TestRandomOperationsPreserveInvariants drives random holds, partial fills,
// cancels and duplicate deliveries and checks after every step that the
// database invariants hold: balanced entries, balances == sum of postings,
// ledger sums to zero per asset, and (by constraint) no negative balance.
func TestRandomOperationsPreserveInvariants(t *testing.T) {
	s, _, ctx := newStore(t)
	rng := rand.New(rand.NewSource(7))
	const users = 6
	for u := int64(1); u <= users; u++ {
		_, _, _ = s.Credit(ctx, "seedU", u, "USDT", usdt("100000"))
		_, _, _ = s.Credit(ctx, "seedB", u, "BTC", btc("10"))
	}
	type ord struct {
		id       uint64
		user     int64
		buy      bool
		limit    int64 // ticks
		leaves   int64 // lots
		finished bool
	}
	var orders []*ord
	var nextID uint64 = 1
	var seq, tradeID uint64
	var applied [][]*matchingv1.Event

	for step := 0; step < 150; step++ {
		switch r := rng.Intn(10); {
		case r < 4: // place a new order with a hold
			o := &ord{id: nextID, user: int64(1 + rng.Intn(users)), buy: rng.Intn(2) == 0,
				limit: int64(4_000_000 + rng.Intn(2_000_000)), leaves: int64(1 + rng.Intn(200_000))}
			nextID++
			var err error
			if o.buy {
				hold, e := btcUsdt.Notional(o.limit, o.leaves, money.RoundUp)
				require.NoError(t, e)
				_, err = s.PlaceHold(ctx, o.id, o.user, "USDT", hold)
			} else {
				base, e := btcUsdt.Money().BaseAmount(o.leaves)
				require.NoError(t, e)
				_, err = s.PlaceHold(ctx, o.id, o.user, "BTC", base)
			}
			if err != nil {
				require.ErrorIs(t, err, store.ErrInsufficientFunds)
				o.finished = true
			}
			orders = append(orders, o)
		case r < 8: // trade between a buy and a sell of different users
			var b, sl *ord
			for _, o := range orders {
				if o.finished {
					continue
				}
				if o.buy && b == nil || !o.buy && sl == nil {
					if o.buy {
						b = o
					} else {
						sl = o
					}
				}
			}
			if b == nil || sl == nil || b.user == sl.user || b.limit < sl.limit {
				continue
			}
			qty := min(b.leaves, sl.leaves, int64(1+rng.Intn(100_000)))
			price := sl.limit // maker = seller at its price (<= buyer limit)
			b.leaves -= qty
			sl.leaves -= qty
			seq++
			tradeID++
			ev := tradeEv(seq, tradeID, commonv1.Side_SIDE_BUY, b.id, uint64(b.user), sl.id, uint64(sl.user), price, qty, b.leaves, sl.leaves)
			require.NoError(t, apply(t, s, ctx, ev))
			applied = append(applied, []*matchingv1.Event{ev})
			b.finished = b.leaves == 0
			sl.finished = sl.leaves == 0
		case r < 9: // cancel an open order
			for _, o := range orders {
				if !o.finished {
					seq++
					ev := &matchingv1.Event{Market: "BTC-USDT", Seq: seq, Type: matchingv1.EventType_EVENT_TYPE_CANCELLED, OrderId: o.id, UserId: uint64(o.user), Qty: o.leaves}
					require.NoError(t, apply(t, s, ctx, ev))
					applied = append(applied, []*matchingv1.Event{ev})
					o.finished = true
					break
				}
			}
		default: // duplicate delivery of everything seen so far
			for _, evs := range applied {
				require.NoError(t, apply(t, s, ctx, evs...))
			}
		}
		require.NoError(t, s.Verify(ctx), "step %d", step)
	}
	assert.NotEmpty(t, applied, "scenario must have produced trades")
}

// SPEC §6.1 and §6.2 are enforced by the database itself, so even a bug or a
// manual SQL session cannot create an unbalanced entry or a negative balance.
func TestDatabaseRejectsUnbalancedEntriesAndNegativeBalances(t *testing.T) {
	s, pool, ctx := newStore(t)
	_, _, err := s.Credit(ctx, "seed", 1, "USDT", usdt("10"))
	require.NoError(t, err)

	var availID int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT id FROM accounts WHERE user_id = 1 AND kind = 'AVAILABLE'`).Scan(&availID))

	// An entry whose postings do not sum to zero cannot be committed.
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	var entry int64
	require.NoError(t, tx.QueryRow(ctx, `INSERT INTO journal_entries (business_key, kind) VALUES ('rogue', 'rogue') RETURNING id`).Scan(&entry))
	_, err = tx.Exec(ctx, `INSERT INTO postings (entry_id, account_id, asset, amount) VALUES ($1, $2, 'USDT', 5)`, entry, availID)
	require.NoError(t, err)
	err = tx.Commit(ctx)
	require.Error(t, err, "unbalanced journal entry must be rejected at commit")
	assert.True(t, pg.IsCode(err, pg.CodeCheckViolation), "got: %v", err)

	// A balance can never go negative (except EXTERNAL).
	_, err = pool.Exec(ctx, `UPDATE balances SET amount = -1 WHERE account_id = $1`, availID)
	require.Error(t, err)
	assert.True(t, pg.IsCode(err, pg.CodeCheckViolation))

	require.NoError(t, s.Verify(ctx))
}
