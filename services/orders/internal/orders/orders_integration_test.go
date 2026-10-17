//go:build integration

package orders_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/tickless/orders/internal/orders"
	"github.com/tickless/orders/migrations"
	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/pg"
	"github.com/tickless/pkg/testkit"
)

type env struct {
	svc     *orders.Service
	pool    *pgxpool.Pool
	markets *orders.Markets
	ledger  *MockLedger
}

func setup(t *testing.T) (*env, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	dsn := testkit.Postgres(t)
	require.NoError(t, pg.Migrate(ctx, dsn, migrations.FS, "."))
	pool, err := pg.Connect(ctx, dsn, 10)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	ms, err := orders.NewMarkets(ctx, pool)
	require.NoError(t, err)
	l := NewMockLedger(gomock.NewController(t))
	return &env{svc: orders.New(pool, l, ms, orders.Config{MaxOpenPerMarket: 3}), pool: pool, markets: ms, ledger: l}, ctx
}

func (e *env) commands(t *testing.T, ctx context.Context) []*matchingv1.Command {
	t.Helper()
	rows, err := e.pool.Query(ctx, `SELECT payload FROM outbox WHERE topic = 'matching.commands' ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()
	var out []*matchingv1.Command
	for rows.Next() {
		var b []byte
		require.NoError(t, rows.Scan(&b))
		var c matchingv1.Command
		require.NoError(t, proto.Unmarshal(b, &c))
		out = append(out, &c)
	}
	return out
}

func buy(user int64, price, qty, cid string) orders.PlaceRequest {
	return orders.PlaceRequest{UserID: user, Market: "BTC-USDT", Side: commonv1.Side_SIDE_BUY, OrderType: "limit",
		Price: price, Quantity: qty, ClientOrderID: cid}
}

func sell(user int64, price, qty, cid string) orders.PlaceRequest {
	r := buy(user, price, qty, cid)
	r.Side = commonv1.Side_SIDE_SELL
	return r
}

func usdt(s string) money.Int128 { v, _ := money.USDT.Parse(s); return v }

func TestPlaceLimitBuyHoldsQuoteThenWritesCommand(t *testing.T) {
	e, ctx := setup(t)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), int64(1), "USDT", usdt("25000")).Return(nil)

	o, dup, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", "c1"))
	require.NoError(t, err)
	assert.False(t, dup)
	assert.Equal(t, orders.StatusNew, o.Status)

	cmds := e.commands(t, ctx)
	require.Len(t, cmds, 1)
	c := cmds[0]
	assert.Equal(t, "BTC-USDT", c.Market)
	assert.Equal(t, o.ID, c.OrderId)
	assert.Equal(t, uint64(1), c.UserId)
	assert.Equal(t, commonv1.Side_SIDE_BUY, c.Side)
	assert.Equal(t, int64(5_000_000), c.Price, "50000.00 USDT = 5,000,000 ticks of 0.01")
	assert.Equal(t, int64(50_000), c.Qty, "0.5 BTC = 50,000 lots of 0.00001")
	assert.Equal(t, matchingv1.TimeInForce_TIME_IN_FORCE_GTC, c.Tif)
	assert.Equal(t, matchingv1.OrderType_ORDER_TYPE_LIMIT, c.OrderType)
}

func TestPlaceSellHoldsBase(t *testing.T) {
	e, ctx := setup(t)
	btc, _ := money.BTC.Parse("0.25")
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), int64(2), "BTC", btc).Return(nil)
	_, _, err := e.svc.Place(ctx, sell(2, "50000.00", "0.25", ""))
	require.NoError(t, err)
}

func TestMarketBuyHoldsAtProtectiveBoundRoundedUp(t *testing.T) {
	e, ctx := setup(t)
	// 0.00001 BTC (1 lot) at bound 50000.01 => 0.5000001 USDT... min notional is checked on the bound: 5 USDT needs >= 0.0001 BTC.
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), int64(1), "USDT", usdt("5000.001")).Return(nil)
	r := orders.PlaceRequest{UserID: 1, Market: "BTC-USDT", Side: commonv1.Side_SIDE_BUY, OrderType: "market", Price: "50000.01", Quantity: "0.1"}
	o, _, err := e.svc.Place(ctx, r)
	require.NoError(t, err)
	assert.Equal(t, "IOC", o.TIF, "market orders default to IOC")
	cmds := e.commands(t, ctx)
	require.Len(t, cmds, 1)
	assert.Equal(t, matchingv1.OrderType_ORDER_TYPE_MARKET, cmds[0].OrderType)
	assert.Equal(t, matchingv1.TimeInForce_TIME_IN_FORCE_IOC, cmds[0].Tif)
}

func TestClientOrderIDIsIdempotent(t *testing.T) {
	e, ctx := setup(t)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	first, dup1, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", "same"))
	require.NoError(t, err)
	assert.False(t, dup1)
	second, dup2, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", "same"))
	require.NoError(t, err)
	assert.True(t, dup2)
	assert.Equal(t, first.ID, second.ID)
	assert.Len(t, e.commands(t, ctx), 1, "exactly one command despite two submissions")

	// The same client id from another user is a different order.
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), int64(2), gomock.Any(), gomock.Any()).Return(nil)
	other, dup3, err := e.svc.Place(ctx, buy(2, "50000.00", "0.5", "same"))
	require.NoError(t, err)
	assert.False(t, dup3)
	assert.NotEqual(t, first.ID, other.ID)
}

func TestInsufficientFundsRejectsAndSendsNoCommand(t *testing.T) {
	e, ctx := setup(t)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(orders.ErrInsufficientFunds).Times(1)
	_, _, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", "poor"))
	assert.ErrorIs(t, err, orders.ErrInsufficientFunds)
	assert.Empty(t, e.commands(t, ctx))

	// Retrying the same client id returns the rejected order without asking the ledger again.
	o, dup, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", "poor"))
	require.NoError(t, err)
	assert.True(t, dup)
	assert.Equal(t, orders.StatusRejected, o.Status)
	assert.Equal(t, "insufficient funds", o.RejectReason)
}

func TestLedgerOutageLeavesPendingHoldAndSweeperFinishesIt(t *testing.T) {
	e, ctx := setup(t)
	boom := errors.New("ledger down")
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(boom)
	_, _, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", "late"))
	assert.ErrorIs(t, err, orders.ErrLedgerUnavailable)
	assert.Empty(t, e.commands(t, ctx), "no command before the hold exists")

	// Not stale yet: the sweeper leaves it alone.
	n, err := e.svc.Sweep(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)

	_, err = e.pool.Exec(ctx, `UPDATE orders SET created_at = now() - interval '1 minute'`)
	require.NoError(t, err)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	n, err = e.svc.Sweep(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	cmds := e.commands(t, ctx)
	require.Len(t, cmds, 1)

	// A second sweep or a client retry does not produce a second command.
	n, _ = e.svc.Sweep(ctx)
	assert.Zero(t, n)
	o, dup, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", "late"))
	require.NoError(t, err)
	assert.True(t, dup)
	assert.Equal(t, orders.StatusNew, o.Status)
	assert.Len(t, e.commands(t, ctx), 1)
}

func TestDuplicateRetryResumesPendingHold(t *testing.T) {
	e, ctx := setup(t)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("timeout"))
	_, _, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", "retry"))
	require.ErrorIs(t, err, orders.ErrLedgerUnavailable)

	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	o, dup, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", "retry"))
	require.NoError(t, err)
	assert.True(t, dup)
	assert.Equal(t, orders.StatusNew, o.Status)
	assert.Len(t, e.commands(t, ctx), 1)
}

func TestValidation(t *testing.T) {
	e, ctx := setup(t)
	_, err := e.pool.Exec(ctx, `UPDATE markets SET last_price_ticks = 5000000 WHERE symbol = 'BTC-USDT'`)
	require.NoError(t, err)
	require.NoError(t, e.markets.Refresh(ctx))

	mk := func(f func(r *orders.PlaceRequest)) orders.PlaceRequest {
		r := buy(1, "50000.00", "0.5", "")
		f(&r)
		return r
	}
	cases := map[string]struct {
		req  orders.PlaceRequest
		want error
	}{
		"unknown market":      {mk(func(r *orders.PlaceRequest) { r.Market = "DOGE-USDT" }), orders.ErrMarketNotFound},
		"off tick":            {mk(func(r *orders.PlaceRequest) { r.Price = "50000.005" }), orders.ErrInvalid},
		"off lot":             {mk(func(r *orders.PlaceRequest) { r.Quantity = "0.000001" }), orders.ErrInvalid},
		"zero quantity":       {mk(func(r *orders.PlaceRequest) { r.Quantity = "0" }), orders.ErrInvalid},
		"negative price":      {mk(func(r *orders.PlaceRequest) { r.Price = "-1" }), orders.ErrInvalid},
		"exponent":            {mk(func(r *orders.PlaceRequest) { r.Price = "5e4" }), orders.ErrInvalid},
		"limit without price": {mk(func(r *orders.PlaceRequest) { r.Price = "" }), orders.ErrInvalid},
		"below min notional":  {mk(func(r *orders.PlaceRequest) { r.Quantity = "0.00001" }), orders.ErrInvalid},
		"above price band":    {mk(func(r *orders.PlaceRequest) { r.Price = "80000.00" }), orders.ErrInvalid},
		"below price band":    {mk(func(r *orders.PlaceRequest) { r.Price = "20000.00" }), orders.ErrInvalid},
		"bad order type":      {mk(func(r *orders.PlaceRequest) { r.OrderType = "stop" }), orders.ErrInvalid},
		"bad side":            {mk(func(r *orders.PlaceRequest) { r.Side = 0 }), orders.ErrInvalid},
		"bad tif":             {mk(func(r *orders.PlaceRequest) { r.TIF = "GTD" }), orders.ErrInvalid},
		"market GTC":          {mk(func(r *orders.PlaceRequest) { r.OrderType, r.TIF = "market", "GTC" }), orders.ErrInvalid},
		"market buy no bound": {mk(func(r *orders.PlaceRequest) { r.OrderType, r.Price = "market", "" }), orders.ErrInvalid},
		"post-only IOC":       {mk(func(r *orders.PlaceRequest) { r.PostOnly, r.TIF = true, "IOC" }), orders.ErrInvalid},
		"client id too long":  {mk(func(r *orders.PlaceRequest) { r.ClientOrderID = string(make([]byte, 65)) }), orders.ErrInvalid},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := e.svc.Place(ctx, tc.req)
			assert.ErrorIs(t, err, tc.want)
		})
	}
	assert.Empty(t, e.commands(t, ctx), "nothing invalid reaches the engine")
}

func TestMaxOpenOrdersPerMarket(t *testing.T) {
	e, ctx := setup(t)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)
	for i := 0; i < 3; i++ {
		_, _, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", ""))
		require.NoError(t, err)
	}
	_, _, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", ""))
	assert.ErrorIs(t, err, orders.ErrTooManyOpen)
	// Another user is unaffected.
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), int64(2), gomock.Any(), gomock.Any()).Return(nil)
	_, _, err = e.svc.Place(ctx, buy(2, "50000.00", "0.5", ""))
	assert.NoError(t, err)
}

func TestHaltedMarketRejectsNewOrdersButAllowsCancel(t *testing.T) {
	e, ctx := setup(t)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	o, _, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", ""))
	require.NoError(t, err)

	_, err = e.markets.SetHalted(ctx, "BTC-USDT", true)
	require.NoError(t, err)
	_, _, err = e.svc.Place(ctx, buy(1, "50000.00", "0.5", ""))
	assert.ErrorIs(t, err, orders.ErrMarketHalted)
	_, err = e.svc.Cancel(ctx, 1, o.ID)
	assert.NoError(t, err, "users can always cancel")
	_, err = e.markets.SetHalted(ctx, "NOPE", true)
	assert.ErrorIs(t, err, orders.ErrMarketNotFound)

	_, err = e.markets.SetHalted(ctx, "BTC-USDT", false)
	require.NoError(t, err)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	_, _, err = e.svc.Place(ctx, buy(1, "50000.00", "0.5", ""))
	assert.NoError(t, err)
}

func TestCancelFlow(t *testing.T) {
	e, ctx := setup(t)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	o, _, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", ""))
	require.NoError(t, err)

	_, err = e.svc.Cancel(ctx, 2, o.ID)
	assert.ErrorIs(t, err, orders.ErrNotFound, "other users cannot see or cancel it")
	_, err = e.svc.Cancel(ctx, 1, 999999)
	assert.ErrorIs(t, err, orders.ErrNotFound)

	_, err = e.svc.Cancel(ctx, 1, o.ID)
	require.NoError(t, err)
	cmds := e.commands(t, ctx)
	require.Len(t, cmds, 2)
	assert.Equal(t, matchingv1.CommandType_COMMAND_TYPE_CANCEL, cmds[1].Type)

	// The engine confirms; the order becomes CANCELLED and further cancels are no-ops.
	require.NoError(t, e.svc.ApplyEvents(ctx, []orders.Incoming{{Ctx: ctx, Event: &matchingv1.Event{
		Market: "BTC-USDT", Seq: 1, Type: matchingv1.EventType_EVENT_TYPE_CANCELLED, Reason: matchingv1.Reason_REASON_CANCEL_USER, OrderId: o.ID, UserId: 1, Qty: 50_000}}}))
	got, err := e.svc.Get(ctx, 1, o.ID)
	require.NoError(t, err)
	assert.Equal(t, orders.StatusCancelled, got.Status)
	_, err = e.svc.Cancel(ctx, 1, o.ID)
	require.NoError(t, err)
	assert.Len(t, e.commands(t, ctx), 2, "cancelling a finished order sends nothing")
}

func TestCancelAll(t *testing.T) {
	e, ctx := setup(t)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)
	for i := 0; i < 3; i++ {
		_, _, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", ""))
		require.NoError(t, err)
	}
	n, err := e.svc.CancelAll(ctx, 1, "ETH-USDT")
	require.NoError(t, err)
	assert.Zero(t, n, "other market untouched")
	n, err = e.svc.CancelAll(ctx, 1, "BTC-USDT")
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Len(t, e.commands(t, ctx), 6)
}

func TestApplyEventsFillsRejectsAndIsIdempotent(t *testing.T) {
	e, ctx := setup(t)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)
	maker, _, _ := e.svc.Place(ctx, sell(2, "50000.00", "1", "")) // 100,000 lots
	taker, _, _ := e.svc.Place(ctx, buy(1, "50000.00", "0.4", ""))
	bad, _, _ := e.svc.Place(ctx, buy(1, "50000.00", "0.1", ""))

	ev := func(seq uint64, typ matchingv1.EventType) *matchingv1.Event {
		return &matchingv1.Event{Market: "BTC-USDT", Seq: seq, Type: typ}
	}
	trade := ev(1, matchingv1.EventType_EVENT_TYPE_TRADE)
	trade.OrderId, trade.UserId, trade.Side = taker.ID, 1, commonv1.Side_SIDE_BUY
	trade.MakerOrderId, trade.MakerUserId = maker.ID, 2
	trade.Price, trade.Qty, trade.TakerLeaves, trade.MakerLeaves, trade.TradeId = 5_000_000, 40_000, 0, 60_000, 1
	rej := ev(2, matchingv1.EventType_EVENT_TYPE_REJECTED)
	rej.OrderId, rej.UserId, rej.Reason = bad.ID, 1, matchingv1.Reason_REASON_POST_ONLY_WOULD_CROSS

	for i := 0; i < 3; i++ { // redelivery
		require.NoError(t, e.svc.ApplyEvents(ctx, []orders.Incoming{{Ctx: ctx, Event: trade}, {Ctx: ctx, Event: rej}, {Ctx: ctx, Event: trade}}))
	}
	g, _ := e.svc.Get(ctx, 1, taker.ID)
	assert.Equal(t, orders.StatusFilled, g.Status)
	assert.Equal(t, int64(40_000), g.FilledLots)
	g, _ = e.svc.Get(ctx, 2, maker.ID)
	assert.Equal(t, orders.StatusPartial, g.Status)
	assert.Equal(t, int64(40_000), g.FilledLots)
	g, _ = e.svc.Get(ctx, 1, bad.ID)
	assert.Equal(t, orders.StatusRejected, g.Status)
	assert.Equal(t, "post only would cross", g.RejectReason)

	var last int64
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT last_price_ticks FROM markets WHERE symbol='BTC-USDT'`).Scan(&last))
	assert.Equal(t, int64(5_000_000), last)

	// A finished order never changes again, even on a late cancel event.
	late := ev(3, matchingv1.EventType_EVENT_TYPE_CANCELLED)
	late.OrderId = taker.ID
	require.NoError(t, e.svc.ApplyEvents(ctx, []orders.Incoming{{Ctx: ctx, Event: late}}))
	g, _ = e.svc.Get(ctx, 1, taker.ID)
	assert.Equal(t, orders.StatusFilled, g.Status)
}

func TestUnknownOrderRejectionRetriesCancelBoundedTimes(t *testing.T) {
	e, ctx := setup(t)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	o, _, _ := e.svc.Place(ctx, buy(1, "50000.00", "0.5", ""))
	_, err := e.svc.Cancel(ctx, 1, o.ID)
	require.NoError(t, err)
	before := len(e.commands(t, ctx))

	// The cancel overtook the place command: the engine answers "unknown order".
	for seq := uint64(1); seq <= 10; seq++ {
		require.NoError(t, e.svc.ApplyEvents(ctx, []orders.Incoming{{Ctx: ctx, Event: &matchingv1.Event{
			Market: "BTC-USDT", Seq: seq, Type: matchingv1.EventType_EVENT_TYPE_REJECTED, Reason: matchingv1.Reason_REASON_UNKNOWN_ORDER, OrderId: o.ID, UserId: 1}}}))
	}
	retries := len(e.commands(t, ctx)) - before
	assert.Equal(t, 5, retries, "retried up to MaxCancelRetries then gave up")

	// An unknown-order rejection for an order that is already finished sends nothing.
	require.NoError(t, e.svc.ApplyEvents(ctx, []orders.Incoming{{Ctx: ctx, Event: &matchingv1.Event{
		Market: "BTC-USDT", Seq: 11, Type: matchingv1.EventType_EVENT_TYPE_CANCELLED, OrderId: o.ID, UserId: 1}}}))
	n := len(e.commands(t, ctx))
	require.NoError(t, e.svc.ApplyEvents(ctx, []orders.Incoming{{Ctx: ctx, Event: &matchingv1.Event{
		Market: "BTC-USDT", Seq: 12, Type: matchingv1.EventType_EVENT_TYPE_REJECTED, Reason: matchingv1.Reason_REASON_UNKNOWN_ORDER, OrderId: o.ID, UserId: 1}}}))
	assert.Len(t, e.commands(t, ctx), n)
}

func TestListOrders(t *testing.T) {
	e, ctx := setup(t)
	e.ledger.EXPECT().PlaceHold(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(3)
	var ids []uint64
	for i := 0; i < 3; i++ {
		o, _, err := e.svc.Place(ctx, buy(1, "50000.00", "0.5", ""))
		require.NoError(t, err)
		ids = append(ids, o.ID)
	}
	all, err := e.svc.List(ctx, 1, "", false, 0, 10)
	require.NoError(t, err)
	require.Len(t, all, 3)
	assert.Equal(t, ids[2], all[0].ID, "newest first")
	page, _ := e.svc.List(ctx, 1, "", false, ids[2], 10)
	assert.Len(t, page, 2)
	other, _ := e.svc.List(ctx, 2, "", false, 0, 10)
	assert.Empty(t, other)
}
