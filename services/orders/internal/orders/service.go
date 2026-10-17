// Package orders implements order intake: validation against market rules,
// the hold-then-command saga, cancellation and the order read model fed by
// matching events.
//
// Saga (see docs/adr/0006-order-placement-saga.md):
//
//  1. insert the order as PENDING_HOLD (idempotent by client_order_id)
//  2. ask the ledger for a hold (idempotent by order id)
//  3. in one transaction mark the order NEW and write the PlaceOrder command
//     to the outbox, so the command reaches the engine only after the hold
//
// A crash between steps is repaired by the sweeper, which re-drives old
// PENDING_HOLD orders; every step is idempotent.
package orders

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/market"
	"github.com/tickless/pkg/money"
	"github.com/tickless/pkg/outbox"
	"github.com/tickless/pkg/pg"
)

// TopicCommands is the matching command log.
const TopicCommands = "matching.commands"

// Statuses.
const (
	StatusPendingHold = "PENDING_HOLD"
	StatusNew         = "NEW"
	StatusPartial     = "PARTIALLY_FILLED"
	StatusFilled      = "FILLED"
	StatusCancelled   = "CANCELLED"
	StatusRejected    = "REJECTED"
)

// Errors mapped to gRPC codes by the transport layer.
var (
	ErrMarketNotFound    = errors.New("orders: unknown market")
	ErrMarketHalted      = errors.New("orders: market is halted")
	ErrInvalid           = errors.New("orders: invalid order")
	ErrInsufficientFunds = errors.New("orders: insufficient funds")
	ErrTooManyOpen       = errors.New("orders: too many open orders")
	ErrNotFound          = errors.New("orders: order not found")
	ErrNotCancellable    = errors.New("orders: order is not cancellable yet")
	ErrLedgerUnavailable = errors.New("orders: ledger unavailable")
)

// invalid wraps ErrInvalid with a user-facing explanation.
func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, a...))
}

// Ledger is the subset of the ledger API the saga needs.
//
//go:generate go run go.uber.org/mock/mockgen -source=service.go -destination=mock_ledger_test.go -package=orders_test
type Ledger interface {
	// PlaceHold must be idempotent by orderID and return ErrInsufficientFunds
	// when the user cannot cover the hold.
	PlaceHold(ctx context.Context, orderID uint64, userID int64, asset string, amount money.Int128) error
}

// Config tunes the service.
type Config struct {
	MaxOpenPerMarket int           // per user, default 200
	SweepAfter       time.Duration // PENDING_HOLD age before the sweeper retries, default 5s
	MaxCancelRetries int
}

// Service is the orders use-case layer.
type Service struct {
	pool    *pgxpool.Pool
	ledger  Ledger
	markets *Markets
	cfg     Config
	now     func() time.Time
}

// New creates the service.
func New(pool *pgxpool.Pool, ledger Ledger, markets *Markets, cfg Config) *Service {
	if cfg.MaxOpenPerMarket == 0 {
		cfg.MaxOpenPerMarket = 200
	}
	if cfg.SweepAfter == 0 {
		cfg.SweepAfter = 5 * time.Second
	}
	if cfg.MaxCancelRetries == 0 {
		cfg.MaxCancelRetries = 5
	}
	return &Service{pool: pool, ledger: ledger, markets: markets, cfg: cfg, now: time.Now}
}

var tracer = otel.Tracer("tickless/orders")

// Order is the read model of an order.
type Order struct {
	ID           uint64
	UserID       int64
	Market       string
	Side         int16
	OrderType    string
	TIF          string
	PostOnly     bool
	PriceTicks   int64
	QtyLots      int64
	FilledLots   int64
	Status       string
	ClientID     string
	RejectReason string
	HoldAsset    string
	HoldAmount   money.Int128
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Open reports whether the order can still trade.
func (o Order) Open() bool { return o.Status == StatusNew || o.Status == StatusPartial }

const orderCols = `id, user_id, market, side, order_type, tif, post_only, price_ticks, qty_lots, filled_lots, status,
	COALESCE(client_order_id, ''), reject_reason, hold_asset, hold_amount::text, created_at, updated_at`

func scanOrder(row pgx.Row) (Order, error) {
	var o Order
	var id int64
	var hold string
	err := row.Scan(&id, &o.UserID, &o.Market, &o.Side, &o.OrderType, &o.TIF, &o.PostOnly, &o.PriceTicks, &o.QtyLots,
		&o.FilledLots, &o.Status, &o.ClientID, &o.RejectReason, &o.HoldAsset, &hold, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		return o, err
	}
	o.ID = uint64(id)
	o.HoldAmount, err = money.ParseInt128(hold)
	return o, err
}

// PlaceRequest is a user's order submission with decimal-string amounts.
type PlaceRequest struct {
	UserID        int64
	Market        string
	Side          commonv1.Side
	OrderType     string // "limit" | "market"
	TIF           string // "" | GTC | IOC | FOK
	PostOnly      bool
	Price         string
	Quantity      string
	ClientOrderID string
}

type validated struct {
	spec       marketInfo
	tif        string
	priceTicks int64
	qtyLots    int64
	holdAsset  string
	hold       money.Int128
}

func (s *Service) validate(r PlaceRequest) (validated, error) {
	mi, ok := s.markets.get(r.Market)
	if !ok {
		return validated{}, ErrMarketNotFound
	}
	if mi.Halted {
		return validated{}, ErrMarketHalted
	}
	v := validated{spec: mi}
	if r.Side != commonv1.Side_SIDE_BUY && r.Side != commonv1.Side_SIDE_SELL {
		return v, invalid("side must be buy or sell")
	}
	if len(r.ClientOrderID) > 64 {
		return v, invalid("client_order_id longer than 64 characters")
	}
	var err error
	if v.qtyLots, err = mi.ParseQty(r.Quantity); err != nil {
		return v, invalid("quantity: %v", err)
	}
	v.tif = strings.ToUpper(r.TIF)
	switch r.OrderType {
	case "limit":
		if v.tif == "" {
			v.tif = "GTC"
		}
		if r.Price == "" {
			return v, invalid("price is required for limit orders")
		}
	case "market":
		if v.tif == "" {
			v.tif = "IOC"
		}
		if v.tif == "GTC" || r.PostOnly {
			return v, invalid("market orders must be IOC or FOK and cannot be post-only")
		}
		if r.Price == "" && r.Side == commonv1.Side_SIDE_BUY {
			// A market buy reserves quote funds up front, so it needs a price bound (ADR 0006).
			return v, invalid("price (protective bound) is required for market buy orders")
		}
	default:
		return v, invalid("order_type must be limit or market")
	}
	if v.tif != "GTC" && v.tif != "IOC" && v.tif != "FOK" {
		return v, invalid("time_in_force must be GTC, IOC or FOK")
	}
	if r.PostOnly && v.tif != "GTC" {
		return v, invalid("post-only orders must be GTC")
	}
	if r.Price != "" {
		if v.priceTicks, err = mi.ParsePrice(r.Price); err != nil {
			return v, invalid("price: %v", err)
		}
		if v.priceTicks < mi.MinPriceTicks || v.priceTicks > mi.MaxPriceTicks {
			return v, invalid("price outside the allowed range")
		}
		if r.OrderType == "limit" && mi.LastPrice > 0 {
			lo := mi.LastPrice * (10_000 - mi.BandBps) / 10_000
			hi := mi.LastPrice * (10_000 + mi.BandBps) / 10_000
			if v.priceTicks < lo || v.priceTicks > hi {
				return v, invalid("price outside the +-%d%% band around the last trade price", mi.BandBps/100)
			}
		}
	}
	// Minimum notional on the value we know: limit price, or the protective bound.
	if v.priceTicks > 0 {
		n, err := mi.Notional(v.priceTicks, v.qtyLots, money.RoundDown)
		if err != nil {
			return v, invalid("order value: %v", err)
		}
		if n.Cmp(mi.MinNotional) < 0 {
			return v, invalid("order value below the minimum notional")
		}
	}
	// Hold: buyers reserve the quote amount at their limit/bound (rounded UP so
	// it always covers the trades, which round down); sellers reserve base.
	if r.Side == commonv1.Side_SIDE_BUY {
		v.holdAsset = mi.Quote
		if v.hold, err = mi.Notional(v.priceTicks, v.qtyLots, money.RoundUp); err != nil {
			return v, invalid("order value: %v", err)
		}
	} else {
		v.holdAsset = mi.Base
		if v.hold, err = mi.Money().BaseAmount(v.qtyLots); err != nil {
			return v, invalid("quantity: %v", err)
		}
	}
	if v.hold.Sign() <= 0 {
		return v, invalid("order value rounds to zero")
	}
	return v, nil
}

// Place validates and places an order. duplicate=true means client_order_id
// matched an existing order, which is returned unchanged.
func (s *Service) Place(ctx context.Context, r PlaceRequest) (o Order, duplicate bool, err error) {
	ctx, span := tracer.Start(ctx, "orders.place")
	defer span.End()

	v, err := s.validate(r)
	if err != nil {
		return Order{}, false, err
	}
	var open int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM orders WHERE user_id = $1 AND market = $2 AND status IN ('NEW', 'PARTIALLY_FILLED', 'PENDING_HOLD')`,
		r.UserID, r.Market).Scan(&open); err != nil {
		return Order{}, false, err
	}
	if open >= s.cfg.MaxOpenPerMarket {
		return Order{}, false, ErrTooManyOpen
	}

	// Step 1: persist as PENDING_HOLD, deduplicating on client_order_id.
	err = pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		var id int64
		if err := tx.QueryRow(ctx, `SELECT nextval('order_id_seq')`).Scan(&id); err != nil {
			return err
		}
		if r.ClientOrderID != "" {
			tag, err := tx.Exec(ctx, `INSERT INTO order_keys (user_id, client_order_id, order_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
				r.UserID, r.ClientOrderID, id)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				duplicate = true
				o, err = scanOrder(tx.QueryRow(ctx, `SELECT `+orderCols+` FROM orders WHERE id = (SELECT order_id FROM order_keys WHERE user_id = $1 AND client_order_id = $2)`,
					r.UserID, r.ClientOrderID))
				return err
			}
		}
		var cid *string
		if r.ClientOrderID != "" {
			cid = &r.ClientOrderID
		}
		o, err = scanOrder(tx.QueryRow(ctx, `
			INSERT INTO orders (id, user_id, market, side, order_type, tif, post_only, price_ticks, qty_lots, status,
			                    client_order_id, hold_asset, hold_amount)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'PENDING_HOLD', $10, $11, $12::numeric)
			RETURNING `+orderCols,
			id, r.UserID, r.Market, int16(r.Side), r.OrderType, v.tif, r.PostOnly, v.priceTicks, v.qtyLots, cid, v.holdAsset, v.hold.String()))
		return err
	})
	if err != nil {
		return Order{}, false, err
	}
	if duplicate && o.Status != StatusPendingHold {
		return o, true, nil
	}
	// Steps 2-3 (also resumes a duplicate that is still PENDING_HOLD).
	o, err = s.completePlacement(ctx, o)
	return o, duplicate, err
}

// completePlacement runs saga steps 2 and 3 for a PENDING_HOLD order.
func (s *Service) completePlacement(ctx context.Context, o Order) (Order, error) {
	if err := s.ledger.PlaceHold(ctx, o.ID, o.UserID, o.HoldAsset, o.HoldAmount); err != nil {
		if errors.Is(err, ErrInsufficientFunds) {
			if _, uerr := s.pool.Exec(ctx, `UPDATE orders SET status = 'REJECTED', reject_reason = 'insufficient funds', updated_at = now() WHERE id = $1 AND status = 'PENDING_HOLD'`, int64(o.ID)); uerr != nil {
				return o, uerr
			}
			return o, ErrInsufficientFunds
		}
		// Ledger unreachable: leave PENDING_HOLD; the client may retry with the
		// same client_order_id and the sweeper will finish the order otherwise.
		slog.WarnContext(ctx, "hold failed, order stays PENDING_HOLD", "order", o.ID, "err", err)
		return o, fmt.Errorf("%w: %w", ErrLedgerUnavailable, err)
	}
	cmd, err := proto.Marshal(s.placeCommand(o))
	if err != nil {
		return o, err
	}
	err = pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE orders SET status = 'NEW', updated_at = now() WHERE id = $1 AND status = 'PENDING_HOLD'`, int64(o.ID))
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil // another instance (sweeper) already finished it
		}
		return outbox.Enqueue(ctx, tx, TopicCommands, o.Market, cmd)
	})
	if err != nil {
		return o, err
	}
	return s.Get(ctx, o.UserID, o.ID)
}

func (s *Service) placeCommand(o Order) *matchingv1.Command {
	c := &matchingv1.Command{
		Market: o.Market, Type: matchingv1.CommandType_COMMAND_TYPE_PLACE, OrderId: o.ID, UserId: uint64(o.UserID),
		Side: commonv1.Side(o.Side), PostOnly: o.PostOnly, Price: o.PriceTicks, Qty: o.QtyLots,
		TimeUnixNano: s.now().UnixNano(),
	}
	if o.OrderType == "limit" {
		c.OrderType = matchingv1.OrderType_ORDER_TYPE_LIMIT
	} else {
		c.OrderType = matchingv1.OrderType_ORDER_TYPE_MARKET
	}
	switch o.TIF {
	case "GTC":
		c.Tif = matchingv1.TimeInForce_TIME_IN_FORCE_GTC
	case "IOC":
		c.Tif = matchingv1.TimeInForce_TIME_IN_FORCE_IOC
	case "FOK":
		c.Tif = matchingv1.TimeInForce_TIME_IN_FORCE_FOK
	}
	return c
}

func (s *Service) cancelCommand(o Order) *matchingv1.Command {
	return &matchingv1.Command{Market: o.Market, Type: matchingv1.CommandType_COMMAND_TYPE_CANCEL,
		OrderId: o.ID, UserId: uint64(o.UserID), TimeUnixNano: s.now().UnixNano()}
}

// Get returns one of the user's orders.
func (s *Service) Get(ctx context.Context, userID int64, id uint64) (Order, error) {
	o, err := scanOrder(s.pool.QueryRow(ctx, `SELECT `+orderCols+` FROM orders WHERE id = $1 AND user_id = $2`, int64(id), userID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	return o, err
}

// Cancel requests cancellation. The final CANCELLED status arrives with the
// engine's event; the call is idempotent and returns the current state.
func (s *Service) Cancel(ctx context.Context, userID int64, id uint64) (Order, error) {
	var o Order
	err := pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		o, err = scanOrder(tx.QueryRow(ctx, `SELECT `+orderCols+` FROM orders WHERE id = $1 AND user_id = $2 FOR UPDATE`, int64(id), userID))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		switch {
		case o.Status == StatusPendingHold:
			return ErrNotCancellable
		case !o.Open():
			return nil // already terminal: nothing to do
		}
		return s.enqueueCancel(ctx, tx, o)
	})
	return o, err
}

func (s *Service) enqueueCancel(ctx context.Context, tx pgx.Tx, o Order) error {
	msg, err := proto.Marshal(s.cancelCommand(o))
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, tx, TopicCommands, o.Market, msg)
}

// CancelAll requests cancellation of all open orders (optionally one market).
func (s *Service) CancelAll(ctx context.Context, userID int64, mkt string) (int, error) {
	n := 0
	err := pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+orderCols+` FROM orders
			WHERE user_id = $1 AND ($2 = '' OR market = $2) AND status IN ('NEW', 'PARTIALLY_FILLED') ORDER BY id`, userID, mkt)
		if err != nil {
			return err
		}
		var open []Order
		for rows.Next() {
			o, err := scanOrder(rows)
			if err != nil {
				rows.Close()
				return err
			}
			open = append(open, o)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, o := range open {
			if err := s.enqueueCancel(ctx, tx, o); err != nil {
				return err
			}
		}
		n = len(open)
		return nil
	})
	return n, err
}

// List returns a user's orders, newest first.
func (s *Service) List(ctx context.Context, userID int64, mkt string, openOnly bool, beforeID uint64, limit int) ([]Order, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT `+orderCols+` FROM orders
		WHERE user_id = $1 AND ($2 = '' OR market = $2)
		  AND (NOT $3 OR status IN ('NEW', 'PARTIALLY_FILLED', 'PENDING_HOLD'))
		  AND ($4::bigint = 0 OR id < $4::bigint)
		ORDER BY id DESC LIMIT $5`, userID, mkt, openOnly, int64(beforeID), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// Sweep re-drives PENDING_HOLD orders older than SweepAfter (crash recovery
// for the window between "order persisted" and "command in the outbox").
func (s *Service) Sweep(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+orderCols+` FROM orders
		WHERE status = 'PENDING_HOLD' AND created_at < $1 ORDER BY id LIMIT 100`, s.now().Add(-s.cfg.SweepAfter))
	if err != nil {
		return 0, err
	}
	var stale []Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		stale = append(stale, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, o := range stale {
		if _, err := s.completePlacement(ctx, o); err != nil && !errors.Is(err, ErrInsufficientFunds) {
			slog.WarnContext(ctx, "sweep: could not finish order", "order", o.ID, "err", err)
		}
	}
	return len(stale), nil
}

// RunSweeper runs Sweep every interval until ctx is cancelled.
func (s *Service) RunSweeper(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := s.Sweep(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("sweep failed", "err", err)
			}
		}
	}
}

// Spec returns the market spec for formatting an order.
func (s *Service) Spec(symbol string) (market.Spec, bool) {
	mi, ok := s.markets.get(symbol)
	return mi.Spec, ok
}
