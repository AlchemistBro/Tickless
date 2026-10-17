package store

import (
	"context"
	"errors"
	"fmt"

	commonv1 "github.com/tickless/pkg/gen/tickless/common/v1"
	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/idempotency"
	"github.com/tickless/pkg/market"
	"github.com/tickless/pkg/money"
	"go.opentelemetry.io/otel"
)

// MarketLookup resolves market configuration for settlement.
type MarketLookup interface {
	Spec(symbol string) (market.Spec, bool)
}

// ErrUnknownMarket is returned (and retried by the consumer) when a trade
// arrives for a market whose configuration is not loaded yet.
var ErrUnknownMarket = errors.New("ledger: unknown market")

// Incoming is a decoded matching event with the context of its trace and its
// Kafka coordinates (for failure records).
type Incoming struct {
	Ctx       context.Context
	Event     *matchingv1.Event
	Partition int32
	Offset    int64
}

// ErrNoActiveHold means a trade references an order that has no active hold:
// retrying can never fix it, so it is a permanent settlement failure.
var ErrNoActiveHold = errors.New("ledger: order has no active hold")

// SettlementError is returned by ApplyEvents when an event cannot be settled
// and retrying cannot help. It identifies the offending event; callers record
// it with RecordFailure and stall the partition (the ledger never skips).
type SettlementError struct {
	Market    string
	Seq       uint64
	Partition int32
	Offset    int64
	Err       error
}

func (e *SettlementError) Error() string {
	return fmt.Sprintf("cannot settle %s:%d (partition %d offset %d): %v", e.Market, e.Seq, e.Partition, e.Offset, e.Err)
}
func (e *SettlementError) Unwrap() error { return e.Err }

// isPermanent classifies business-rule failures. Infrastructure errors
// (database down, unknown market not loaded yet) stay transient.
func isPermanent(err error) bool {
	return errors.Is(err, ErrNoActiveHold) || errors.Is(err, ErrUnbalanced) || errors.Is(err, ErrInsufficientFunds) ||
		errors.Is(err, money.ErrOverflow) || errors.Is(err, money.ErrInvalid) || errors.Is(err, money.ErrDivByZero)
}

// RecordFailure stores the failed event in settlement_failures (idempotent per
// market+seq; repeated sightings bump attempts).
func (s *Store) RecordFailure(ctx context.Context, e *SettlementError) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO settlement_failures (market, seq, kafka_partition, kafka_offset, error) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (market, seq) DO UPDATE SET attempts = settlement_failures.attempts + 1, last_seen = now(), error = EXCLUDED.error`,
		e.Market, int64(e.Seq), e.Partition, e.Offset, e.Err.Error())
	return err
}

const consumerName = "ledger-settle"

var tracer = otel.Tracer("tickless/ledger")

// ApplyEvents settles a batch of matching events in ONE transaction.
// Each event is deduplicated by (market, seq) inside the same transaction as
// its effects, so redelivery and replay after a matching restart are harmless
// (SPEC §3 exactly-once effects, §6 invariant "settled at most once").
func (s *Store) ApplyEvents(ctx context.Context, markets MarketLookup, evs []Incoming) error {
	return s.inSession(ctx, "trade", func(ss *session) error {
		for _, in := range evs {
			ev := in.Event
			switch ev.Type {
			case matchingv1.EventType_EVENT_TYPE_TRADE, matchingv1.EventType_EVENT_TYPE_CANCELLED, matchingv1.EventType_EVENT_TYPE_REJECTED:
			default:
				continue // accepted / book deltas have no ledger effect
			}
			first, err := idempotency.First(ctx, ss.tx, consumerName, fmt.Sprintf("%s:%d", ev.Market, ev.Seq))
			if err != nil {
				return err
			}
			if !first {
				continue
			}
			ectx, span := tracer.Start(in.Ctx, "ledger.apply "+ev.Type.String())
			err = ss.applyOne(ectx, markets, ev)
			span.End()
			if err != nil {
				if isPermanent(err) {
					return &SettlementError{Market: ev.Market, Seq: ev.Seq, Partition: in.Partition, Offset: in.Offset, Err: err}
				}
				return fmt.Errorf("event %s:%d: %w", ev.Market, ev.Seq, err)
			}
		}
		return nil
	})
}

func (ss *session) applyOne(ctx context.Context, markets MarketLookup, ev *matchingv1.Event) error {
	switch ev.Type {
	case matchingv1.EventType_EVENT_TYPE_TRADE:
		spec, ok := markets.Spec(ev.Market)
		if !ok {
			return fmt.Errorf("%w: %s", ErrUnknownMarket, ev.Market)
		}
		return ss.settleTrade(ctx, spec, ev)
	case matchingv1.EventType_EVENT_TYPE_CANCELLED:
		_, err := ss.release(ctx, ev.OrderId)
		return err
	case matchingv1.EventType_EVENT_TYPE_REJECTED:
		// A rejected cancel (unknown order) or a duplicate place of a live or
		// finished order must NOT touch the original order's hold.
		if ev.Reason == matchingv1.Reason_REASON_UNKNOWN_ORDER || ev.Reason == matchingv1.Reason_REASON_DUPLICATE_ORDER {
			return nil
		}
		_, err := ss.release(ctx, ev.OrderId)
		return err
	}
	return nil
}

// settleTrade posts the trade as one balanced journal entry:
//
//	seller: HELD(base) -base                buyer: HELD(quote) -quote
//	buyer:  AVAILABLE(base) +base-feeBase   seller: AVAILABLE(quote) +quote-feeQuote
//	exchange: FEE(base) +feeBase            exchange: FEE(quote) +feeQuote
//
// Fees are charged in the received asset and rounded UP (toward the exchange,
// SPEC §6.8). The trade price is the maker price; the quote amount is rounded
// down while holds are rounded up, so a hold always covers the trade (ADR 0006).
func (ss *session) settleTrade(ctx context.Context, spec market.Spec, ev *matchingv1.Event) error {
	buyerOrder, buyerUser, sellerOrder, sellerUser := ev.OrderId, int64(ev.UserId), ev.MakerOrderId, int64(ev.MakerUserId)
	buyerBps, sellerBps := spec.TakerFeeBps, spec.MakerFeeBps
	if ev.Side == commonv1.Side_SIDE_SELL { // taker sells: maker is the buyer
		buyerOrder, buyerUser, sellerOrder, sellerUser = ev.MakerOrderId, int64(ev.MakerUserId), ev.OrderId, int64(ev.UserId)
		buyerBps, sellerBps = spec.MakerFeeBps, spec.TakerFeeBps
	}
	baseAmt, err := spec.Money().BaseAmount(ev.Qty)
	if err != nil {
		return err
	}
	quoteAmt, err := spec.Notional(ev.Price, ev.Qty, money.RoundDown)
	if err != nil {
		return err
	}
	feeBase, err := money.Fee(baseAmt, buyerBps)
	if err != nil {
		return err
	}
	feeQuote, err := money.Fee(quoteAmt, sellerBps)
	if err != nil {
		return err
	}
	buyerGets, _ := baseAmt.Sub(feeBase)
	sellerGets, _ := quoteAmt.Sub(feeQuote)
	negBase, _ := baseAmt.Neg()
	negQuote, _ := quoteAmt.Neg()

	ps := []posting{
		{held(sellerUser, spec.Base), negBase},
		{available(buyerUser, spec.Base), buyerGets},
		{held(buyerUser, spec.Quote), negQuote},
		{available(sellerUser, spec.Quote), sellerGets},
	}
	if !feeBase.IsZero() {
		ps = append(ps, posting{acctKey{SystemUser, spec.Base, KindFee}, feeBase})
	}
	if !feeQuote.IsZero() {
		ps = append(ps, posting{acctKey{SystemUser, spec.Quote, KindFee}, feeQuote})
	}
	_, applied, err := ss.post(ctx, fmt.Sprintf("trade:%s:%d", spec.Symbol, ev.TradeId), "trade", ps)
	if err != nil {
		return err
	}
	if !applied {
		return nil
	}
	if err := ss.consumeHold(ctx, buyerOrder, spec.Quote, quoteAmt); err != nil {
		return err
	}
	if err := ss.consumeHold(ctx, sellerOrder, spec.Base, baseAmt); err != nil {
		return err
	}
	// A fully filled order gets its hold excess back (buyer paid less than its limit).
	if ev.TakerLeaves == 0 {
		if _, err := ss.release(ctx, ev.OrderId); err != nil {
			return err
		}
	}
	if ev.MakerLeaves == 0 {
		if _, err := ss.release(ctx, ev.MakerOrderId); err != nil {
			return err
		}
	}
	return nil
}

// consumeHold reduces the remaining amount of an order's hold by what the
// trade consumed and verifies the hold is in the expected asset.
func (ss *session) consumeHold(ctx context.Context, orderID uint64, asset string, amt money.Int128) error {
	tag, err := ss.tx.Exec(ctx, `UPDATE holds SET remaining = remaining - $2::numeric WHERE order_id = $1 AND asset = $3 AND status = 'ACTIVE'`,
		int64(orderID), amt.String(), asset)
	if err != nil {
		return mapDBErr(err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: order %d, asset %s", ErrNoActiveHold, orderID, asset)
	}
	return nil
}
