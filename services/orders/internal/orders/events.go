package orders

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	matchingv1 "github.com/tickless/pkg/gen/tickless/matching/v1"
	"github.com/tickless/pkg/idempotency"
	"github.com/tickless/pkg/pg"
)

// Incoming is a decoded matching event with its trace context.
type Incoming struct {
	Ctx   context.Context
	Event *matchingv1.Event
}

const consumerName = "orders-fills"

// ApplyEvents updates the order read model from a batch of matching events in
// one transaction. Events are deduplicated by (market, seq) in the same
// transaction, so redelivery and engine replay are harmless.
func (s *Service) ApplyEvents(ctx context.Context, evs []Incoming) error {
	return pg.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		for _, in := range evs {
			ev := in.Event
			switch ev.Type {
			case matchingv1.EventType_EVENT_TYPE_TRADE, matchingv1.EventType_EVENT_TYPE_CANCELLED, matchingv1.EventType_EVENT_TYPE_REJECTED:
			default:
				continue
			}
			first, err := idempotency.First(ctx, tx, consumerName, fmt.Sprintf("%s:%d", ev.Market, ev.Seq))
			if err != nil {
				return err
			}
			if !first {
				continue
			}
			ectx, span := tracer.Start(in.Ctx, "orders.apply "+ev.Type.String())
			err = s.applyOne(ectx, tx, ev)
			span.End()
			if err != nil {
				return fmt.Errorf("event %s:%d: %w", ev.Market, ev.Seq, err)
			}
		}
		return nil
	})
}

const terminal = `('FILLED', 'CANCELLED', 'REJECTED')`

func (s *Service) applyOne(ctx context.Context, tx pgx.Tx, ev *matchingv1.Event) error {
	switch ev.Type {
	case matchingv1.EventType_EVENT_TYPE_TRADE:
		for _, f := range []struct {
			id     uint64
			leaves int64
		}{{ev.OrderId, ev.TakerLeaves}, {ev.MakerOrderId, ev.MakerLeaves}} {
			if _, err := tx.Exec(ctx, `
				UPDATE orders SET filled_lots = qty_lots - $2,
				    status = CASE WHEN $2 = 0 THEN 'FILLED' ELSE 'PARTIALLY_FILLED' END, updated_at = now()
				WHERE id = $1 AND status NOT IN `+terminal, int64(f.id), f.leaves); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `UPDATE markets SET last_price_ticks = $2 WHERE symbol = $1`, ev.Market, ev.Price)
		return err

	case matchingv1.EventType_EVENT_TYPE_CANCELLED:
		reason := ""
		if ev.Reason != matchingv1.Reason_REASON_CANCEL_USER {
			reason = reasonText(ev.Reason)
		}
		_, err := tx.Exec(ctx, `UPDATE orders SET status = 'CANCELLED', reject_reason = $2, updated_at = now()
			WHERE id = $1 AND status NOT IN `+terminal, int64(ev.OrderId), reason)
		return err

	case matchingv1.EventType_EVENT_TYPE_REJECTED:
		switch ev.Reason {
		case matchingv1.Reason_REASON_DUPLICATE_ORDER:
			return nil // a re-delivered place command; the original order is unaffected
		case matchingv1.Reason_REASON_UNKNOWN_ORDER:
			return s.retryCancel(ctx, tx, ev)
		}
		_, err := tx.Exec(ctx, `UPDATE orders SET status = 'REJECTED', reject_reason = $2, updated_at = now()
			WHERE id = $1 AND status NOT IN `+terminal, int64(ev.OrderId), reasonText(ev.Reason))
		return err
	}
	return nil
}

// retryCancel handles "the engine does not know this order". If our database
// still shows it open, the cancel overtook the place command (or was lost), so
// the cancel is sent again (bounded).
func (s *Service) retryCancel(ctx context.Context, tx pgx.Tx, ev *matchingv1.Event) error {
	o, err := scanOrder(tx.QueryRow(ctx, `SELECT `+orderCols+` FROM orders WHERE id = $1 FOR UPDATE`, int64(ev.OrderId)))
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil
		}
		return err
	}
	if !o.Open() {
		return nil
	}
	var attempts int
	if err := tx.QueryRow(ctx, `UPDATE orders SET cancel_attempts = cancel_attempts + 1 WHERE id = $1 RETURNING cancel_attempts`, int64(o.ID)).Scan(&attempts); err != nil {
		return err
	}
	if attempts > s.cfg.MaxCancelRetries {
		return nil
	}
	return s.enqueueCancel(ctx, tx, o)
}

func reasonText(r matchingv1.Reason) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(r.String(), "REASON_"), "_", " "))
}
