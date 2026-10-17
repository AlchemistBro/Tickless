// Package idempotency makes at-least-once consumers effectively exactly-once:
// the event id is recorded in the SAME database transaction as the effect, so
// either both happen or neither does (SPEC §3 "Exactly-once effects").
package idempotency

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Schema is the dedupe table every consuming service creates in its migrations.
const Schema = `
CREATE TABLE processed_events (
    consumer     TEXT        NOT NULL,
    event_id     TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
CREATE INDEX processed_events_age ON processed_events (processed_at);
`

// First records (consumer, eventID) inside tx and reports whether this is the
// first time it was seen. When it returns false the caller must skip the
// effect (the transaction can still commit).
func First(ctx context.Context, tx pgx.Tx, consumer, eventID string) (bool, error) {
	tag, err := tx.Exec(ctx,
		`INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		consumer, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
