-- +goose Up

-- A withdrawal. The state is only ever changed by a guarded UPDATE
-- (WHERE status = expected), so two concurrent actors cannot both win a
-- transition (SPEC §1 withdrawals state machine).
CREATE TABLE withdrawals (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT         NOT NULL CHECK (user_id > 0),
    chain           TEXT           NOT NULL CHECK (chain IN ('btc', 'eth')),
    asset           TEXT           NOT NULL,
    amount          NUMERIC(39, 0) NOT NULL CHECK (amount > 0),
    destination     TEXT           NOT NULL,
    status          TEXT           NOT NULL,
    reason          TEXT           NOT NULL DEFAULT '',
    txid            TEXT,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    -- The UTC day the request was made, for the daily limit (ADR 0011).
    day             DATE           NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')::date
);
CREATE INDEX withdrawals_user_day ON withdrawals (user_id, day);
CREATE INDEX withdrawals_status ON withdrawals (status) WHERE status NOT IN ('CONFIRMED', 'REJECTED', 'FAILED');

-- Serialises limit checks per user: the daily total is read and the new row
-- inserted under the same advisory lock, so two concurrent requests cannot
-- both pass a limit that only one of them fits in.
-- (The lock itself is taken in Go; see store.go.)

CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT        NOT NULL,
    key          TEXT        NOT NULL,
    payload      BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox, withdrawals;
