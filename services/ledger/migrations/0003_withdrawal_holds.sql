-- +goose Up

-- Funds reserved for a withdrawal. A hold ends exactly once: SETTLED (the
-- funds left the exchange, HELD -> EXTERNAL) or RELEASED (HELD -> AVAILABLE).
-- Both transitions are guarded updates (WHERE status = 'ACTIVE'), so a hold
-- can never be both settled and released.
CREATE TABLE withdrawal_holds (
    withdrawal_id BIGINT PRIMARY KEY,
    user_id       BIGINT         NOT NULL,
    asset         TEXT           NOT NULL,
    amount        NUMERIC(39, 0) NOT NULL CHECK (amount > 0),
    status        TEXT           NOT NULL CHECK (status IN ('ACTIVE', 'SETTLED', 'RELEASED')),
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ
);

-- +goose Down
DROP TABLE withdrawal_holds;
