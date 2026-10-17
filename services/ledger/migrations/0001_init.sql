-- +goose Up

-- Accounts: one per (user, asset, kind). System accounts use user_id 0.
--   AVAILABLE / HELD : user funds (never negative)
--   FEE              : exchange fee income per asset (never negative)
--   EXTERNAL         : the outside world; mirrors deposits/withdrawals, negative by design
CREATE TABLE accounts (
    id      BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    asset   TEXT   NOT NULL,
    kind    TEXT   NOT NULL CHECK (kind IN ('AVAILABLE', 'HELD', 'FEE', 'EXTERNAL')),
    UNIQUE (user_id, asset, kind)
);

-- Denormalised balances, updated in the same transaction as the postings.
-- SPEC §6.2: no balance except EXTERNAL may ever be negative; the database
-- enforces it, so no code path can violate it.
CREATE TABLE balances (
    account_id BIGINT PRIMARY KEY REFERENCES accounts (id),
    kind       TEXT           NOT NULL,
    amount     NUMERIC(39, 0) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT balance_non_negative CHECK (kind = 'EXTERNAL' OR amount >= 0)
);

CREATE TABLE journal_entries (
    id           BIGSERIAL PRIMARY KEY,
    business_key TEXT        NOT NULL UNIQUE, -- idempotency key of the business event
    kind         TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE postings (
    id         BIGSERIAL PRIMARY KEY,
    entry_id   BIGINT         NOT NULL REFERENCES journal_entries (id),
    account_id BIGINT         NOT NULL REFERENCES accounts (id),
    asset      TEXT           NOT NULL,
    amount     NUMERIC(39, 0) NOT NULL CHECK (amount <> 0),
    created_at TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX postings_entry ON postings (entry_id);
CREATE INDEX postings_account ON postings (account_id, id DESC);

-- SPEC §6.1: every journal entry sums to zero per asset. Enforced at COMMIT by
-- a deferred constraint trigger, in addition to the check in application code.
-- +goose StatementBegin
CREATE FUNCTION check_entry_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE bad RECORD;
BEGIN
    SELECT asset, sum(amount) AS s INTO bad
    FROM postings WHERE entry_id = NEW.entry_id
    GROUP BY asset HAVING sum(amount) <> 0 LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'journal entry % is unbalanced for asset % (sum %)', NEW.entry_id, bad.asset, bad.s
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END $$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER postings_balanced
    AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_entry_balanced();

-- SPEC §4: balances must always equal the sum of postings. Returns offending accounts.
-- +goose StatementBegin
CREATE FUNCTION ledger_verify() RETURNS TABLE (account_id BIGINT, balance NUMERIC, postings_sum NUMERIC)
LANGUAGE sql STABLE AS $$
    SELECT b.account_id, b.amount, COALESCE(p.s, 0)
    FROM balances b
    LEFT JOIN (SELECT account_id, sum(amount) AS s FROM postings GROUP BY account_id) p ON p.account_id = b.account_id
    WHERE b.amount <> COALESCE(p.s, 0)
$$;
-- +goose StatementEnd

CREATE TABLE holds (
    order_id   BIGINT PRIMARY KEY,
    user_id    BIGINT         NOT NULL,
    asset      TEXT           NOT NULL,
    amount     NUMERIC(39, 0) NOT NULL CHECK (amount > 0),
    remaining  NUMERIC(39, 0) NOT NULL CHECK (remaining >= 0),
    status     TEXT           NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED')),
    created_at TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX holds_user ON holds (user_id) WHERE status = 'ACTIVE';

CREATE TABLE processed_events (
    consumer     TEXT        NOT NULL,
    event_id     TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
CREATE INDEX processed_events_age ON processed_events (processed_at);

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
DROP TABLE outbox, processed_events, holds, postings, journal_entries, balances, accounts CASCADE;
DROP FUNCTION IF EXISTS ledger_verify(), check_entry_balanced();
