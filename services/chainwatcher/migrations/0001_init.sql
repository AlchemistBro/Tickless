-- +goose Up

-- Deposit addresses registered by the wallet service. address_key is the
-- case-normalised form used for matching (bech32 is lower case by definition;
-- Ethereum hex is case-insensitive, the mixed case is only an EIP-55 checksum).
CREATE TABLE deposit_addresses (
    chain            TEXT        NOT NULL,
    address          TEXT        NOT NULL,
    address_key      TEXT        NOT NULL,
    user_id          BIGINT      NOT NULL CHECK (user_id > 0),
    derivation_index INTEGER     NOT NULL CHECK (derivation_index >= 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain, address_key)
);
CREATE INDEX deposit_addresses_user ON deposit_addresses (user_id);

-- Every processed block of the best chain as seen by the scanner (ADR 0010).
-- Rows above the common ancestor are deleted on a reorg.
CREATE TABLE blocks (
    chain        TEXT        NOT NULL,
    height       BIGINT      NOT NULL CHECK (height >= 0),
    hash         TEXT        NOT NULL,
    parent_hash  TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain, height)
);

-- SPEC §6 invariant 4: a deposit is identified by (chain, txid, index) and
-- exists once. CONFIRMED is terminal: the ledger credits on that transition.
CREATE TABLE deposits (
    id               BIGSERIAL PRIMARY KEY,
    chain            TEXT           NOT NULL,
    txid             TEXT           NOT NULL,
    idx              BIGINT         NOT NULL CHECK (idx >= -1), -- -1 = the value of an Ethereum transaction itself
    address          TEXT           NOT NULL,
    user_id          BIGINT         NOT NULL,
    asset            TEXT           NOT NULL,
    amount           NUMERIC(39, 0) NOT NULL CHECK (amount > 0),
    status           TEXT           NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'REORGED')),
    block_height     BIGINT         NOT NULL,
    block_hash       TEXT           NOT NULL,
    confirmations    BIGINT         NOT NULL DEFAULT 0,
    detected_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    confirmed_at     TIMESTAMPTZ,
    UNIQUE (chain, txid, idx),
    CONSTRAINT confirmed_has_time CHECK ((status = 'CONFIRMED') = (confirmed_at IS NOT NULL))
);
CREATE INDEX deposits_user ON deposits (user_id, id DESC);
CREATE INDEX deposits_pending ON deposits (chain, block_height) WHERE status = 'PENDING';

-- A CONFIRMED deposit never changes again (it has been credited).
-- +goose StatementBegin
CREATE FUNCTION deposits_confirmed_is_final() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status = 'CONFIRMED' THEN
        RAISE EXCEPTION 'deposit % is CONFIRMED and immutable', OLD.id USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER deposits_confirmed_final BEFORE UPDATE ON deposits
    FOR EACH ROW EXECUTE FUNCTION deposits_confirmed_is_final();

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
DROP TABLE outbox, deposits, blocks, deposit_addresses CASCADE;
DROP FUNCTION IF EXISTS deposits_confirmed_is_final();
