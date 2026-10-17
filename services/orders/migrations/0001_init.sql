-- +goose Up

CREATE TABLE markets (
    symbol          TEXT PRIMARY KEY,
    base_asset      TEXT           NOT NULL,
    quote_asset     TEXT           NOT NULL,
    base_decimals   INT            NOT NULL,
    quote_decimals  INT            NOT NULL,
    tick_size       BIGINT         NOT NULL CHECK (tick_size > 0),  -- quote smallest units per tick
    lot_size        BIGINT         NOT NULL CHECK (lot_size > 0),   -- base smallest units per lot
    min_notional    NUMERIC(39, 0) NOT NULL,                        -- quote smallest units
    maker_fee_bps   INT            NOT NULL CHECK (maker_fee_bps BETWEEN 0 AND 10000),
    taker_fee_bps   INT            NOT NULL CHECK (taker_fee_bps BETWEEN 0 AND 10000),
    min_price_ticks BIGINT         NOT NULL,
    max_price_ticks BIGINT         NOT NULL,
    -- Limit prices must stay within +-band_bps of the last trade price once one exists;
    -- this keeps the live price-level count (and so sorted-slice insert cost) bounded (ADR 0002).
    band_bps        INT            NOT NULL DEFAULT 5000,
    last_price_ticks BIGINT,
    status          TEXT           NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'HALTED')),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT now()
);

INSERT INTO markets (symbol, base_asset, quote_asset, base_decimals, quote_decimals, tick_size, lot_size, min_notional,
                     maker_fee_bps, taker_fee_bps, min_price_ticks, max_price_ticks) VALUES
    -- tick 0.01 USDT, lot 0.00001 BTC, min notional 5 USDT
    ('BTC-USDT', 'BTC', 'USDT', 8, 6, 10000, 1000, 5000000, 10, 20, 1, 1000000000),
    -- tick 0.01 USDT, lot 0.0001 ETH, min notional 5 USDT
    ('ETH-USDT', 'ETH', 'USDT', 18, 6, 10000, 100000000000000, 5000000, 10, 20, 1, 1000000000),
    -- tick 0.000001 BTC, lot 0.0001 ETH, min notional 0.0001 BTC
    ('ETH-BTC', 'ETH', 'BTC', 18, 8, 100, 100000000000000, 10000, 10, 20, 1, 1000000000);

CREATE SEQUENCE order_id_seq START 1;

CREATE TABLE orders (
    id               BIGINT PRIMARY KEY,
    user_id          BIGINT      NOT NULL,
    market           TEXT        NOT NULL REFERENCES markets (symbol),
    side             SMALLINT    NOT NULL CHECK (side IN (1, 2)), -- 1 buy, 2 sell (engine values)
    order_type       TEXT        NOT NULL CHECK (order_type IN ('limit', 'market')),
    tif              TEXT        NOT NULL CHECK (tif IN ('GTC', 'IOC', 'FOK')),
    post_only        BOOLEAN     NOT NULL DEFAULT false,
    price_ticks      BIGINT      NOT NULL DEFAULT 0, -- limit price, or protective bound for market orders
    qty_lots         BIGINT      NOT NULL CHECK (qty_lots > 0),
    filled_lots      BIGINT      NOT NULL DEFAULT 0 CHECK (filled_lots >= 0 AND filled_lots <= qty_lots),
    status           TEXT        NOT NULL CHECK (status IN ('PENDING_HOLD', 'NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED')),
    client_order_id  TEXT,
    reject_reason    TEXT        NOT NULL DEFAULT '',
    hold_asset       TEXT        NOT NULL,
    hold_amount      NUMERIC(39, 0) NOT NULL,
    cancel_attempts  INT         NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX orders_user_recent ON orders (user_id, id DESC);
CREATE INDEX orders_user_open ON orders (user_id, market) WHERE status IN ('NEW', 'PARTIALLY_FILLED');
CREATE INDEX orders_pending_hold ON orders (created_at) WHERE status = 'PENDING_HOLD';

-- client_order_id is unique per user (idempotent submission). Kept outside the
-- orders table so that orders can later be partitioned by month without
-- losing the global uniqueness guarantee.
CREATE TABLE order_keys (
    user_id         BIGINT NOT NULL,
    client_order_id TEXT   NOT NULL,
    order_id        BIGINT NOT NULL,
    PRIMARY KEY (user_id, client_order_id)
);

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
DROP TABLE outbox, processed_events, order_keys, orders, markets CASCADE;
DROP SEQUENCE order_id_seq;
