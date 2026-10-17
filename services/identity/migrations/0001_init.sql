-- +goose Up

CREATE TABLE users (
    id                BIGSERIAL PRIMARY KEY,
    email             TEXT        NOT NULL,
    role              TEXT        NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin')),
    email_verified_at TIMESTAMPTZ,
    frozen            BOOLEAN     NOT NULL DEFAULT false,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Emails are stored normalised (lower-case, trimmed); uniqueness is case-insensitive.
CREATE UNIQUE INDEX users_email_lower ON users (lower(email));

CREATE TABLE credentials (
    user_id       BIGINT PRIMARY KEY REFERENCES users (id),
    password_hash TEXT        NOT NULL, -- argon2id PHC string
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE email_verifications (
    token_hash BYTEA PRIMARY KEY, -- sha256 of the emailed token
    user_id    BIGINT      NOT NULL REFERENCES users (id),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at    TIMESTAMPTZ
);

CREATE TABLE refresh_tokens (
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT      NOT NULL REFERENCES users (id),
    token_hash BYTEA       NOT NULL UNIQUE, -- sha256 of the opaque token
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX refresh_tokens_user ON refresh_tokens (user_id) WHERE revoked_at IS NULL;

CREATE TABLE api_keys (
    key_id     TEXT PRIMARY KEY,
    user_id    BIGINT      NOT NULL REFERENCES users (id),
    label      TEXT        NOT NULL DEFAULT '',
    secret_enc BYTEA       NOT NULL, -- AES-256-GCM sealed HMAC secret (needed to verify signatures)
    can_trade  BOOLEAN     NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ
);
CREATE INDEX api_keys_user ON api_keys (user_id);

-- TOTP seeds are encrypted at rest with a key from the environment (used from Phase 6).
CREATE TABLE totp_secrets (
    user_id      BIGINT PRIMARY KEY REFERENCES users (id),
    secret_enc   BYTEA       NOT NULL,
    confirmed_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

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
DROP TABLE outbox, totp_secrets, api_keys, refresh_tokens, email_verifications, credentials, users CASCADE;
