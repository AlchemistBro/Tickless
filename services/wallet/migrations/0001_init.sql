-- +goose Up

-- One deposit address per (user, chain), derived from the chain's account xpub
-- at a non-hardened index allocated from chain_counters (ADR 0009). Indexes may
-- have gaps (a lost race wastes one); nothing restores these keys from a seed
-- with a gap limit, so gaps are harmless.
CREATE TABLE chain_counters (
    chain      TEXT    PRIMARY KEY,
    next_index INTEGER NOT NULL CHECK (next_index >= 0 AND next_index < 2147483648)
);

CREATE TABLE deposit_addresses (
    user_id          BIGINT      NOT NULL CHECK (user_id > 0),
    chain            TEXT        NOT NULL,
    derivation_index INTEGER     NOT NULL CHECK (derivation_index >= 0),
    address          TEXT        NOT NULL,
    registered       BOOLEAN     NOT NULL DEFAULT false, -- known to chainwatcher
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, chain),
    UNIQUE (chain, derivation_index),
    UNIQUE (chain, address)
);

-- +goose Down
DROP TABLE deposit_addresses, chain_counters;
