-- +goose Up
ALTER TABLE withdrawals
    ADD COLUMN confirmations BIGINT  NOT NULL DEFAULT 0,
    ADD COLUMN network_fee   TEXT    NOT NULL DEFAULT '',   -- smallest units of fee_asset, paid by the hot wallet
    ADD COLUMN fee_asset     TEXT    NOT NULL DEFAULT '',
    ADD COLUMN reviewed_by   BIGINT,
    ADD COLUMN attempts      INTEGER NOT NULL DEFAULT 0,     -- signing/broadcast attempts, for backoff and alerting
    -- The ledger effect of a final state (settle for CONFIRMED, release for
    -- REJECTED/FAILED) is applied after the state change; this flag records that
    -- it was. A crash in between is repaired by the finaliser (ADR 0011).
    ADD COLUMN ledger_finalized BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX withdrawals_unfinalized ON withdrawals (id) WHERE status IN ('CONFIRMED', 'REJECTED', 'FAILED') AND NOT ledger_finalized;

-- +goose Down
ALTER TABLE withdrawals DROP COLUMN confirmations, DROP COLUMN network_fee, DROP COLUMN fee_asset, DROP COLUMN reviewed_by, DROP COLUMN attempts, DROP COLUMN ledger_finalized;
