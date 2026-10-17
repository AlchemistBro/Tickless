-- +goose Up

-- The ledger is append-only: corrections are new entries, never edits. Even a
-- superuser session running UPDATE/DELETE/TRUNCATE on these tables fails
-- (a superuser could still drop the trigger; that is auditable DDL, not a
-- silent data change).
-- +goose StatementBegin
CREATE FUNCTION forbid_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% on % is forbidden: the ledger is append-only', TG_OP, TG_TABLE_NAME
        USING ERRCODE = '42501';
END $$;
-- +goose StatementEnd

CREATE TRIGGER postings_append_only BEFORE UPDATE OR DELETE ON postings
    FOR EACH ROW EXECUTE FUNCTION forbid_ledger_mutation();
CREATE TRIGGER postings_no_truncate BEFORE TRUNCATE ON postings
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_ledger_mutation();
CREATE TRIGGER journal_entries_append_only BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_ledger_mutation();
CREATE TRIGGER journal_entries_no_truncate BEFORE TRUNCATE ON journal_entries
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_ledger_mutation();

-- A matching event that can never be settled (e.g. the order has no hold) is
-- recorded here and its partition stalls; the ledger never skips an event.
CREATE TABLE settlement_failures (
    id           BIGSERIAL PRIMARY KEY,
    market       TEXT        NOT NULL,
    seq          BIGINT      NOT NULL,
    kafka_partition INT      NOT NULL,
    kafka_offset BIGINT      NOT NULL,
    error        TEXT        NOT NULL,
    attempts     INT         NOT NULL DEFAULT 1,
    first_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (market, seq)
);

-- +goose Down
DROP TABLE settlement_failures;
DROP TRIGGER journal_entries_no_truncate ON journal_entries;
DROP TRIGGER journal_entries_append_only ON journal_entries;
DROP TRIGGER postings_no_truncate ON postings;
DROP TRIGGER postings_append_only ON postings;
DROP FUNCTION forbid_ledger_mutation();
