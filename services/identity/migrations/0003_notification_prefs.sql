-- +goose Up
-- Opt-in "order filled" emails (SPEC §1 notifications).
ALTER TABLE users ADD COLUMN notify_order_fills BOOLEAN NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE users DROP COLUMN notify_order_fills;
