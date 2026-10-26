-- +goose Up

-- The last RFC 6238 step a code was accepted for. A code is refused at or below
-- this step, so one code cannot be used twice even by concurrent requests
-- (the update is guarded: WHERE last_used_step < new step).
ALTER TABLE totp_secrets ADD COLUMN last_used_step BIGINT NOT NULL DEFAULT -1;

-- +goose Down
ALTER TABLE totp_secrets DROP COLUMN last_used_step;
