-- 000003_add_salt_and_fallback_to_history.up.sql

ALTER TABLE flag_history ADD COLUMN IF NOT EXISTS fallback_value BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE flag_history ADD COLUMN IF NOT EXISTS salt VARCHAR(64) NOT NULL DEFAULT '';
