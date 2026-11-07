-- 000003_add_salt_and_fallback_to_history.down.sql

ALTER TABLE flag_history DROP COLUMN IF EXISTS salt;
ALTER TABLE flag_history DROP COLUMN IF EXISTS fallback_value;
