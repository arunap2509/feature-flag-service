-- 000005_restore_canonical_environment_version.up.sql
-- Restore canonical environment_version in environments table and remove global cross-tenant sequence

UPDATE environments e
SET environment_version = COALESCE(
    (SELECT MAX(o.env_version) FROM outbox o WHERE o.env_id = e.id),
    e.environment_version
);

DROP SEQUENCE IF EXISTS environment_version_seq;
