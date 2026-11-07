-- 000004_segment_version_and_env_seq.down.sql

DROP SEQUENCE IF EXISTS environment_version_seq;
ALTER TABLE segments DROP COLUMN IF EXISTS version;
