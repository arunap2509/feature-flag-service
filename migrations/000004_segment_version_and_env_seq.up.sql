-- 000004_segment_version_and_env_seq.up.sql

-- 1. Add version column to segments for optimistic concurrency control (If-Match)
ALTER TABLE segments ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;

-- 2. Create non-blocking global sequence for environment versions to eliminate hot-row locking
CREATE SEQUENCE IF NOT EXISTS environment_version_seq START WITH 1000;
