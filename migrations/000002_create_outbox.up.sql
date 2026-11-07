-- 000002_create_outbox.up.sql

CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    env_id VARCHAR(64) NOT NULL,
    env_version BIGINT NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for performance and ordering
CREATE INDEX IF NOT EXISTS idx_outbox_env_version ON outbox(env_id, env_version);

-- Create logical replication publication specifically for outbox table inserts
CREATE PUBLICATION outbox_pub FOR TABLE outbox WITH (publish = 'insert');
