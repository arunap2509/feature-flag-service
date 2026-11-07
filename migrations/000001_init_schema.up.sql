-- 000001_init_schema.up.sql

CREATE TABLE IF NOT EXISTS tenants (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS environments (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name VARCHAR(64) NOT NULL,
    environment_version BIGINT NOT NULL DEFAULT 1,
    shard_count INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tenant_environment_name UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS sdk_keys (
    id BIGSERIAL PRIMARY KEY,
    environment_id VARCHAR(64) NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    key VARCHAR(128) NOT NULL UNIQUE,
    key_type VARCHAR(16) NOT NULL CHECK (key_type IN ('client', 'server')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS flags (
    id BIGSERIAL PRIMARY KEY,
    environment_id VARCHAR(64) NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    key VARCHAR(128) NOT NULL,
    state VARCHAR(8) NOT NULL DEFAULT 'off' CHECK (state IN ('on', 'off')),
    version BIGINT NOT NULL DEFAULT 1,
    rules JSONB NOT NULL DEFAULT '[]'::jsonb,
    fallback_value BOOLEAN NOT NULL DEFAULT FALSE,
    salt VARCHAR(64) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_environment_flag_key UNIQUE (environment_id, key)
);

CREATE TABLE IF NOT EXISTS segments (
    id BIGSERIAL PRIMARY KEY,
    environment_id VARCHAR(64) NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    key VARCHAR(128) NOT NULL,
    user_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_environment_segment_key UNIQUE (environment_id, key)
);

CREATE TABLE IF NOT EXISTS flag_history (
    id BIGSERIAL PRIMARY KEY,
    flag_id BIGINT NOT NULL REFERENCES flags(id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    state VARCHAR(8) NOT NULL,
    rules JSONB NOT NULL,
    changed_by VARCHAR(128) NOT NULL DEFAULT 'system',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_flags_environment_id ON flags(environment_id);
CREATE INDEX IF NOT EXISTS idx_flag_history_flag_id ON flag_history(flag_id);
CREATE INDEX IF NOT EXISTS idx_sdk_keys_environment_id ON sdk_keys(environment_id);
CREATE INDEX IF NOT EXISTS idx_segments_environment_id ON segments(environment_id);
