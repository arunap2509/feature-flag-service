package repository

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/model"
)

var (
	ErrNotFound        = errors.New(constants.ErrMsgRecordNotFound)
	ErrVersionMismatch = errors.New(constants.ErrMsgVersionMismatch)
)

type Repository struct {
	db *sql.DB
}

func New(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func randomSalt(n int) string {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "fallback_salt"
	}
	return hex.EncodeToString(bytes)
}

func insertOutbox(ctx context.Context, tx *sql.Tx, envID string, envVersion int64, eventType string, payload model.OutboxEventPayload) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal outbox payload: %w", err)
	}
	query := `INSERT INTO outbox (env_id, env_version, event_type, payload) VALUES ($1, $2, $3, $4)`
	if _, err := tx.ExecContext(ctx, query, envID, envVersion, eventType, payloadJSON); err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}
	return nil
}

// --- Tenants ---

func (r *Repository) CreateTenant(ctx context.Context, id, name string) (*model.Tenant, error) {
	query := `INSERT INTO tenants (id, name) VALUES ($1, $2) RETURNING id, name, created_at`
	t := &model.Tenant{}
	err := r.db.QueryRowContext(ctx, query, id, name).Scan(&t.ID, &t.Name, &t.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create tenant: %w", err)
	}
	return t, nil
}

func (r *Repository) GetTenant(ctx context.Context, id string) (*model.Tenant, error) {
	query := `SELECT id, name, created_at FROM tenants WHERE id = $1`
	t := &model.Tenant{}
	err := r.db.QueryRowContext(ctx, query, id).Scan(&t.ID, &t.Name, &t.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (r *Repository) ListTenants(ctx context.Context) ([]model.Tenant, error) {
	query := `SELECT id, name, created_at FROM tenants ORDER BY created_at DESC`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tenants := []model.Tenant{}
	for rows.Next() {
		t := model.Tenant{}
		if err := rows.Scan(&t.ID, &t.Name, &t.CreatedAt); err != nil {
			return nil, err
		}
		tenants = append(tenants, t)
	}
	return tenants, nil
}

// --- Environments ---

func (r *Repository) CreateEnvironment(ctx context.Context, id, tenantID, name string) (*model.Environment, error) {
	query := `INSERT INTO environments (id, tenant_id, name, shard_count) 
              VALUES ($1, $2, $3, $4) 
              RETURNING id, tenant_id, name, environment_version, shard_count, created_at`
	env := &model.Environment{}
	err := r.db.QueryRowContext(ctx, query, id, tenantID, name, constants.DefaultShardCount).
		Scan(&env.ID, &env.TenantID, &env.Name, &env.EnvironmentVersion, &env.ShardCount, &env.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create environment: %w", err)
	}
	return env, nil
}

func (r *Repository) GetEnvironment(ctx context.Context, id string) (*model.Environment, error) {
	query := `SELECT id, tenant_id, name, environment_version, shard_count, created_at FROM environments WHERE id = $1`
	env := &model.Environment{}
	err := r.db.QueryRowContext(ctx, query, id).
		Scan(&env.ID, &env.TenantID, &env.Name, &env.EnvironmentVersion, &env.ShardCount, &env.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return env, nil
}

func (r *Repository) ListEnvironments(ctx context.Context, tenantID string) ([]model.Environment, error) {
	query := `SELECT id, tenant_id, name, environment_version, shard_count, created_at FROM environments WHERE tenant_id = $1 ORDER BY created_at ASC`
	rows, err := r.db.QueryContext(ctx, query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	envs := []model.Environment{}
	for rows.Next() {
		env := model.Environment{}
		if err := rows.Scan(&env.ID, &env.TenantID, &env.Name, &env.EnvironmentVersion, &env.ShardCount, &env.CreatedAt); err != nil {
			return nil, err
		}
		envs = append(envs, env)
	}
	return envs, nil
}

// --- SDK Keys ---

func (r *Repository) CreateSDKKey(ctx context.Context, envID string, req model.CreateSDKKeyRequest) (*model.SDKKey, error) {
	keyToken := req.Key
	if keyToken == "" {
		prefix := constants.PrefixSDKClient
		if req.KeyType == constants.KeyTypeServer {
			prefix = constants.PrefixSDKServer
		}
		keyToken = fmt.Sprintf("%s%s", prefix, randomSalt(constants.DefaultKeyTokenByteLength))
	}

	query := `INSERT INTO sdk_keys (environment_id, key, key_type) VALUES ($1, $2, $3) RETURNING id, environment_id, key, key_type, created_at`
	k := &model.SDKKey{}
	err := r.db.QueryRowContext(ctx, query, envID, keyToken, req.KeyType).Scan(&k.ID, &k.EnvironmentID, &k.Key, &k.KeyType, &k.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create sdk key: %w", err)
	}
	return k, nil
}

func (r *Repository) ListSDKKeys(ctx context.Context, envID string) ([]model.SDKKey, error) {
	query := `SELECT id, environment_id, key, key_type, created_at FROM sdk_keys WHERE environment_id = $1 ORDER BY created_at ASC`
	rows, err := r.db.QueryContext(ctx, query, envID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := []model.SDKKey{}
	for rows.Next() {
		k := model.SDKKey{}
		if err := rows.Scan(&k.ID, &k.EnvironmentID, &k.Key, &k.KeyType, &k.CreatedAt); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, nil
}

// --- Flags ---

func (r *Repository) CreateFlag(ctx context.Context, envID string, req model.CreateFlagRequest, changedBy string) (*model.Flag, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	state := req.State
	if state == "" {
		state = constants.FlagStateOff
	}
	rulesJSON := req.Rules
	if len(rulesJSON) == 0 {
		rulesJSON = json.RawMessage("[]")
	}
	salt := randomSalt(constants.DefaultSaltByteLength)

	// 1. Insert flag (starts at version 1)
	insertFlagQuery := `
		INSERT INTO flags (environment_id, key, state, version, rules, fallback_value, salt)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, environment_id, key, state, version, rules, fallback_value, salt, updated_at
	`
	flag := &model.Flag{}
	err = tx.QueryRowContext(ctx, insertFlagQuery, envID, req.Key, state, constants.DefaultFlagVersion, rulesJSON, req.FallbackValue, salt).
		Scan(&flag.ID, &flag.EnvironmentID, &flag.Key, &flag.State, &flag.Version, &flag.Rules, &flag.FallbackValue, &flag.Salt, &flag.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to insert flag: %w", err)
	}

	// 2. Increment environment_version
	var nextEnvVersion int64
	updateEnvQuery := `UPDATE environments SET environment_version = environment_version + 1 WHERE id = $1 RETURNING environment_version`
	if err := tx.QueryRowContext(ctx, updateEnvQuery, envID).Scan(&nextEnvVersion); err != nil {
		return nil, fmt.Errorf("failed to increment environment version: %w", err)
	}

	// 3. Insert into flag_history
	insertHistoryQuery := `
		INSERT INTO flag_history (flag_id, version, state, rules, changed_by)
		VALUES ($1, $2, $3, $4, $5)
	`
	if _, err := tx.ExecContext(ctx, insertHistoryQuery, flag.ID, flag.Version, flag.State, flag.Rules, changedBy); err != nil {
		return nil, fmt.Errorf("failed to insert flag history: %w", err)
	}

	// 4. Insert into outbox (Transactional Outbox)
	outboxPayload := model.OutboxEventPayload{
		EventType:          constants.EventTypeFlagCreated,
		EnvironmentID:      envID,
		EnvironmentVersion: nextEnvVersion,
		FlagKey:            flag.Key,
		State:              flag.State,
		FlagVersion:        flag.Version,
		Rules:              flag.Rules,
		FallbackValue:      flag.FallbackValue,
		ChangedBy:          changedBy,
		Timestamp:          time.Now().UTC(),
	}
	if err := insertOutbox(ctx, tx, envID, nextEnvVersion, constants.EventTypeFlagCreated, outboxPayload); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return flag, nil
}

func (r *Repository) GetFlag(ctx context.Context, envID, key string) (*model.Flag, error) {
	query := `SELECT id, environment_id, key, state, version, rules, fallback_value, salt, updated_at 
              FROM flags WHERE environment_id = $1 AND key = $2`
	flag := &model.Flag{}
	err := r.db.QueryRowContext(ctx, query, envID, key).
		Scan(&flag.ID, &flag.EnvironmentID, &flag.Key, &flag.State, &flag.Version, &flag.Rules, &flag.FallbackValue, &flag.Salt, &flag.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return flag, nil
}

func (r *Repository) ListFlags(ctx context.Context, envID string) ([]model.Flag, error) {
	query := `SELECT id, environment_id, key, state, version, rules, fallback_value, salt, updated_at 
              FROM flags WHERE environment_id = $1 ORDER BY key ASC`
	rows, err := r.db.QueryContext(ctx, query, envID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	flags := []model.Flag{}
	for rows.Next() {
		f := model.Flag{}
		if err := rows.Scan(&f.ID, &f.EnvironmentID, &f.Key, &f.State, &f.Version, &f.Rules, &f.FallbackValue, &f.Salt, &f.UpdatedAt); err != nil {
			return nil, err
		}
		flags = append(flags, f)
	}
	return flags, nil
}

// UpdateFlag executes optimistic concurrency check via expectedVersion
func (r *Repository) UpdateFlag(ctx context.Context, envID, key string, req model.UpdateFlagRequest, expectedVersion *int64, changedBy string) (*model.Flag, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// 1. Fetch current flag with row lock
	var currentID, currentVer int64
	var currentState, currentSalt string
	var currentFallback bool
	var currentRules json.RawMessage
	selectQuery := `SELECT id, version, state, rules, fallback_value, salt FROM flags WHERE environment_id = $1 AND key = $2 FOR UPDATE`
	err = tx.QueryRowContext(ctx, selectQuery, envID, key).
		Scan(&currentID, &currentVer, &currentState, &currentRules, &currentFallback, &currentSalt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// 2. Optimistic Concurrency Check (If-Match)
	if expectedVersion != nil && currentVer != *expectedVersion {
		return nil, fmt.Errorf("%w: flag is at version %d, expected %d", ErrVersionMismatch, currentVer, *expectedVersion)
	}

	// 3. Compute new values
	nextVer := currentVer + 1
	newState := currentState
	if req.State != nil {
		newState = *req.State
	}
	newRules := currentRules
	if len(req.Rules) > 0 {
		newRules = req.Rules
	}
	newFallback := currentFallback
	if req.FallbackValue != nil {
		newFallback = *req.FallbackValue
	}
	newSalt := currentSalt
	if req.ReshuffleSalt {
		newSalt = randomSalt(constants.DefaultSaltByteLength)
	}

	// 4. Update flag
	updateFlagQuery := `
		UPDATE flags 
		SET state = $1, version = $2, rules = $3, fallback_value = $4, salt = $5, updated_at = NOW()
		WHERE id = $6
		RETURNING id, environment_id, key, state, version, rules, fallback_value, salt, updated_at
	`
	updatedFlag := &model.Flag{}
	err = tx.QueryRowContext(ctx, updateFlagQuery, newState, nextVer, newRules, newFallback, newSalt, currentID).
		Scan(&updatedFlag.ID, &updatedFlag.EnvironmentID, &updatedFlag.Key, &updatedFlag.State, &updatedFlag.Version, &updatedFlag.Rules, &updatedFlag.FallbackValue, &updatedFlag.Salt, &updatedFlag.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to update flag: %w", err)
	}

	// 5. Increment environment_version
	var nextEnvVersion int64
	updateEnvQuery := `UPDATE environments SET environment_version = environment_version + 1 WHERE id = $1 RETURNING environment_version`
	if err := tx.QueryRowContext(ctx, updateEnvQuery, envID).Scan(&nextEnvVersion); err != nil {
		return nil, fmt.Errorf("failed to increment environment version: %w", err)
	}

	// 6. Insert flag_history
	insertHistoryQuery := `INSERT INTO flag_history (flag_id, version, state, rules, changed_by) VALUES ($1, $2, $3, $4, $5)`
	if _, err := tx.ExecContext(ctx, insertHistoryQuery, updatedFlag.ID, updatedFlag.Version, updatedFlag.State, updatedFlag.Rules, changedBy); err != nil {
		return nil, fmt.Errorf("failed to record history: %w", err)
	}

	// 7. Insert into outbox (Transactional Outbox)
	outboxPayload := model.OutboxEventPayload{
		EventType:          constants.EventTypeFlagUpdated,
		EnvironmentID:      envID,
		EnvironmentVersion: nextEnvVersion,
		FlagKey:            updatedFlag.Key,
		State:              updatedFlag.State,
		FlagVersion:        updatedFlag.Version,
		Rules:              updatedFlag.Rules,
		FallbackValue:      updatedFlag.FallbackValue,
		ChangedBy:          changedBy,
		Timestamp:          time.Now().UTC(),
	}
	if err := insertOutbox(ctx, tx, envID, nextEnvVersion, constants.EventTypeFlagUpdated, outboxPayload); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return updatedFlag, nil
}

// KillFlag unconditionally flips state to 'off', bypassing version checks
func (r *Repository) KillFlag(ctx context.Context, envID, key string, changedBy string) (*model.Flag, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// 1. Fetch current flag with row lock
	var currentID, currentVer int64
	var currentRules json.RawMessage
	selectQuery := `SELECT id, version, rules FROM flags WHERE environment_id = $1 AND key = $2 FOR UPDATE`
	err = tx.QueryRowContext(ctx, selectQuery, envID, key).Scan(&currentID, &currentVer, &currentRules)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// 2. Unconditionally set state to 'off' and bump version
	nextVer := currentVer + 1
	updateQuery := `
		UPDATE flags 
		SET state = $1, version = $2, updated_at = NOW()
		WHERE id = $3
		RETURNING id, environment_id, key, state, version, rules, fallback_value, salt, updated_at
	`
	killedFlag := &model.Flag{}
	err = tx.QueryRowContext(ctx, updateQuery, constants.FlagStateOff, nextVer, currentID).
		Scan(&killedFlag.ID, &killedFlag.EnvironmentID, &killedFlag.Key, &killedFlag.State, &killedFlag.Version, &killedFlag.Rules, &killedFlag.FallbackValue, &killedFlag.Salt, &killedFlag.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to kill flag: %w", err)
	}

	// 3. Increment environment_version
	var nextEnvVersion int64
	updateEnvQuery := `UPDATE environments SET environment_version = environment_version + 1 WHERE id = $1 RETURNING environment_version`
	if err := tx.QueryRowContext(ctx, updateEnvQuery, envID).Scan(&nextEnvVersion); err != nil {
		return nil, fmt.Errorf("failed to increment environment version: %w", err)
	}

	// 4. Record history
	insertHistoryQuery := `INSERT INTO flag_history (flag_id, version, state, rules, changed_by) VALUES ($1, $2, $3, $4, $5)`
	if _, err := tx.ExecContext(ctx, insertHistoryQuery, killedFlag.ID, killedFlag.Version, constants.FlagStateOff, currentRules, changedBy); err != nil {
		return nil, fmt.Errorf("failed to record history: %w", err)
	}

	// 5. Insert into outbox (Transactional Outbox)
	outboxPayload := model.OutboxEventPayload{
		EventType:          constants.EventTypeFlagKilled,
		EnvironmentID:      envID,
		EnvironmentVersion: nextEnvVersion,
		FlagKey:            killedFlag.Key,
		State:              constants.FlagStateOff,
		FlagVersion:        killedFlag.Version,
		Rules:              killedFlag.Rules,
		FallbackValue:      killedFlag.FallbackValue,
		ChangedBy:          changedBy,
		Timestamp:          time.Now().UTC(),
	}
	if err := insertOutbox(ctx, tx, envID, nextEnvVersion, constants.EventTypeFlagKilled, outboxPayload); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return killedFlag, nil
}

// RollbackFlag copies rules from an older version and saves them as a new forward version
func (r *Repository) RollbackFlag(ctx context.Context, envID, key string, targetVersion int64, changedBy string) (*model.Flag, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// 1. Fetch current flag
	var currentID, currentVer int64
	selectQuery := `SELECT id, version FROM flags WHERE environment_id = $1 AND key = $2 FOR UPDATE`
	err = tx.QueryRowContext(ctx, selectQuery, envID, key).Scan(&currentID, &currentVer)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// 2. Fetch target rules from history
	var targetState string
	var targetRules json.RawMessage
	historyQuery := `SELECT state, rules FROM flag_history WHERE flag_id = $1 AND version = $2`
	err = tx.QueryRowContext(ctx, historyQuery, currentID, targetVersion).Scan(&targetState, &targetRules)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("target version %d not found in history: %w", targetVersion, ErrNotFound)
	}
	if err != nil {
		return nil, err
	}

	// 3. Update flag to target rules, bumping version forward (currentVer + 1)
	nextVer := currentVer + 1
	updateQuery := `
		UPDATE flags 
		SET state = $1, rules = $2, version = $3, updated_at = NOW()
		WHERE id = $4
		RETURNING id, environment_id, key, state, version, rules, fallback_value, salt, updated_at
	`
	rolledBackFlag := &model.Flag{}
	err = tx.QueryRowContext(ctx, updateQuery, targetState, targetRules, nextVer, currentID).
		Scan(&rolledBackFlag.ID, &rolledBackFlag.EnvironmentID, &rolledBackFlag.Key, &rolledBackFlag.State, &rolledBackFlag.Version, &rolledBackFlag.Rules, &rolledBackFlag.FallbackValue, &rolledBackFlag.Salt, &rolledBackFlag.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to rollback flag: %w", err)
	}

	// 4. Increment environment_version
	var nextEnvVersion int64
	updateEnvQuery := `UPDATE environments SET environment_version = environment_version + 1 WHERE id = $1 RETURNING environment_version`
	if err := tx.QueryRowContext(ctx, updateEnvQuery, envID).Scan(&nextEnvVersion); err != nil {
		return nil, fmt.Errorf("failed to increment environment version: %w", err)
	}

	// 5. Record rollback in history
	insertHistoryQuery := `INSERT INTO flag_history (flag_id, version, state, rules, changed_by) VALUES ($1, $2, $3, $4, $5)`
	rollbackAuthor := fmt.Sprintf("%s (rollback to v%d)", changedBy, targetVersion)
	if _, err := tx.ExecContext(ctx, insertHistoryQuery, rolledBackFlag.ID, rolledBackFlag.Version, targetState, targetRules, rollbackAuthor); err != nil {
		return nil, fmt.Errorf("failed to record history: %w", err)
	}

	// 6. Insert into outbox (Transactional Outbox)
	outboxPayload := model.OutboxEventPayload{
		EventType:          constants.EventTypeFlagRolledBack,
		EnvironmentID:      envID,
		EnvironmentVersion: nextEnvVersion,
		FlagKey:            rolledBackFlag.Key,
		State:              rolledBackFlag.State,
		FlagVersion:        rolledBackFlag.Version,
		Rules:              rolledBackFlag.Rules,
		FallbackValue:      rolledBackFlag.FallbackValue,
		ChangedBy:          changedBy,
		Timestamp:          time.Now().UTC(),
	}
	if err := insertOutbox(ctx, tx, envID, nextEnvVersion, constants.EventTypeFlagRolledBack, outboxPayload); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return rolledBackFlag, nil
}

func (r *Repository) GetFlagHistory(ctx context.Context, envID, key string) ([]model.FlagHistory, error) {
	query := `
		SELECT h.id, h.flag_id, h.version, h.state, h.rules, h.changed_by, h.created_at
		FROM flag_history h
		JOIN flags f ON f.id = h.flag_id
		WHERE f.environment_id = $1 AND f.key = $2
		ORDER BY h.version DESC
	`
	rows, err := r.db.QueryContext(ctx, query, envID, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	history := []model.FlagHistory{}
	for rows.Next() {
		h := model.FlagHistory{}
		if err := rows.Scan(&h.ID, &h.FlagID, &h.Version, &h.State, &h.Rules, &h.ChangedBy, &h.CreatedAt); err != nil {
			return nil, err
		}
		history = append(history, h)
	}
	return history, nil
}

// --- Segments ---

func (r *Repository) CreateSegment(ctx context.Context, envID string, req model.CreateSegmentRequest) (*model.Segment, error) {
	userIDsJSON, err := json.Marshal(req.UserIDs)
	if err != nil {
		userIDsJSON = []byte("[]")
	}

	query := `
		INSERT INTO segments (environment_id, key, user_ids)
		VALUES ($1, $2, $3)
		RETURNING id, environment_id, key, user_ids, updated_at
	`
	seg := &model.Segment{}
	err = r.db.QueryRowContext(ctx, query, envID, req.Key, userIDsJSON).
		Scan(&seg.ID, &seg.EnvironmentID, &seg.Key, &seg.UserIDs, &seg.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment: %w", err)
	}
	return seg, nil
}

func (r *Repository) GetSegment(ctx context.Context, envID, key string) (*model.Segment, error) {
	query := `SELECT id, environment_id, key, user_ids, updated_at FROM segments WHERE environment_id = $1 AND key = $2`
	seg := &model.Segment{}
	err := r.db.QueryRowContext(ctx, query, envID, key).
		Scan(&seg.ID, &seg.EnvironmentID, &seg.Key, &seg.UserIDs, &seg.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return seg, nil
}

func (r *Repository) ListSegments(ctx context.Context, envID string) ([]model.Segment, error) {
	query := `SELECT id, environment_id, key, user_ids, updated_at FROM segments WHERE environment_id = $1 ORDER BY key ASC`
	rows, err := r.db.QueryContext(ctx, query, envID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	segs := []model.Segment{}
	for rows.Next() {
		s := model.Segment{}
		if err := rows.Scan(&s.ID, &s.EnvironmentID, &s.Key, &s.UserIDs, &s.UpdatedAt); err != nil {
			return nil, err
		}
		segs = append(segs, s)
	}
	return segs, nil
}

func (r *Repository) UpdateSegment(ctx context.Context, envID, key string, req model.UpdateSegmentRequest) (*model.Segment, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	userIDsJSON, err := json.Marshal(req.UserIDs)
	if err != nil {
		return nil, err
	}

	query := `
		UPDATE segments SET user_ids = $1, updated_at = NOW() 
		WHERE environment_id = $2 AND key = $3
		RETURNING id, environment_id, key, user_ids, updated_at
	`
	seg := &model.Segment{}
	err = tx.QueryRowContext(ctx, query, userIDsJSON, envID, key).
		Scan(&seg.ID, &seg.EnvironmentID, &seg.Key, &seg.UserIDs, &seg.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Increment environment version because segment membership changes can alter flag answers
	var nextEnvVersion int64
	updateEnvQuery := `UPDATE environments SET environment_version = environment_version + 1 WHERE id = $1 RETURNING environment_version`
	if err := tx.QueryRowContext(ctx, updateEnvQuery, envID).Scan(&nextEnvVersion); err != nil {
		return nil, fmt.Errorf("failed to increment environment version: %w", err)
	}

	// Insert into outbox (Transactional Outbox)
	outboxPayload := model.OutboxEventPayload{
		EventType:          constants.EventTypeSegmentUpdated,
		EnvironmentID:      envID,
		EnvironmentVersion: nextEnvVersion,
		SegmentKey:         seg.Key,
		UserIDs:            seg.UserIDs,
		ChangedBy:          "segment_admin",
		Timestamp:          time.Now().UTC(),
	}
	if err := insertOutbox(ctx, tx, envID, nextEnvVersion, constants.EventTypeSegmentUpdated, outboxPayload); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return seg, nil
}
