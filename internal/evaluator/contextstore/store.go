package contextstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/evaluator/session"
)

// Store provides durable, fast-delete session context management backed by Redis
type Store struct {
	client *redis.Client
}

// NewStore connects to the Redis Context Store and verifies connectivity
func NewStore(addr string) (*Store, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis context store at %s: %w", addr, err)
	}

	return &Store{client: rdb}, nil
}

// ShardKey formats the Redis hash key for an environment shard: shard:{env_id}:{shard_id}
func ShardKey(envID string, shardID int) string {
	return fmt.Sprintf("%s%s:%d", constants.RedisPrefixShard, envID, shardID)
}

// SaveSession writes a client's context to Redis before connection registration completes
func (s *Store) SaveSession(ctx context.Context, sess *session.ClientSession) error {
	key := ShardKey(sess.EnvID, sess.ShardID)
	data, err := json.Marshal(sess)
	if err != nil {
		return fmt.Errorf("failed to marshal session context: %w", err)
	}

	return s.client.HSet(ctx, key, sess.ConnectionID, data).Err()
}

// DeleteSession purges a client connection from Redis when disconnected
func (s *Store) DeleteSession(ctx context.Context, envID string, shardID int, connID string) error {
	key := ShardKey(envID, shardID)
	return s.client.HDel(ctx, key, connID).Err()
}

// GetShardSessions retrieves all active client sessions for a shard during failover recovery
func (s *Store) GetShardSessions(ctx context.Context, envID string, shardID int) ([]*session.ClientSession, error) {
	key := ShardKey(envID, shardID)
	fields, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	sessions := make([]*session.ClientSession, 0, len(fields))
	for _, val := range fields {
		var sess session.ClientSession
		if err := json.Unmarshal([]byte(val), &sess); err == nil {
			sessions = append(sessions, &sess)
		}
	}

	return sessions, nil
}

// Close closes the Redis client connection
func (s *Store) Close() error {
	return s.client.Close()
}
