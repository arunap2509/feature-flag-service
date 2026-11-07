package session

import (
	"sync"

	"feature-flag-service/internal/evaluator/engine"
)

// ClientSession represents an active client stream connected via a Connection Server
type ClientSession struct {
	ConnectionID string            `json:"connection_id"`
	UserID       string            `json:"user_id"`
	ShardID      int               `json:"shard_id"`
	EnvID        string            `json:"env_id"`
	ServerSlot   int               `json:"server_slot"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	LastAnswers  map[string]bool   `json:"last_answers"`
	PatchChan    chan DiffPatch    `json:"-"`
}

// DiffPatch represents a delta update pushed to an affected client connection
type DiffPatch struct {
	ConnectionID string          `json:"connection_id"`
	ServerSlot   int             `json:"server_slot"`
	EnvID        string          `json:"env_id"`
	EnvVersion   int64           `json:"env_version"`
	Changes      map[string]bool `json:"changes"`
}

// Registry tracks client sessions in memory partitioned by shard
type Registry struct {
	mu       sync.RWMutex
	// env_id -> shard_id -> conn_id -> ClientSession
	sessions map[string]map[int]map[string]*ClientSession
}

// NewRegistry initializes an empty session registry
func NewRegistry() *Registry {
	return &Registry{
		sessions: make(map[string]map[int]map[string]*ClientSession),
	}
}

// Register adds or updates a client session in the given shard
func (r *Registry) Register(sess *ClientSession) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.sessions[sess.EnvID]; !ok {
		r.sessions[sess.EnvID] = make(map[int]map[string]*ClientSession)
	}
	if _, ok := r.sessions[sess.EnvID][sess.ShardID]; !ok {
		r.sessions[sess.EnvID][sess.ShardID] = make(map[string]*ClientSession)
	}
	r.sessions[sess.EnvID][sess.ShardID][sess.ConnectionID] = sess
}

// Deregister removes a client session when the stream disconnects
func (r *Registry) Deregister(envID string, shardID int, connID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if shards, ok := r.sessions[envID]; ok {
		if conns, ok := shards[shardID]; ok {
			delete(conns, connID)
		}
	}
}

// SessionCount returns the total number of connected client sessions tracked by this evaluator
func (r *Registry) SessionCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	total := 0
	for _, shards := range r.sessions {
		for _, conns := range shards {
			total += len(conns)
		}
	}
	return total
}

// ReevaluateShard computes diff patches for all active sessions in a shard when a flag changes
func (r *Registry) ReevaluateShard(
	envID string,
	shardID int,
	flagKey string,
	flags map[string]*engine.ParsedFlag,
	segments map[string]*engine.ParsedSegment,
	envVersion int64,
) []DiffPatch {
	r.mu.Lock()
	defer r.mu.Unlock()

	patches := make([]DiffPatch, 0)
	targetFlag, flagExists := flags[flagKey]
	if !flagExists {
		return patches
	}

	shards, ok := r.sessions[envID]
	if !ok {
		return patches
	}
	conns, ok := shards[shardID]
	if !ok {
		return patches
	}

	for _, sess := range conns {
		userCtx := engine.UserContext{
			UserID:     sess.UserID,
			Attributes: sess.Attributes,
		}

		newVal := engine.EvaluateFlag(targetFlag, segments, userCtx)
		oldVal, hadOldVal := sess.LastAnswers[flagKey]

		// If value flipped or was not previously evaluated, produce a diff patch!
		if !hadOldVal || oldVal != newVal {
			if sess.LastAnswers == nil {
				sess.LastAnswers = make(map[string]bool)
			}
			sess.LastAnswers[flagKey] = newVal
			patch := DiffPatch{
				ConnectionID: sess.ConnectionID,
				ServerSlot:   sess.ServerSlot,
				EnvID:        envID,
				EnvVersion:   envVersion,
				Changes: map[string]bool{
					flagKey: newVal,
				},
			}
			patches = append(patches, patch)
			if sess.PatchChan != nil {
				select {
				case sess.PatchChan <- patch:
				default:
				}
			}
		}
	}

	return patches
}
