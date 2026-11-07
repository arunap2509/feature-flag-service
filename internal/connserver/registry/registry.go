package registry

import (
	"sync"
)

// ClientConnection represents an active client SSE stream connected to this Connection Server
type ClientConnection struct {
	ConnectionID string
	UserID       string
	EnvID        string
	ShardID      int
	PatchChan    chan []byte
}

// Registry tracks all active client connections on this Connection Server
type Registry struct {
	mu    sync.RWMutex
	conns map[string]*ClientConnection
}

// NewRegistry creates a new in-memory connection registry
func NewRegistry() *Registry {
	return &Registry{
		conns: make(map[string]*ClientConnection),
	}
}

// Register stores a client connection
func (r *Registry) Register(conn *ClientConnection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.conns[conn.ConnectionID] = conn
}

// Deregister removes a client connection
func (r *Registry) Deregister(connID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.conns, connID)
}

// Get retrieves a client connection by its connection ID
func (r *Registry) Get(connID string) (*ClientConnection, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conn, exists := r.conns[connID]
	return conn, exists
}

// Count returns the number of active client connections on this server
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.conns)
}
