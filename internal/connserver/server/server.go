package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"feature-flag-service/internal/connserver/cluster"
	"feature-flag-service/internal/connserver/registry"
	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/evaluator/engine"
)

// Server handles client HTTP SSE streams and proxies session registration to Evaluators
type Server struct {
	slotID      int
	shardCount  int
	coordinator *cluster.Coordinator
	registry    *registry.Registry
	httpClient  *http.Client
}

// NewServer creates a new connection server HTTP handler
func NewServer(slotID, shardCount int, coord *cluster.Coordinator, reg *registry.Registry) *Server {
	return &Server{
		slotID:      slotID,
		shardCount:  shardCount,
		coordinator: coord,
		registry:    reg,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// Handler configures and returns the HTTP mux for the Connection Server
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Healthcheck & stats
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"service":            constants.ServiceNameConnServer,
			"status":             constants.StatusHealthy,
			"slot_id":            s.slotID,
			"active_connections": s.registry.Count(),
		})
	})

	// Client SSE stream endpoint
	mux.HandleFunc("GET /api/v1/stream", s.handleStream)

	// Wrap in CORS
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-User, Last-Event-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Disable response write timeout for persistent SSE connections
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	envID := r.URL.Query().Get("env_id")
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = "usr_client"
	}
	connID := r.URL.Query().Get("conn_id")
	if connID == "" {
		connID = fmt.Sprintf("conn_%d", time.Now().UnixNano()%10000000)
	}

	shardID := engine.ComputeShard(userID, s.shardCount)

	// 1. Resolve Evaluator that owns this shard
	evaluatorAddr, err := s.coordinator.GetEvaluatorForShard(envID, shardID)
	if err != nil {
		log.Printf("[%s] Shard routing error: %v", constants.ServiceNameConnServer, err)
		http.Error(w, "Cluster unavailable", http.StatusServiceUnavailable)
		return
	}

	// 2. Register session with Evaluator to get initial flag evaluation
	regPayload := map[string]any{
		"connection_id": connID,
		"user_id":       userID,
		"shard_id":      shardID,
		"env_id":        envID,
		"server_slot":   s.slotID,
	}
	reqBytes, _ := json.Marshal(regPayload)

	resp, err := s.httpClient.Post(
		fmt.Sprintf("%s/api/v1/sessions/register", evaluatorAddr),
		constants.ContentTypeJSON,
		bytes.NewReader(reqBytes),
	)
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Printf("[%s] Evaluator registration failed at %s: %v", constants.ServiceNameConnServer, evaluatorAddr, err)
		http.Error(w, "Evaluator registration failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	var regResult struct {
		Status             string          `json:"status"`
		ConnectionID       string          `json:"connection_id"`
		ShardID            int             `json:"shard_id"`
		EnvironmentVersion int64           `json:"environment_version"`
		Answers            map[string]bool `json:"answers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&regResult); err != nil {
		http.Error(w, "Invalid evaluator response", http.StatusInternalServerError)
		return
	}

	// 3. Register client connection locally for Kafka diff patch delivery
	patchChan := make(chan []byte, 32)
	clientConn := &registry.ClientConnection{
		ConnectionID: connID,
		UserID:       userID,
		EnvID:        envID,
		ShardID:      shardID,
		PatchChan:    patchChan,
	}
	s.registry.Register(clientConn)

	// Clean up on disconnect
	defer func() {
		s.registry.Deregister(connID)
		// Notify Evaluator in background
		go func() {
			deregPayload, _ := json.Marshal(map[string]any{
				"env_id":        envID,
				"shard_id":      shardID,
				"connection_id": connID,
			})
			deregCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(deregCtx, "POST",
				fmt.Sprintf("%s/api/v1/sessions/deregister", evaluatorAddr),
				bytes.NewReader(deregPayload))
			req.Header.Set(constants.HeaderContentType, constants.ContentTypeJSON)
			_, _ = s.httpClient.Do(req)
		}()
	}()

	// 4. Send initial SSE 'put' event
	initFrame, _ := json.Marshal(map[string]any{
		"type":                constants.SSEEventPut,
		"connection_id":       connID,
		"user_id":             userID,
		"shard_id":            shardID,
		"environment_version": regResult.EnvironmentVersion,
		"flags":               regResult.Answers,
	})
	if _, err := fmt.Fprintf(w, "event: %s\nid: %d\ndata: %s\n\n",
		constants.SSEEventPut, regResult.EnvironmentVersion, initFrame); err != nil {
		return
	}
	flusher.Flush()

	// 5. Keepalive ticker (15 seconds)
	ticker := time.NewTicker(time.Duration(constants.DefaultKeepaliveIntervalSec) * time.Second)
	defer ticker.Stop()

	// 6. Push diff patches received via Kafka deliveries topic
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case patchBytes := <-patchChan:
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", constants.SSEEventPatch, patchBytes); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
