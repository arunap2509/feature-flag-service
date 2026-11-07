package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/twmb/franz-go/pkg/kgo"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/evaluator/cache"
	"feature-flag-service/internal/evaluator/cluster"
	"feature-flag-service/internal/evaluator/engine"
	"feature-flag-service/internal/evaluator/ring"
	"feature-flag-service/internal/evaluator/session"
	"feature-flag-service/internal/mgmt/model"
)

type Config struct {
	Port          string
	NodeID        string
	KafkaBroker   string
	EtcdEndpoints []string
	DatabaseURL   string
	ShardCount    int
}

func loadConfig() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = constants.DefaultEvaluatorPort
	}
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = fmt.Sprintf("evaluator-%s", port)
	}
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = constants.DefaultKafkaBroker
	}
	etcdEp := os.Getenv("ETCD_ENDPOINTS")
	if etcdEp == "" {
		etcdEp = constants.DefaultEtcdEndpoints
	}
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@postgres:5432/featureflags?sslmode=disable"
	}
	shardCount := constants.DefaultShardCount
	if scStr := os.Getenv("SHARD_COUNT"); scStr != "" {
		if sc, err := strconv.Atoi(scStr); err == nil && sc > 0 {
			shardCount = sc
		}
	}

	return Config{
		Port:          port,
		NodeID:        nodeID,
		KafkaBroker:   broker,
		EtcdEndpoints: strings.Split(etcdEp, ","),
		DatabaseURL:   dbURL,
		ShardCount:    shardCount,
	}
}

func main() {
	cfg := loadConfig()
	log.Printf("[%s] Starting Evaluator Node '%s' on port %s (ShardCount: %d)...",
		constants.ServiceNameEvaluator, cfg.NodeID, cfg.Port, cfg.ShardCount)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1. In-Memory Ring, Store, and Session Registry
	hashRing := ring.New(constants.DefaultVirtualNodes)
	rulesStore := cache.NewStore()
	sessionRegistry := session.NewRegistry()

	// 2. Connect to PostgreSQL and hydrate initial flag and segment state
	var db *sql.DB
	var err error
	for i := 0; i < 10; i++ {
		db, err = sql.Open("postgres", cfg.DatabaseURL)
		if err == nil && db.Ping() == nil {
			break
		}
		log.Printf("[%s] Waiting for PostgreSQL (attempt %d/10)...", constants.ServiceNameEvaluator, i+1)
		time.Sleep(2 * time.Second)
	}
	if err == nil && db != nil {
		if err := rulesStore.HydrateFromDB(db); err != nil {
			log.Printf("[%s] Warning: database hydration error: %v", constants.ServiceNameEvaluator, err)
		}
		_ = db.Close()
	}

	// 3. Connect to etcd and join cluster ring
	coordinator, err := cluster.NewCoordinator(cfg.EtcdEndpoints, cfg.NodeID, hashRing, constants.DefaultEtcdLeaseTTL)
	if err != nil {
		log.Fatalf("[%s] Failed to initialize etcd coordinator: %v", constants.ServiceNameEvaluator, err)
	}
	if err := coordinator.Start(ctx); err != nil {
		log.Fatalf("[%s] Failed to start cluster coordinator: %v", constants.ServiceNameEvaluator, err)
	}
	defer coordinator.Stop()

	// 4. Initialize Kafka Consumer for flag-changes topic
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBroker),
		kgo.ConsumeTopics(constants.TopicFlagChanges),
		kgo.ConsumerGroup(fmt.Sprintf("%s-%s", constants.KafkaGroupEvaluator, cfg.NodeID)), // broadcast: every evaluator gets all messages
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		log.Fatalf("[%s] Failed to initialize Kafka consumer: %v", constants.ServiceNameEvaluator, err)
	}
	defer kafkaClient.Close()

	// 5. Start Kafka Ingestion Goroutine
	go func() {
		log.Printf("[%s] Subscribed to topic '%s' for live flag updates...", constants.ServiceNameEvaluator, constants.TopicFlagChanges)
		for {
			if ctx.Err() != nil {
				return
			}
			fetches := kafkaClient.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					log.Printf("[%s] Kafka poll error: %v", constants.ServiceNameEvaluator, err.Err)
				}
				continue
			}

			fetches.EachRecord(func(record *kgo.Record) {
				var payload model.OutboxEventPayload
				if err := json.Unmarshal(record.Value, &payload); err != nil {
					log.Printf("[%s] Error unmarshaling outbox payload: %v", constants.ServiceNameEvaluator, err)
					return
				}

				// Apply to in-memory state
				rulesStore.ApplyEvent(payload)

				// Determine shards to re-evaluate: ring-owned shards + locally connected sessions
				ownedShards := hashRing.GetOwnedShards(cfg.NodeID, payload.EnvironmentID, cfg.ShardCount)
				shardsToEval := make(map[int]struct{})
				for _, s := range ownedShards {
					shardsToEval[s] = struct{}{}
				}
				for _, s := range sessionRegistry.ActiveShards(payload.EnvironmentID) {
					shardsToEval[s] = struct{}{}
				}

				if len(shardsToEval) == 0 {
					return
				}

				flags, segments, envVer := rulesStore.GetEnvironmentData(payload.EnvironmentID)
				for shardID := range shardsToEval {
					patches := sessionRegistry.ReevaluateShard(
						payload.EnvironmentID,
						shardID,
						payload.FlagKey,
						flags,
						segments,
						envVer,
					)
					if len(patches) > 0 {
						target := payload.FlagKey
						if target == "" {
							target = fmt.Sprintf("segment:%s", payload.SegmentKey)
						}
						log.Printf("[%s] Shard %d: generated %d diff patches for target '%s' (env_version: %d)",
							constants.ServiceNameEvaluator, shardID, len(patches), target, envVer)
					}
				}
			})
		}
	}()

	// 6. HTTP API Server
	mux := http.NewServeMux()

	// Healthcheck & cluster state
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"service":        constants.ServiceNameEvaluator,
			"status":         constants.StatusHealthy,
			"node_id":        cfg.NodeID,
			"cluster_nodes":  hashRing.Members(),
			"etcd_revision":  coordinator.Revision(),
			"total_sessions": sessionRegistry.SessionCount(),
		})
	})

	// Shards inspection
	mux.HandleFunc("GET /api/v1/shards/{env_id}", func(w http.ResponseWriter, r *http.Request) {
		envID := r.PathValue("env_id")
		owned := hashRing.GetOwnedShards(cfg.NodeID, envID, cfg.ShardCount)
		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":      cfg.NodeID,
			"env_id":       envID,
			"shard_count":  cfg.ShardCount,
			"owned_shards": owned,
		})
	})

	// Direct in-memory flag evaluation endpoint (< 1ms evaluation check)
	mux.HandleFunc("POST /api/v1/evaluate/{env_id}", func(w http.ResponseWriter, r *http.Request) {
		envID := r.PathValue("env_id")
		var userCtx engine.UserContext
		if err := json.NewDecoder(r.Body).Decode(&userCtx); err != nil {
			http.Error(w, constants.ErrMsgInvalidRequestBody, http.StatusBadRequest)
			return
		}

		flags, segments, envVer := rulesStore.GetEnvironmentData(envID)
		results := engine.EvaluateAll(flags, segments, userCtx)

		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"environment_id":      envID,
			"environment_version": envVer,
			"user_id":             userCtx.UserID,
			"shard_id":            engine.ComputeShard(userCtx.UserID, cfg.ShardCount),
			"flags":               results,
		})
	})

	// Session registration (used by Connection Servers or simulation clients)
	mux.HandleFunc("POST /api/v1/sessions/register", func(w http.ResponseWriter, r *http.Request) {
		var sess session.ClientSession
		if err := json.NewDecoder(r.Body).Decode(&sess); err != nil {
			http.Error(w, constants.ErrMsgInvalidRequestBody, http.StatusBadRequest)
			return
		}
		if sess.ShardID == 0 && cfg.ShardCount > 1 {
			sess.ShardID = engine.ComputeShard(sess.UserID, cfg.ShardCount)
		}

		// Initial evaluation for newly connected session
		flags, segments, envVer := rulesStore.GetEnvironmentData(sess.EnvID)
		sess.LastAnswers = engine.EvaluateAll(flags, segments, engine.UserContext{
			UserID:     sess.UserID,
			Attributes: sess.Attributes,
		})

		sessionRegistry.Register(&sess)

		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":              "registered",
			"connection_id":       sess.ConnectionID,
			"shard_id":            sess.ShardID,
			"environment_version": envVer,
			"answers":             sess.LastAnswers,
		})
	})

	// Live SSE Stream endpoint (used by Web UI simulator or Connection Servers)
	mux.HandleFunc("GET /api/v1/sessions/stream", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}

		// Disable response write timeout for long-lived SSE streaming
		rc := http.NewResponseController(w)
		_ = rc.SetWriteDeadline(time.Time{})

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		envID := r.URL.Query().Get("env_id")
		userID := r.URL.Query().Get("user_id")
		if userID == "" {
			userID = "usr_alice"
		}
		connID := r.URL.Query().Get("conn_id")
		if connID == "" {
			connID = fmt.Sprintf("conn_%d", time.Now().UnixNano()%1000000)
		}

		shardID := engine.ComputeShard(userID, cfg.ShardCount)
		flags, segments, envVer := rulesStore.GetEnvironmentData(envID)
		initialAnswers := engine.EvaluateAll(flags, segments, engine.UserContext{
			UserID: userID,
		})

		patchChan := make(chan session.DiffPatch, 32)
		sess := &session.ClientSession{
			ConnectionID: connID,
			UserID:       userID,
			ShardID:      shardID,
			EnvID:        envID,
			ServerSlot:   1,
			LastAnswers:  initialAnswers,
			PatchChan:    patchChan,
		}

		sessionRegistry.Register(sess)
		defer sessionRegistry.Deregister(envID, shardID, connID)

		// Push initial PUT frame
		initPayload, _ := json.Marshal(map[string]any{
			"type":                "put",
			"connection_id":       connID,
			"user_id":             userID,
			"shard_id":            shardID,
			"environment_version": envVer,
			"flags":               initialAnswers,
		})
		_, _ = fmt.Fprintf(w, "event: put\ndata: %s\n\n", initPayload)
		flusher.Flush()

		// Periodic keep-alive ping ticker
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		// Stream patches until client disconnects
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
			case patch := <-patchChan:
				patchPayload, _ := json.Marshal(map[string]any{
					"type":                "patch",
					"connection_id":       patch.ConnectionID,
					"environment_version": patch.EnvVersion,
					"changes":             patch.Changes,
				})
				if _, err := fmt.Fprintf(w, "event: patch\ndata: %s\n\n", patchPayload); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	})

	corsMux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-User")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		mux.ServeHTTP(w, r)
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%s", cfg.Port),
		Handler:           corsMux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] HTTP server error: %v", constants.ServiceNameEvaluator, err)
		}
	}()

	log.Printf("[%s] Evaluator HTTP API ready on :%s", constants.ServiceNameEvaluator, cfg.Port)

	// Graceful shutdown
	<-ctx.Done()
	log.Printf("[%s] Initiating graceful shutdown...", constants.ServiceNameEvaluator)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)

	log.Printf("[%s] Evaluator Node cleanly stopped.", constants.ServiceNameEvaluator)
}
