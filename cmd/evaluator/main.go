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
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/twmb/franz-go/pkg/kgo"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/evaluator/cache"
	"feature-flag-service/internal/evaluator/cluster"
	"feature-flag-service/internal/evaluator/contextstore"
	"feature-flag-service/internal/evaluator/engine"
	"feature-flag-service/internal/evaluator/ring"
	"feature-flag-service/internal/evaluator/session"
	"feature-flag-service/internal/mgmt/model"
)

type Config struct {
	Port                  string
	NodeID                string
	NodeAddr              string
	KafkaBroker           string
	KafkaDeliveriesBroker string
	EtcdEndpoints         []string
	DatabaseURL           string
	RedisAddr             string
	ShardCount            int
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
	nodeAddr := os.Getenv("NODE_ADDR")
	if nodeAddr == "" {
		nodeAddr = fmt.Sprintf("http://localhost:%s", port)
	}
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = constants.DefaultKafkaControlBroker
	}
	delivBroker := os.Getenv("KAFKA_DELIVERIES_BROKER")
	if delivBroker == "" {
		delivBroker = constants.DefaultKafkaDeliveriesBroker
	}
	etcdEp := os.Getenv("ETCD_ENDPOINTS")
	if etcdEp == "" {
		etcdEp = constants.DefaultEtcdEndpoints
	}
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@postgres:5432/featureflags?sslmode=disable"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = constants.DefaultRedisAddr
	}
	shardCount := constants.DefaultShardCount
	if scStr := os.Getenv("SHARD_COUNT"); scStr != "" {
		if sc, err := strconv.Atoi(scStr); err == nil && sc > 0 {
			shardCount = sc
		}
	}

	return Config{
		Port:                  port,
		NodeID:                nodeID,
		NodeAddr:              nodeAddr,
		KafkaBroker:           broker,
		KafkaDeliveriesBroker: delivBroker,
		EtcdEndpoints:         strings.Split(etcdEp, ","),
		DatabaseURL:           dbURL,
		RedisAddr:             redisAddr,
		ShardCount:            shardCount,
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

	// 3. Connect to Redis Context Store
	contextStore, err := contextstore.NewStore(cfg.RedisAddr)
	if err != nil {
		log.Printf("[%s] Warning: Redis context store connection failed (%s): %v",
			constants.ServiceNameEvaluator, cfg.RedisAddr, err)
	} else {
		defer contextStore.Close()
		log.Printf("[%s] Connected to Redis Context Store at %s",
			constants.ServiceNameEvaluator, cfg.RedisAddr)
	}

	// 4. Connect to etcd and join cluster ring
	coordinator, err := cluster.NewCoordinator(cfg.EtcdEndpoints, cfg.NodeID, cfg.NodeAddr, hashRing, constants.DefaultEtcdLeaseTTL)
	if err != nil {
		log.Fatalf("[%s] Failed to initialize etcd coordinator: %v", constants.ServiceNameEvaluator, err)
	}

	// 5. Failover Ring Watcher: Detect adopted shards and recover session contexts from Redis
	var ownedMu sync.Mutex
	lastOwnedShards := make(map[string]map[int]struct{})

	coordinator.SetOnRingChange(func(activeNodes []string) {
		if contextStore == nil {
			return
		}

		ownedMu.Lock()
		defer ownedMu.Unlock()

		for _, envID := range rulesStore.KnownEnvironments() {
			if _, ok := lastOwnedShards[envID]; !ok {
				lastOwnedShards[envID] = make(map[int]struct{})
			}

			newOwned := hashRing.GetOwnedShards(cfg.NodeID, envID, cfg.ShardCount)
			newOwnedMap := make(map[int]struct{})
			for _, s := range newOwned {
				newOwnedMap[s] = struct{}{}
			}

			// Check for shards that this node just adopted
			for _, shardID := range newOwned {
				if _, wasOwned := lastOwnedShards[envID][shardID]; !wasOwned {
					recoverCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					recovered, err := contextStore.GetShardSessions(recoverCtx, envID, shardID)
					cancel()
					if err != nil {
						log.Printf("[%s] Error recovering shard %d sessions from Redis: %v",
							constants.ServiceNameEvaluator, shardID, err)
						continue
					}

					if len(recovered) > 0 {
						flags, segments, _ := rulesStore.GetEnvironmentData(envID)
						for _, sess := range recovered {
							sess.LastAnswers = engine.EvaluateAll(flags, segments, engine.UserContext{
								UserID:     sess.UserID,
								Attributes: sess.Attributes,
							})
							sessionRegistry.Register(sess)
						}
						log.Printf("[%s] Zero-Reconnect Failover: Adopted Shard %d for env '%s' - recovered %d active sessions from Redis",
							constants.ServiceNameEvaluator, shardID, envID, len(recovered))
					}
				}
			}

			lastOwnedShards[envID] = newOwnedMap
		}
	})

	if err := coordinator.Start(ctx); err != nil {
		log.Fatalf("[%s] Failed to start cluster coordinator: %v", constants.ServiceNameEvaluator, err)
	}
	defer coordinator.Stop()

	// 6. Initialize Kafka Deliveries Producer (Slot Mailboxes on Data Plane Redpanda)
	deliveriesProducer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaDeliveriesBroker),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		log.Fatalf("[%s] Failed to initialize Kafka deliveries producer: %v", constants.ServiceNameEvaluator, err)
	}
	defer deliveriesProducer.Close()

	// 5. Initialize Kafka Consumer for flag-changes topic
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

						// Group patches by Connection Server slot ID and publish to Kafka deliveries topic
						slotBatches := make(map[int][]session.DiffPatch)
						for _, p := range patches {
							if p.ServerSlot > 0 {
								slotBatches[p.ServerSlot] = append(slotBatches[p.ServerSlot], p)
							}
						}

						for slotID, pList := range slotBatches {
							batchData, err := json.Marshal(map[string]any{
								"slot_id":     slotID,
								"env_id":      payload.EnvironmentID,
								"env_version": envVer,
								"patches":     pList,
							})
							if err != nil {
								log.Printf("[%s] Error marshaling deliveries batch: %v", constants.ServiceNameEvaluator, err)
								continue
							}

							// Map slotID (e.g. 1..8) to 0-indexed partition (0..7)
							partition := int32((slotID - 1) % constants.DefaultDeliveriesPartitions)
							rec := &kgo.Record{
								Topic:     constants.TopicDeliveries,
								Partition: partition,
								Value:     batchData,
							}
							deliveriesProducer.Produce(ctx, rec, func(_ *kgo.Record, err error) {
								if err != nil {
									log.Printf("[%s] Deliveries produce error for slot %d (partition %d): %v",
										constants.ServiceNameEvaluator, slotID, partition, err)
								}
							})
						}
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

		// 1. Initial evaluation for newly connected session
		flags, segments, envVer := rulesStore.GetEnvironmentData(sess.EnvID)
		sess.LastAnswers = engine.EvaluateAll(flags, segments, engine.UserContext{
			UserID:     sess.UserID,
			Attributes: sess.Attributes,
		})

		// 2. Persist to Redis Context Store before responding (Zero-reconnect contract)
		if contextStore != nil {
			if err := contextStore.SaveSession(r.Context(), &sess); err != nil {
				log.Printf("[%s] Warning: failed to persist session %s to Redis: %v",
					constants.ServiceNameEvaluator, sess.ConnectionID, err)
			}
		}

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

	// Session deregistration (called when Connection Server detects client disconnect)
	mux.HandleFunc("POST /api/v1/sessions/deregister", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			EnvID        string `json:"env_id"`
			ShardID      int    `json:"shard_id"`
			ConnectionID string `json:"connection_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, constants.ErrMsgInvalidRequestBody, http.StatusBadRequest)
			return
		}

		sessionRegistry.Deregister(req.EnvID, req.ShardID, req.ConnectionID)
		if contextStore != nil {
			_ = contextStore.DeleteSession(r.Context(), req.EnvID, req.ShardID, req.ConnectionID)
		}

		w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "deregistered",
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

		if contextStore != nil {
			_ = contextStore.SaveSession(r.Context(), sess)
		}
		sessionRegistry.Register(sess)
		defer func() {
			sessionRegistry.Deregister(envID, shardID, connID)
			if contextStore != nil {
				_ = contextStore.DeleteSession(context.Background(), envID, shardID, connID)
			}
		}()

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
