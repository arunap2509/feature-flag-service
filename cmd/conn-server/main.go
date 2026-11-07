package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"feature-flag-service/internal/connserver/cluster"
	"feature-flag-service/internal/connserver/deliveries"
	"feature-flag-service/internal/connserver/registry"
	"feature-flag-service/internal/connserver/server"
	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/evaluator/ring"
)

type Config struct {
	Port          string
	SlotID        int
	ServerAddr    string
	EtcdEndpoints []string
	KafkaBroker   string
	ShardCount    int
}

func loadConfig() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = constants.DefaultConnServerPort
	}
	slotID := 1
	if sStr := os.Getenv("SLOT_ID"); sStr != "" {
		if s, err := strconv.Atoi(sStr); err == nil && s > 0 {
			slotID = s
		}
	}
	serverAddr := os.Getenv("SERVER_ADDR")
	if serverAddr == "" {
		serverAddr = fmt.Sprintf("http://localhost:%s", port)
	}
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = constants.DefaultKafkaBroker
	}
	etcdEp := os.Getenv("ETCD_ENDPOINTS")
	if etcdEp == "" {
		etcdEp = constants.DefaultEtcdEndpoints
	}
	shardCount := constants.DefaultShardCount
	if scStr := os.Getenv("SHARD_COUNT"); scStr != "" {
		if sc, err := strconv.Atoi(scStr); err == nil && sc > 0 {
			shardCount = sc
		}
	}

	return Config{
		Port:          port,
		SlotID:        slotID,
		ServerAddr:    serverAddr,
		EtcdEndpoints: strings.Split(etcdEp, ","),
		KafkaBroker:   broker,
		ShardCount:    shardCount,
	}
}

func main() {
	cfg := loadConfig()
	log.Printf("[%s] Starting Connection Server Slot %d on port %s (ShardCount: %d)...",
		constants.ServiceNameConnServer, cfg.SlotID, cfg.Port, cfg.ShardCount)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1. Connection Registry & Evaluator Hash Ring
	connRegistry := registry.NewRegistry()
	hashRing := ring.New(constants.DefaultVirtualNodes)

	// 2. Connect to etcd and claim slot lease
	coordinator, err := cluster.NewCoordinator(cfg.EtcdEndpoints, cfg.SlotID, cfg.ServerAddr, hashRing, constants.DefaultEtcdLeaseTTL)
	if err != nil {
		log.Fatalf("[%s] Failed to initialize etcd coordinator: %v", constants.ServiceNameConnServer, err)
	}
	if err := coordinator.Start(ctx); err != nil {
		log.Fatalf("[%s] Failed to start cluster coordinator: %v", constants.ServiceNameConnServer, err)
	}
	defer coordinator.Stop()

	// 3. Start Deliveries Kafka Consumer (Slot Mailbox)
	delivConsumer, err := deliveries.NewConsumer(cfg.KafkaBroker, cfg.SlotID, connRegistry)
	if err != nil {
		log.Fatalf("[%s] Failed to initialize deliveries consumer: %v", constants.ServiceNameConnServer, err)
	}
	defer delivConsumer.Close()
	go delivConsumer.Start(ctx)

	// 4. HTTP SSE Gateway Server
	srv := server.NewServer(cfg.SlotID, cfg.ShardCount, coordinator, connRegistry)
	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%s", cfg.Port),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] HTTP server error: %v", constants.ServiceNameConnServer, err)
		}
	}()

	log.Printf("[%s] Edge SSE Gateway ready on :%s (Slot %d)",
		constants.ServiceNameConnServer, cfg.Port, cfg.SlotID)

	<-ctx.Done()
	log.Printf("[%s] Initiating graceful shutdown...", constants.ServiceNameConnServer)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)

	log.Printf("[%s] Connection Server Slot %d cleanly stopped.", constants.ServiceNameConnServer, cfg.SlotID)
}
