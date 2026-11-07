package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/lb"
)

type Config struct {
	Port          string
	EtcdEndpoints []string
}

func loadConfig() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = constants.DefaultLoadBalancerPort
	}

	etcdEp := os.Getenv("ETCD_ENDPOINTS")
	if etcdEp == "" {
		etcdEp = constants.DefaultEtcdEndpoints
	}

	return Config{
		Port:          port,
		EtcdEndpoints: strings.Split(etcdEp, ","),
	}
}

func main() {
	cfg := loadConfig()

	log.Printf("=================================================================")
	log.Printf("🚀 Starting [%s] on port :%s", constants.ServiceNameLoadBalancer, cfg.Port)
	log.Printf("   etcd endpoints: %v", cfg.EtcdEndpoints)
	log.Printf("=================================================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize dynamic Upstream Pool connected to etcd
	pool, err := lb.NewUpstreamPool(cfg.EtcdEndpoints)
	if err != nil {
		log.Fatalf("[%s] Failed to initialize etcd upstream pool: %v", constants.ServiceNameLoadBalancer, err)
	}
	defer pool.Stop()

	if err := pool.Start(ctx); err != nil {
		log.Fatalf("[%s] Failed to start upstream pool discovery: %v", constants.ServiceNameLoadBalancer, err)
	}

	// 2. Initialize Reverse Proxy with SSE immediate flushing (-1)
	proxyServer := lb.NewProxyServer(pool)

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: proxyServer,
	}

	// 3. Start HTTP Server in background
	go func() {
		log.Printf("[%s] HTTP server listening on :%s", constants.ServiceNameLoadBalancer, cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] HTTP server fatal error: %v", constants.ServiceNameLoadBalancer, err)
		}
	}()

	// 4. Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Printf("[%s] Shutdown signal received. Draining connections...", constants.ServiceNameLoadBalancer)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[%s] Error during HTTP server shutdown: %v", constants.ServiceNameLoadBalancer, err)
	}

	log.Printf("[%s] Service stopped gracefully.", constants.ServiceNameLoadBalancer)
}
