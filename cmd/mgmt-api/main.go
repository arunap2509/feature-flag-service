package main

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/handler"
	"feature-flag-service/internal/mgmt/repository"
	"feature-flag-service/internal/mgmt/service"
)

//go:embed web/index.html
var dashboardHTML []byte

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/featureflags?sslmode=disable"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = constants.DefaultPort
	}

	log.Printf("[mgmt-api] Connecting to PostgreSQL at %s...", dbURL)
	var db *sql.DB
	var err error

	// Retry connecting for up to 30 seconds
	for i := 0; i < 15; i++ {
		db, err = sql.Open("postgres", dbURL)
		if err == nil {
			err = db.Ping()
			if err == nil {
				break
			}
		}
		log.Printf("[mgmt-api] Waiting for database (attempt %d/15): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		log.Fatalf("[mgmt-api] Could not connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	log.Printf("[mgmt-api] Database connection established.")

	// Wire components
	repo := repository.New(db)
	svc := service.New(repo)
	h := handler.New(svc)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	// Dashboard UI
	serveDashboard := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(dashboardHTML)
	}
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" && r.URL.Path != "/dashboard" {
			http.NotFound(w, r)
			return
		}
		serveDashboard(w, r)
	})
	mux.HandleFunc("GET /dashboard", serveDashboard)

	loggingMux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-User, If-Match, Last-Event-ID")
		w.Header().Set("Access-Control-Expose-Headers", "ETag")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		start := time.Now()
		mux.ServeHTTP(w, r)
		log.Printf("[mgmt-api] %s %s from %s in %v", r.Method, r.URL.Path, r.RemoteAddr, time.Since(start))
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      loggingMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("[mgmt-api] Management API Server listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[mgmt-api] Server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("[mgmt-api] Initiating graceful shutdown...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[mgmt-api] Server forced to shutdown: %v", err)
	} else {
		log.Printf("[mgmt-api] Server cleanly stopped.")
	}
}
