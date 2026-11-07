package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"

	"feature-flag-service/internal/constants"
	"feature-flag-service/internal/mgmt/handler"
	"feature-flag-service/internal/mgmt/repository"
	"feature-flag-service/internal/mgmt/service"
)

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

	loggingMux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		mux.ServeHTTP(w, r)
		log.Printf("[mgmt-api] %s %s from %s in %v", r.Method, r.URL.Path, r.RemoteAddr, time.Since(start))
	})

	serverAddr := fmt.Sprintf(":%s", port)
	log.Printf("[mgmt-api] Management API Server listening on %s", serverAddr)
	if err := http.ListenAndServe(serverAddr, loggingMux); err != nil {
		log.Fatalf("[mgmt-api] Server error: %v", err)
	}
}
