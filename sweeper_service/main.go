package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	envFile := os.Getenv("ENV_FILE")
	if envFile != "" {
		if err := godotenv.Load(envFile); err != nil {
			log.Printf("Notice: failed to load %s: %v", envFile, err)
		}
	} else {
		_ = godotenv.Load()
	}

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	dbPool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer dbPool.Close()

	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()
	if err := dbPool.Ping(pingCtx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	sweeper := NewSweeper(dbPool, cfg.PollInterval, cfg.BatchSize, cfg.TaskTimeoutSec)

	log.Printf("Sweeper service running (interval: %v, batch size: %d)",
		cfg.PollInterval, cfg.BatchSize)

	sweeper.Start(ctx)

	log.Println("Shutdown complete")
}
