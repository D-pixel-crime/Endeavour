package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	db_orchestrator "github.com/D-pixel-crime/Endeavor/orchestrator/db"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func startWorker(ctx context.Context, interval time.Duration, cdc_orchestrator_db_pool *pgxpool.Pool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Print("CDC Orchestrator Worker is Stopping!")
			return

		case <-ticker.C:
			go ProcessBatch(cdc_orchestrator_db_pool)
		}
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Failed to load .env file: %v", err)
	}

	cdc_orchestrator_db_pool, err := db_orchestrator.ConnectToDb()
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer cdc_orchestrator_db_pool.Close()

	interval_str := os.Getenv("CDC_ORCHESTRATOR_INTERVAL_SECONDS")
	if interval_str == "" {
		log.Fatalf("CDC_ORCHESTRATOR_INTERVAL_SECONDS is not set")
	}

	interval_sec, err := strconv.Atoi(interval_str)
	if err != nil {
		log.Fatalf("Failed to convert interval to seconds: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go startWorker(ctx, time.Duration(interval_sec)*time.Second, cdc_orchestrator_db_pool)

	<-ctx.Done()
	log.Print("CDC Orchestrator Worker Stopped!")
}
