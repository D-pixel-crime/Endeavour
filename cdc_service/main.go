package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/producer"
	sqs_config "github.com/D-pixel-crime/Endeavour/aws_sqs_shared/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func main() {
	envFile := os.Getenv("ENV_FILE")
	if envFile != "" {
		if err := godotenv.Load(envFile); err != nil {
			log.Printf("Warning: failed to load %s: %v", envFile, err)
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

	sqsClient, err := sqs_config.NewDefaultSQSClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create SQS client: %v", err)
	}

	sqsProducer := producer.NewProducer(sqsClient, cfg.TargetQueueURL)
	router := NewRouter(cfg)
	poller := NewPoller(dbPool, sqsProducer, router, cfg.PollInterval, cfg.BatchSize)
	server := NewServer(cfg.HTTPPort, sqsClient, router)

	server.Start()
	go poller.Start(ctx)

	log.Printf("CDC service [%s] running in %s mode (port :%s, interval: %v)",
		cfg.ServiceName, cfg.Mode, cfg.HTTPPort, cfg.PollInterval)

	<-ctx.Done()
	log.Printf("Shutting down CDC service [%s]...", cfg.ServiceName)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Shutdown complete")
}
