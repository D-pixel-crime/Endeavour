package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	sqs_config "github.com/D-pixel-crime/Endeavour/aws_sqs_shared/config"
	"github.com/D-pixel-crime/Endeavour/payment/poller"
	"github.com/D-pixel-crime/Endeavour/payment/shared"
	"github.com/D-pixel-crime/Endeavour/utility"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Failed to load .env file: %v", err)
	}

	var err error
	shared.DBPool, err = utility.ConnectToDBFromEnv(context.Background(), "PAYMENT_DB_URL")
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer shared.DBPool.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	poll_interval_sec := 5
	if poll_interval_sec_str := os.Getenv("PAYMENT_POLL_INTERVAL_SECONDS"); poll_interval_sec_str != "" {
		if sec, err := strconv.Atoi(poll_interval_sec_str); err == nil && sec > 0 {
			poll_interval_sec = sec
		}
	}

	visTimeoutStr := os.Getenv("PAYMENT_INGRESS_VISIBILITY_TIMEOUT_SECONDS")
	if visTimeoutStr == "" {
		visTimeoutStr = os.Getenv("PAYMENT_VISIBILITY_TIMEOUT_SECONDS")
	}
	if visTimeoutStr == "" {
		log.Fatalf("PAYMENT_INGRESS_VISIBILITY_TIMEOUT_SECONDS is not set")
	}
	visTimeoutSec, err := strconv.Atoi(visTimeoutStr)
	if err != nil || visTimeoutSec <= 0 {
		log.Fatalf("Invalid PAYMENT_INGRESS_VISIBILITY_TIMEOUT_SECONDS: %v", err)
	}
	shared.IngressVisibilityTimeout = int32(visTimeoutSec)

	shared.IngressQueueURL = os.Getenv("PAYMENT_INGRESS_SQS_URL")
	if shared.IngressQueueURL == "" {
		log.Fatalf("PAYMENT_INGRESS_SQS_URL is not set")
	}
	if !strings.Contains(shared.IngressQueueURL, ".fifo") {
		log.Fatalf("PAYMENT_INGRESS_SQS_URL '%s' must be a FIFO queue (must contain .fifo)", shared.IngressQueueURL)
	}

	shared.IngressSQSClient, err = sqs_config.NewDefaultSQSClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create Ingress SQS client: %v", err)
	}

	maxConcurrentPulls := 5
	if maxConcurrentStr := os.Getenv("PAYMENT_MAX_CONCURRENT_PULLS"); maxConcurrentStr != "" {
		if n, err := strconv.Atoi(maxConcurrentStr); err == nil && n > 0 {
			maxConcurrentPulls = n
		}
	}

	var pollerWg sync.WaitGroup
	pollerWg.Go(func() {
		poller.StartPoller(ctx, time.Duration(poll_interval_sec)*time.Second, maxConcurrentPulls)
	})

	<-ctx.Done()
	log.Println("Shutting down Payment Worker...")

	pollerWg.Wait()
	log.Println("Payment ingress poller stopped cleanly")
	log.Println("Shutdown complete")
}
