package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	sqs_config "github.com/D-pixel-crime/Endeavour/aws_sqs_shared/config"
	db_worker_payment "github.com/D-pixel-crime/Endeavour/payment/db"
	"github.com/D-pixel-crime/Endeavour/payment/poller"
	"github.com/D-pixel-crime/Endeavour/payment/shared"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Failed to load .env file: %v", err)
	}

	var err error
	shared.DBPool, err = db_worker_payment.ConnectToDb()
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer shared.DBPool.Close()

	poll_interval_str := os.Getenv("PAYMENT_POLL_INTERVAL_SECONDS")
	if poll_interval_str == "" {
		log.Fatalf("PAYMENT_POLL_INTERVAL_SECONDS is not set")
	}

	poll_interval_sec, err := strconv.Atoi(poll_interval_str)
	if err != nil {
		log.Fatalf("Failed to convert interval to seconds: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	shared.IngressQueueURL = os.Getenv("PAYMENT_INGRESS_SQS_URL")
	if shared.IngressQueueURL == "" {
		log.Fatalf("PAYMENT_INGRESS_SQS_URL is not set")
	}

	shared.IngressSQSClient, err = sqs_config.NewDefaultSQSClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create Ingress SQS client: %v", err)
	}

	r := gin.Default()

	port := os.Getenv("PAYMENT_PORT")
	if port == "" {
		port = "3001"
	}

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Starting HTTP server on :%s", port)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	go poller.StartPoller(ctx, time.Duration(poll_interval_sec)*time.Second)

	<-ctx.Done()
	log.Print("Shutting down Payment Worker...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Shutdown complete")
}
