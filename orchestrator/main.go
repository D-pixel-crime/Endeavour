package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	sqs_config "github.com/D-pixel-crime/Endeavour/aws_sqs_shared/config"
	api_routes "github.com/D-pixel-crime/Endeavour/orchestrator/api_routes"
	db "github.com/D-pixel-crime/Endeavour/orchestrator/db"
	"github.com/D-pixel-crime/Endeavour/orchestrator/poller"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("Notice: No .env file loaded or error loading .env: %v", err)
	}

	var err error
	shared_vars.Orchestrator_DB_Pool, err = db.ConnectToDb()
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer shared_vars.Orchestrator_DB_Pool.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Initialize SQS consumer client and start booking egress poller if URL is configured
	shared_vars.BookingEgressQueueURL = os.Getenv("BOOKING_EGRESS_SQS_URL")
	if shared_vars.BookingEgressQueueURL != "" {
		shared_vars.BookingEgressSQSClient, err = sqs_config.NewDefaultSQSClient(ctx)
		if err != nil {
			log.Fatalf("Failed to create Booking Egress SQS client: %v", err)
		}

		pollInterval := 5 * time.Second
		if pollIntervalStr := os.Getenv("ORCHESTRATOR_POLL_INTERVAL_SECONDS"); pollIntervalStr != "" {
			if sec, err := strconv.Atoi(pollIntervalStr); err == nil && sec > 0 {
				pollInterval = time.Duration(sec) * time.Second
			}
		}

		go poller.StartBookingEgressPoller(ctx, pollInterval)
	} else {
		log.Println("WARNING: BOOKING_EGRESS_SQS_URL is not set. Booking egress poller will not run.")
	}

	r := gin.Default()

	getRouter := r.Group("/get")
	api_routes.GetRoutes(getRouter)

	postRouter := r.Group("/post")
	api_routes.PostRoutes(postRouter)

	port := os.Getenv("ORCHESTRATOR_PORT")
	if port == "" {
		port = "8080"
	}
	addr := port
	if !strings.HasPrefix(addr, ":") {
		addr = ":" + addr
	}

	server := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Server is starting on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down Orchestrator...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Shutdown complete")
}
