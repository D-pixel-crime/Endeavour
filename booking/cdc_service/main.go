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
	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/monitoring"
	db_booking "github.com/D-pixel-crime/Endeavour/booking/db"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

var (
	sqsClient           *sqs.Client
	cdc_booking_db_pool *pgxpool.Pool
	egressQueueURL      string
)

func startWorker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Print("CDC Booking Worker is Stopping!")
			return

		case <-ticker.C:
			go ProcessEgressBatch()
		}
	}
}

func healthHandler(monitor *monitoring.QueueMonitor) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		if err := monitor.HealthCheck(ctx, egressQueueURL); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status": "healthy",
		})
	}
}

func statsHandler(monitor *monitoring.QueueMonitor) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		stats, err := monitor.GetQueueStats(ctx, egressQueueURL)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, stats)
	}
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Failed to load .env file: %v", err)
	}

	var err error
	cdc_booking_db_pool, err = db_booking.ConnectToDb()
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer cdc_booking_db_pool.Close()

	interval_str := os.Getenv("CDC_BOOKING_INTERVAL_SECONDS")
	if interval_str == "" {
		interval_str = os.Getenv("BOOKING_CDC_INTERVAL_SECONDS")
	}
	if interval_str == "" {
		log.Fatalf("CDC_BOOKING_INTERVAL_SECONDS is not set")
	}

	interval_sec, err := strconv.Atoi(interval_str)
	if err != nil {
		log.Fatalf("Failed to convert interval to seconds: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	egressQueueURL = os.Getenv("BOOKING_EGRESS_SQS_URL")
	if egressQueueURL == "" {
		log.Fatalf("BOOKING_EGRESS_SQS_URL is not set")
	}

	sqsClient, err = sqs_config.NewDefaultSQSClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create SQS client: %v", err)
	}

	monitor := monitoring.NewQueueMonitor(sqsClient)

	r := gin.Default()

	r.GET("/health", healthHandler(monitor))
	r.GET("/stats", statsHandler(monitor))

	port := os.Getenv("CDC_BOOKING_PORT")
	if port == "" {
		port = os.Getenv("BOOKING_CDC_PORT")
	}
	if port == "" {
		port = "8082"
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

	go startWorker(ctx, time.Duration(interval_sec)*time.Second)

	<-ctx.Done()
	log.Print("Shutting down CDC Booking Worker...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Shutdown complete")
}
