package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/monitoring"
	db_orchestrator "github.com/D-pixel-crime/Endeavour/orchestrator/db"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

var (
	sqsClient *sqs.Client
	initOnce  sync.Once
)

func getSQSClient(ctx context.Context) (*sqs.Client, error) {
	var initErr error
	initOnce.Do(func() {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			initErr = err
			return
		}
		sqsClient = sqs.NewFromConfig(cfg)
	})
	return sqsClient, initErr
}

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

func healthHandler(monitor *monitoring.QueueMonitor, queueURL string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		if err := monitor.HealthCheck(ctx, queueURL); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status": "healthy",
		})
	}
}

func statsHandler(monitor *monitoring.QueueMonitor, queueURL string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		stats, err := monitor.GetQueueStats(ctx, queueURL)
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

	queueURL := os.Getenv("BOOKING_INGRESS_SQS_URL")
	if queueURL == "" {
		log.Fatalf("BOOKING_INGRESS_SQS_URL is not set")
	}

	sqsClient, err := getSQSClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create SQS client: %v", err)
	}

	monitor := monitoring.NewQueueMonitor(sqsClient)

	r := gin.Default()

	r.GET("/health", healthHandler(monitor, queueURL))
	r.GET("/stats", statsHandler(monitor, queueURL))

	port := os.Getenv("CDC_ORCHESTRATOR_PORT")
	if port == "" {
		port = "8081"
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

	go startWorker(ctx, time.Duration(interval_sec)*time.Second, cdc_orchestrator_db_pool)

	<-ctx.Done()
	log.Print("Shutting down CDC Orchestrator Worker...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Shutdown complete")
}
