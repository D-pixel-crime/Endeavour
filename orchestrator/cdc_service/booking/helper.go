package booking

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	sqs_config "github.com/D-pixel-crime/Endeavour/aws_sqs_shared/config"
	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/monitoring"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/gin-gonic/gin"
)

func InitBookingEgressSQSClient(ctx context.Context) (*sqs.Client, error) {
	return sqs_config.NewDefaultSQSClient(ctx)
}

func InitSQSClient(ctx context.Context) (*sqs.Client, error) {
	return InitBookingEgressSQSClient(ctx)
}

func InitBooking(ctx context.Context) error {
	queueURL := os.Getenv("BOOKING_INGRESS_SQS_URL")
	if queueURL == "" {
		return fmt.Errorf("BOOKING_INGRESS_SQS_URL is not set")
	}
	BookingEgressQueueURL = queueURL

	client, err := InitBookingEgressSQSClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create SQS client: %w", err)
	}
	BookingSQSClient = client
	return nil
}

func StartBookingWorker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Print("CDC Orchestrator Booking Worker is Stopping!")
			return

		case <-ticker.C:
			go ProcessEgressBatch()
		}
	}
}

func StartWorker(ctx context.Context, interval time.Duration) {
	StartBookingWorker(ctx, interval)
}

func HealthHandler(monitor *monitoring.QueueMonitor) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		if err := monitor.HealthCheck(ctx, getQueueURL()); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status": "healthy",
		})
	}
}

func StatsHandler(monitor *monitoring.QueueMonitor) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		stats, err := monitor.GetQueueStats(ctx, getQueueURL())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, stats)
	}
}

func RegisterRoutes(r *gin.Engine, monitor *monitoring.QueueMonitor) {
	r.GET("/health", HealthHandler(monitor))
	r.GET("/stats", StatsHandler(monitor))
}
