package poller

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	"github.com/D-pixel-crime/Endeavour/booking/handlers"
	"github.com/D-pixel-crime/Endeavour/booking/shared"
)

const defaultMaxConcurrentPulls = 5

func resolveMaxConcurrency(maxConcurrency []int) int {
	if len(maxConcurrency) > 0 && maxConcurrency[0] > 0 {
		return maxConcurrency[0]
	}
	if envStr := os.Getenv("BOOKING_MAX_CONCURRENT_PULLS"); envStr != "" {
		if val, err := strconv.Atoi(envStr); err == nil && val > 0 {
			return val
		}
	}
	return defaultMaxConcurrentPulls
}

func StartPoller(ctx context.Context, interval time.Duration, maxConcurrency ...int) {
	limit := resolveMaxConcurrency(maxConcurrency)
	bp := consumer.NewBatchPoller(consumer.PollerConfig{
		Name:           "Booking Ingress",
		Client:         shared.IngressSQSClient,
		QueueURL:       shared.IngressQueueURL,
		Interval:       interval,
		MaxConcurrency: limit,
	}, handlers.ProcessIngressBatch)
	bp.Start(ctx)
}
