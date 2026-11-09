package poller

import (
	"context"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	egresshandlers "github.com/D-pixel-crime/Endeavour/orchestrator/handlers/egress_handlers"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
)

const defaultMaxConcurrentPulls = 5

func resolveMaxConcurrency(maxConcurrency []int) int {
	if len(maxConcurrency) > 0 && maxConcurrency[0] > 0 {
		return maxConcurrency[0]
	}
	return defaultMaxConcurrentPulls
}

func StartBookingEgressPoller(ctx context.Context, interval time.Duration, maxConcurrency ...int) {
	limit := resolveMaxConcurrency(maxConcurrency)
	bp := consumer.NewBatchPoller(consumer.PollerConfig{
		Name:              "Orchestrator Booking Egress",
		Client:            shared_vars.BookingEgressSQSClient,
		QueueURL:          shared_vars.BookingEgressQueueURL,
		Interval:          interval,
		MaxConcurrency:    limit,
		VisibilityTimeout: shared_vars.BookingEgressVisibilityTimeout,
	}, egresshandlers.ProcessBookingEgressBatch)
	bp.Start(ctx)
}

func StartPaymentEgressPoller(ctx context.Context, interval time.Duration, maxConcurrency ...int) {
	limit := resolveMaxConcurrency(maxConcurrency)
	bp := consumer.NewBatchPoller(consumer.PollerConfig{
		Name:              "Orchestrator Payment Egress",
		Client:            shared_vars.PaymentEgressSQSClient,
		QueueURL:          shared_vars.PaymentEgressQueueURL,
		Interval:          interval,
		MaxConcurrency:    limit,
		VisibilityTimeout: shared_vars.PaymentEgressVisibilityTimeout,
	}, egresshandlers.ProcessPaymentEgressBatch)
	bp.Start(ctx)
}
