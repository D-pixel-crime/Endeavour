package poller

import (
	"context"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	egresshandlers "github.com/D-pixel-crime/Endeavour/orchestrator/handlers/egress_handlers"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
)

func StartBookingEgressPoller(ctx context.Context, interval time.Duration, maxConcurrency int) {
	bp := consumer.NewBatchPoller(consumer.PollerConfig{
		Name:              "Orchestrator Booking Egress",
		Client:            shared_vars.BookingEgressSQSClient,
		QueueURL:          shared_vars.BookingEgressQueueURL,
		Interval:          interval,
		MaxConcurrency:    maxConcurrency,
		VisibilityTimeout: shared_vars.BookingEgressVisibilityTimeout,
	}, egresshandlers.ProcessBookingEgressMessage)
	bp.Start(ctx)
}

func StartPaymentEgressPoller(ctx context.Context, interval time.Duration, maxConcurrency int) {
	bp := consumer.NewBatchPoller(consumer.PollerConfig{
		Name:              "Orchestrator Payment Egress",
		Client:            shared_vars.PaymentEgressSQSClient,
		QueueURL:          shared_vars.PaymentEgressQueueURL,
		Interval:          interval,
		MaxConcurrency:    maxConcurrency,
		VisibilityTimeout: shared_vars.PaymentEgressVisibilityTimeout,
	}, egresshandlers.ProcessPaymentEgressMessage)
	bp.Start(ctx)
}
