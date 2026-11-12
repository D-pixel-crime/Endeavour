package poller

import (
	"context"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	"github.com/D-pixel-crime/Endeavour/booking/handlers"
	"github.com/D-pixel-crime/Endeavour/booking/shared"
)

func StartPoller(ctx context.Context, interval time.Duration, maxConcurrency int) {
	bp := consumer.NewBatchPoller(consumer.PollerConfig{
		Name:              "Booking Ingress",
		Client:            shared.IngressSQSClient,
		QueueURL:          shared.IngressQueueURL,
		Interval:          interval,
		MaxConcurrency:    maxConcurrency,
		VisibilityTimeout: shared.IngressVisibilityTimeout,
	}, handlers.ProcessBookingIngressMessage)
	bp.Start(ctx)
}
