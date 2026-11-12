package poller

import (
	"context"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	"github.com/D-pixel-crime/Endeavour/payment/handlers"
	"github.com/D-pixel-crime/Endeavour/payment/shared"
)

func StartPoller(ctx context.Context, interval time.Duration, maxConcurrency int) {
	bp := consumer.NewBatchPoller(consumer.PollerConfig{
		Name:              "Payment Ingress",
		Client:            shared.IngressSQSClient,
		QueueURL:          shared.IngressQueueURL,
		Interval:          interval,
		MaxConcurrency:    maxConcurrency,
		VisibilityTimeout: shared.IngressVisibilityTimeout,
	}, handlers.ProcessPaymentIngressMessage)
	bp.Start(ctx)
}
