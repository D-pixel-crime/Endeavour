package poller

import (
	"context"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
)

func PullBatchFromBookingEgressSQS(ctx context.Context) ([]*consumer.Message, error) {
	if shared_vars.BookingEgressSQSClient == nil || shared_vars.BookingEgressQueueURL == "" {
		return nil, nil
	}
	return consumer.ReceiveMessages(ctx, shared_vars.BookingEgressSQSClient, shared_vars.BookingEgressQueueURL, 10, 0, shared_vars.BookingEgressVisibilityTimeout)
}

func PullBatchFromPaymentEgressSQS(ctx context.Context) ([]*consumer.Message, error) {
	if shared_vars.PaymentEgressSQSClient == nil || shared_vars.PaymentEgressQueueURL == "" {
		return nil, nil
	}
	return consumer.ReceiveMessages(ctx, shared_vars.PaymentEgressSQSClient, shared_vars.PaymentEgressQueueURL, 10, 0, shared_vars.PaymentEgressVisibilityTimeout)
}

