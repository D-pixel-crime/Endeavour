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
	return consumer.ReceiveBatch(ctx, shared_vars.BookingEgressSQSClient, shared_vars.BookingEgressQueueURL, 10)
}
