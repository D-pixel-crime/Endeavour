package poller

import (
	"context"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	"github.com/D-pixel-crime/Endeavour/booking/shared"
)

func PullBatchFromSQS(ctx context.Context) ([]*consumer.Message, error) {
	return consumer.ReceiveBatch(ctx, shared.IngressSQSClient, shared.IngressQueueURL, 10)
}
