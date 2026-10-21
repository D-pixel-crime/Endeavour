package poller

import (
	"context"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	"github.com/D-pixel-crime/Endeavour/booking/shared"
)

func PullBatchFromSQS(ctx context.Context) ([]*consumer.Message, error) {
	if shared.IngressSQSClient == nil || shared.IngressQueueURL == "" {
		return nil, nil
	}
	return consumer.ReceiveMessages(ctx, shared.IngressSQSClient, shared.IngressQueueURL, 10, 0, shared.IngressVisibilityTimeout)
}
