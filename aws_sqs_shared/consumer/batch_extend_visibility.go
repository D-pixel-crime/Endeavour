package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// ChangeMessageVisibilityBatch changes the visibility timeout for a batch of up to 10 messages.
func ChangeMessageVisibilityBatch(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message, visibilityTimeout int32) (*BatchResult, error) {
	if len(messages) == 0 {
		return &BatchResult{}, nil
	}
	if err := validateFIFOBatch(client, queueURL, messages); err != nil {
		return nil, err
	}
	if visibilityTimeout < 0 {
		return nil, fmt.Errorf("visibilityTimeout must be non-negative, got %d", visibilityTimeout)
	}

	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(messages))
	for i, msg := range messages {
		entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(batchEntryID(msg, i)),
			ReceiptHandle:     aws.String(msg.ReceiptHandle),
			VisibilityTimeout: visibilityTimeout,
		}
	}

	reqCtx, cancel := batchRequestContext(ctx)
	defer cancel()

	result, err := client.ChangeMessageVisibilityBatch(reqCtx, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(queueURL),
		Entries:  entries,
	})
	if err != nil {
		return nil, fmt.Errorf("error changing message visibility in batch: %w", err)
	}

	batchResult := &BatchResult{
		Successful: make([]string, len(result.Successful)),
		Failed:     mapBatchErrors(result.Failed),
	}
	for i, s := range result.Successful {
		batchResult.Successful[i] = *s.Id
	}
	return batchResult, nil
}

// ChangeVisibilityAllBatches divides messages into chunks of 10 and changes visibility for all.
func ChangeVisibilityAllBatches(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message, visibilityTimeout int32) (*BatchResult, error) {
	if len(messages) == 0 || client == nil || queueURL == "" || visibilityTimeout < 0 {
		return &BatchResult{}, nil
	}
	return runAllBatches(messages, queueURL, "change SQS message visibility", func(chunk []*Message) (*BatchResult, error) {
		return ChangeMessageVisibilityBatch(ctx, client, queueURL, chunk, visibilityTimeout)
	})
}
