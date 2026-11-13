package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// DeleteMessageBatch deletes up to 10 SQS messages from a FIFO queue in one batch call.
func DeleteMessageBatch(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message) (*BatchResult, error) {
	if len(messages) == 0 {
		return &BatchResult{}, nil
	}
	if err := validateFIFOBatch(client, queueURL, messages); err != nil {
		return nil, err
	}

	entries := make([]types.DeleteMessageBatchRequestEntry, len(messages))
	for i, msg := range messages {
		entries[i] = types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(batchEntryID(msg, i)),
			ReceiptHandle: aws.String(msg.ReceiptHandle),
		}
	}

	result, err := client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(queueURL),
		Entries:  entries,
	})
	if err != nil {
		return nil, fmt.Errorf("error deleting messages in batch: %w", err)
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

// DeleteAllBatches divides messages into chunks of 10 and deletes all from the queue.
func DeleteAllBatches(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message) (*BatchResult, error) {
	if len(messages) == 0 || client == nil || queueURL == "" {
		return &BatchResult{}, nil
	}
	return runAllBatches(messages, queueURL, "batch delete SQS message", func(chunk []*Message) (*BatchResult, error) {
		return DeleteMessageBatch(ctx, client, queueURL, chunk)
	})
}
