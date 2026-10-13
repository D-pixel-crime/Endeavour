package consumer

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type BatchDeleteError struct {
	ID      string
	Code    string
	Message string
}

type BatchDeleteResult struct {
	Successful []string
	Failed     []BatchDeleteError
}

func DeleteMessageBatch(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message) (*BatchDeleteResult, error) {
	if len(messages) == 0 {
		return &BatchDeleteResult{}, nil
	}

	if len(messages) > 10 {
		return nil, fmt.Errorf("Error! Batch Size exceeds maximum allowed of 10 messages.")
	}

	entries := make([]types.DeleteMessageBatchRequestEntry, len(messages))
	for i, msg := range messages {
		entries[i] = types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(msg.ID),
			ReceiptHandle: aws.String(msg.ReceiptHandle),
		}
	}

	input := &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(queueURL),
		Entries:  entries,
	}

	result, err := client.DeleteMessageBatch(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("Error Deleting Messages in Batch: %w", err)
	}

	batchResult := &BatchDeleteResult{
		Successful: make([]string, len(result.Successful)),
		Failed:     make([]BatchDeleteError, len(result.Failed)),
	}

	for i, s := range result.Successful {
		batchResult.Successful[i] = *s.Id
	}

	for i, f := range result.Failed {
		batchResult.Failed[i] = BatchDeleteError{
			ID:      *f.Id,
			Code:    *f.Code,
			Message: *f.Message,
		}
	}

	return batchResult, nil
}

func (consumer *Consumer) DeleteMessageBatch(ctx context.Context, messages []*Message) (*BatchDeleteResult, error) {
	return DeleteMessageBatch(ctx, consumer.client, consumer.queueURL, messages)
}
