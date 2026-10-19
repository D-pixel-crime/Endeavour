package consumer

import (
	"context"
	"fmt"
	"log"

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
		entryID := msg.ID
		if entryID == "" {
			entryID = fmt.Sprintf("msg-%d", i)
		}
		entries[i] = types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(entryID),
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

func DeleteAllBatches(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message) (*BatchDeleteResult, error) {
	if len(messages) == 0 || client == nil || queueURL == "" {
		return &BatchDeleteResult{}, nil
	}

	aggregatedResult := &BatchDeleteResult{}

	for i := 0; i < len(messages); i += 10 {
		end := min(i+10, len(messages))
		chunk := messages[i:end]

		result, err := DeleteMessageBatch(ctx, client, queueURL, chunk)
		if err != nil {
			log.Printf("Failed to batch delete SQS messages from %s: %v", queueURL, err)
			continue
		}

		aggregatedResult.Successful = append(aggregatedResult.Successful, result.Successful...)
		aggregatedResult.Failed = append(aggregatedResult.Failed, result.Failed...)

		for _, f := range result.Failed {
			log.Printf("Failed to delete SQS message (id=%s, code=%s) from %s: %s", f.ID, f.Code, queueURL, f.Message)
		}
	}

	return aggregatedResult, nil
}
