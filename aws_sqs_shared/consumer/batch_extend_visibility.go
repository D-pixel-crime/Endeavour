package consumer

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

type BatchExtendError struct {
	ID      string
	Code    string
	Message string
}

type BatchExtendResult struct {
	Successful []string
	Failed     []BatchExtendError
}

// ExtendMessageVisibility changes the visibility timeout of a single message by receipt handle.
func ExtendMessageVisibility(ctx context.Context, client *sqs.Client, queueURL string, receiptHandle string, visibilityTimeout int32) error {
	if client == nil || queueURL == "" || receiptHandle == "" {
		return fmt.Errorf("client, queueURL, and receiptHandle cannot be empty")
	}
	if !strings.Contains(queueURL, ".fifo") {
		return fmt.Errorf(" queue %s must be a FIFO queue (name must end with .fifo)", queueURL)
	}
	if visibilityTimeout < 0 {
		return fmt.Errorf("visibilityTimeout must be non-negative, got %d", visibilityTimeout)
	}

	input := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(queueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: visibilityTimeout,
	}

	_, err := client.ChangeMessageVisibility(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to change message visibility: %w", err)
	}
	return nil
}

// ExtendMessageBatchVisibility changes the visibility timeout for a batch of up to 10 messages.
func ExtendMessageBatchVisibility(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message, visibilityTimeout int32) (*BatchExtendResult, error) {
	if len(messages) == 0 {
		return &BatchExtendResult{}, nil
	}
	if !strings.Contains(queueURL, ".fifo") {
		return nil, fmt.Errorf(" queue %s must be a FIFO queue (name must end with .fifo)", queueURL)
	}
	if len(messages) > 10 {
		return nil, fmt.Errorf("batch size exceeds maximum allowed of 10 messages")
	}
	if client == nil || queueURL == "" {
		return nil, fmt.Errorf("client and queueURL cannot be empty")
	}
	if visibilityTimeout < 0 {
		return nil, fmt.Errorf("visibilityTimeout must be non-negative, got %d", visibilityTimeout)
	}

	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(messages))
	for i, msg := range messages {
		entryID := msg.ID.String()
		if msg.ID == uuid.Nil {
			entryID = fmt.Sprintf("msg-%d", i)
		}
		entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(entryID),
			ReceiptHandle:     aws.String(msg.ReceiptHandle),
			VisibilityTimeout: visibilityTimeout,
		}
	}

	input := &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(queueURL),
		Entries:  entries,
	}

	result, err := client.ChangeMessageVisibilityBatch(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("error changing message visibility in batch: %w", err)
	}

	batchResult := &BatchExtendResult{
		Successful: make([]string, len(result.Successful)),
		Failed:     make([]BatchExtendError, len(result.Failed)),
	}

	for i, s := range result.Successful {
		batchResult.Successful[i] = *s.Id
	}

	for i, f := range result.Failed {
		batchResult.Failed[i] = BatchExtendError{
			ID:      *f.Id,
			Code:    *f.Code,
			Message: *f.Message,
		}
	}

	return batchResult, nil
}

// ExtendAllBatchesVisibility divides messages into chunks of 10 and extends visibility for all.
func ExtendAllBatchesVisibility(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message, visibilityTimeout int32) (*BatchExtendResult, error) {
	if len(messages) == 0 || client == nil || queueURL == "" || visibilityTimeout < 0 {
		return &BatchExtendResult{}, nil
	}

	aggregatedResult := &BatchExtendResult{}

	for i := 0; i < len(messages); i += 10 {
		end := min(i+10, len(messages))
		chunk := messages[i:end]

		result, err := ExtendMessageBatchVisibility(ctx, client, queueURL, chunk, visibilityTimeout)
		if err != nil {
			log.Printf("Failed to extend SQS message visibility on %s: %v", queueURL, err)
			continue
		}

		aggregatedResult.Successful = append(aggregatedResult.Successful, result.Successful...)
		aggregatedResult.Failed = append(aggregatedResult.Failed, result.Failed...)

		for _, f := range result.Failed {
			log.Printf("Failed to extend SQS message visibility (id=%s, code=%s) on %s: %s", f.ID, f.Code, queueURL, f.Message)
		}
	}

	return aggregatedResult, nil
}
