package consumer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

type BatchVisibilityError struct {
	ID      string
	Code    string
	Message string
}

type BatchVisibilityResult struct {
	Successful []string
	Failed     []BatchVisibilityError
}

type BatchExtendError = BatchVisibilityError
type BatchExtendResult = BatchVisibilityResult

// ChangeMessageVisibilityBatch changes the visibility timeout for a batch of up to 10 messages.
func ChangeMessageVisibilityBatch(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message, visibilityTimeout int32) (*BatchVisibilityResult, error) {
	if len(messages) == 0 {
		return &BatchVisibilityResult{}, nil
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

	reqCtx := ctx
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}

	result, err := client.ChangeMessageVisibilityBatch(reqCtx, input)
	if err != nil {
		return nil, fmt.Errorf("error changing message visibility in batch: %w", err)
	}

	batchResult := &BatchVisibilityResult{
		Successful: make([]string, len(result.Successful)),
		Failed:     make([]BatchVisibilityError, len(result.Failed)),
	}

	for i, s := range result.Successful {
		batchResult.Successful[i] = *s.Id
	}

	for i, f := range result.Failed {
		batchResult.Failed[i] = BatchVisibilityError{
			ID:      *f.Id,
			Code:    *f.Code,
			Message: *f.Message,
		}
	}

	return batchResult, nil
}

// ChangeVisibilityAllBatches divides messages into chunks of 10 and changes visibility for all.
func ChangeVisibilityAllBatches(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message, visibilityTimeout int32) (*BatchVisibilityResult, error) {
	if len(messages) == 0 || client == nil || queueURL == "" || visibilityTimeout < 0 {
		return &BatchVisibilityResult{}, nil
	}

	aggregatedResult := &BatchVisibilityResult{}

	for i := 0; i < len(messages); i += 10 {
		end := min(i+10, len(messages))
		chunk := messages[i:end]

		result, err := ChangeMessageVisibilityBatch(ctx, client, queueURL, chunk, visibilityTimeout)
		if err != nil {
			log.Printf("Failed to change SQS message visibility on %s: %v", queueURL, err)
			continue
		}

		aggregatedResult.Successful = append(aggregatedResult.Successful, result.Successful...)
		aggregatedResult.Failed = append(aggregatedResult.Failed, result.Failed...)

		for _, f := range result.Failed {
			log.Printf("Failed to change SQS message visibility (id=%s, code=%s) on %s: %s", f.ID, f.Code, queueURL, f.Message)
		}
	}

	return aggregatedResult, nil
}

// ChangeAllBatchesVisibility is an alias for ChangeVisibilityAllBatches.
func ChangeAllBatchesVisibility(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message, visibilityTimeout int32) (*BatchVisibilityResult, error) {
	return ChangeVisibilityAllBatches(ctx, client, queueURL, messages, visibilityTimeout)
}

// ChangeMessageVisibilityAllBatches is an alias for ChangeVisibilityAllBatches.
func ChangeMessageVisibilityAllBatches(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message, visibilityTimeout int32) (*BatchVisibilityResult, error) {
	return ChangeVisibilityAllBatches(ctx, client, queueURL, messages, visibilityTimeout)
}

// ExtendMessageBatchVisibility changes the visibility timeout for a batch of up to 10 messages.
func ExtendMessageBatchVisibility(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message, visibilityTimeout int32) (*BatchExtendResult, error) {
	return ChangeMessageVisibilityBatch(ctx, client, queueURL, messages, visibilityTimeout)
}

// ExtendAllBatchesVisibility divides messages into chunks of 10 and extends visibility for all.
func ExtendAllBatchesVisibility(ctx context.Context, client *sqs.Client, queueURL string, messages []*Message, visibilityTimeout int32) (*BatchExtendResult, error) {
	return ChangeVisibilityAllBatches(ctx, client, queueURL, messages, visibilityTimeout)
}
