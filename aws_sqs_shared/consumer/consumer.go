package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

type Message struct {
	ID            uuid.UUID         `json:"id"`
	Type          string            `json:"type"`
	Payload       map[string]any    `json:"payload"`
	Timestamp     time.Time         `json:"timestamp"`
	UpdatedAt     time.Time         `json:"updated_at,omitempty"`
	ReceiptHandle string            `json:"-"`
	Attributes    map[string]string `json:"-"`
}

// BatchError describes an individual SQS message failure within a batch API call.
type BatchError struct {
	ID      string
	Code    string
	Message string
}

// BatchResult aggregates succeeded and failed entry IDs from an SQS batch operation.
type BatchResult struct {
	Successful []string
	Failed     []BatchError
}

// DecodePayload validates an SQS message and unmarshals its JSON payload into T.
// Returns ok=false for nil or malformed messages (poison pills) after logging, so callers can ack and drop them.
func DecodePayload[T any](msg *Message, label string, workflowID func(*T) uuid.UUID) (T, []byte, bool) {
	var zero T
	if msg == nil {
		return zero, nil, false
	}
	if msg.Payload == nil {
		log.Printf("[POISON PILL DETECTED] Message payload is nil in %s message %s; acknowledging to prevent FIFO HoL blocking", label, msg.ID)
		return zero, nil, false
	}

	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		log.Printf("[POISON PILL DETECTED] Failed to marshal payload for %s message %s: %v; acknowledging to prevent FIFO HoL blocking", label, msg.ID, err)
		return zero, nil, false
	}

	var decoded T
	if err := json.Unmarshal(payloadBytes, &decoded); err != nil {
		log.Printf("[POISON PILL DETECTED] Unmarshal failed for %s message %s: %v; acknowledging to prevent FIFO HoL blocking", label, msg.ID, err)
		return zero, nil, false
	}

	if workflowID(&decoded) == uuid.Nil {
		log.Printf("[POISON PILL DETECTED] Missing or invalid workflow ID in %s message %s; acknowledging to prevent FIFO HoL blocking", label, msg.ID)
		return zero, nil, false
	}

	return decoded, payloadBytes, true
}

func ReceiveMessages(ctx context.Context, client *sqs.Client, queueURL string, maxMessages int32, waitTimeSeconds int32, visibilityTimeout int32) ([]*Message, error) {
	if !strings.Contains(queueURL, ".fifo") {
		return nil, fmt.Errorf("queue %s must be a FIFO queue (name must end with .fifo)", queueURL)
	}

	if maxMessages <= 0 || maxMessages > 10 {
		maxMessages = 10
	}

	input := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		MaxNumberOfMessages:   maxMessages,
		WaitTimeSeconds:       waitTimeSeconds,
		MessageAttributeNames: []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameAll,
		},
	}
	if visibilityTimeout > 0 {
		input.VisibilityTimeout = visibilityTimeout
	}

	result, err := client.ReceiveMessage(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("Error Receiving Messages: %w", err)
	}

	messages := make([]*Message, 0, len(result.Messages))
	for _, sqsMsg := range result.Messages {
		var msg Message
		if err := json.Unmarshal([]byte(*sqsMsg.Body), &msg); err != nil {
			log.Printf("[POISON PILL DETECTED] Error unmarshalling raw SQS message %s from %s: %v; deleting message to prevent FIFO HoL blocking", *sqsMsg.MessageId, queueURL, err)
			_, delErr := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: sqsMsg.ReceiptHandle,
			})
			if delErr != nil {
				log.Printf("Failed to delete corrupt SQS message %s: %v", *sqsMsg.MessageId, delErr)
			}
			continue
		}

		msg.ReceiptHandle = *sqsMsg.ReceiptHandle
		msg.Attributes = sqsMsg.Attributes

		if msg.UpdatedAt.IsZero() {
			msg.UpdatedAt = resolveMessageTime(&msg)
		}

		messages = append(messages, &msg)
	}

	return messages, nil
}

func resolveMessageTime(msg *Message) time.Time {
	if !msg.Timestamp.IsZero() {
		return msg.Timestamp
	}
	if msg.Payload != nil {
		if uaStr, ok := msg.Payload["updated_at"].(string); ok && uaStr != "" {
			if t, err := time.Parse(time.RFC3339, uaStr); err == nil {
				return t
			}
		}
	}
	if msg.Attributes != nil {
		if sentTsStr, ok := msg.Attributes["SentTimestamp"]; ok && sentTsStr != "" {
			if ms, err := strconv.ParseInt(sentTsStr, 10, 64); err == nil {
				return time.UnixMilli(ms)
			}
		}
	}
	return time.Now()
}

func validateFIFOBatch(client *sqs.Client, queueURL string, messages []*Message) error {
	if client == nil || queueURL == "" {
		return fmt.Errorf("client and queueURL cannot be empty")
	}
	if !strings.Contains(queueURL, ".fifo") {
		return fmt.Errorf("queue %s must be a FIFO queue (name must end with .fifo)", queueURL)
	}
	if len(messages) > 10 {
		return fmt.Errorf("batch size exceeds maximum allowed of 10 messages")
	}
	return nil
}

func batchEntryID(msg *Message, idx int) string {
	if msg.ID == uuid.Nil {
		return fmt.Sprintf("msg-%d", idx)
	}
	return msg.ID.String()
}

func batchRequestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() != nil {
		return context.WithTimeout(context.Background(), 5*time.Second)
	}
	return ctx, func() {}
}

func mapBatchErrors(entries []types.BatchResultErrorEntry) []BatchError {
	failed := make([]BatchError, len(entries))
	for i, f := range entries {
		failed[i] = BatchError{
			ID:      *f.Id,
			Code:    *f.Code,
			Message: *f.Message,
		}
	}
	return failed
}

func runAllBatches(
	messages []*Message,
	queueURL string,
	opDesc string,
	fn func(chunk []*Message) (*BatchResult, error),
) (*BatchResult, error) {
	aggregated := &BatchResult{}
	for i := 0; i < len(messages); i += 10 {
		chunk := messages[i:min(i+10, len(messages))]
		result, err := fn(chunk)
		if err != nil {
			log.Printf("Failed to %s on %s: %v", opDesc, queueURL, err)
			continue
		}

		aggregated.Successful = append(aggregated.Successful, result.Successful...)
		aggregated.Failed = append(aggregated.Failed, result.Failed...)
		for _, f := range result.Failed {
			log.Printf("Failed to %s (id=%s, code=%s) on %s: %s", opDesc, f.ID, f.Code, queueURL, f.Message)
		}
	}
	return aggregated, nil
}
