package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

type BatchSendResult struct {
	Successful []string
	Failed     []BatchSendError
}

type BatchSendError struct {
	MessageID string
	Code      string
	Message   string
}

func (producer *Producer) sendSingleBatch(
	ctx context.Context,
	queueURL string,
	messages []*Message,
) (*BatchSendResult, error) {
	if len(messages) == 0 {
		return &BatchSendResult{}, nil
	}

	entries := make([]types.SendMessageBatchRequestEntry, len(messages))

	for i, msg := range messages {
		if msg.ID == "" {
			msg.ID = uuid.New().String()
		}
		if msg.Timestamp.IsZero() {
			msg.Timestamp = time.Now().UTC()
		}

		body, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("Error Marshalling Message %d: %w", i, err)
		}

		entry := types.SendMessageBatchRequestEntry{
			Id:          aws.String(msg.ID),
			MessageBody: aws.String(string(body)),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"MessageType": {
					DataType:    aws.String("String"),
					StringValue: aws.String(msg.Type),
				},
				"CorrelationId": {
					DataType:    aws.String("String"),
					StringValue: aws.String(msg.ID),
				},
			},
		}

		if !strings.Contains(queueURL, ".fifo") {
			return nil, fmt.Errorf("queue %s must be a FIFO queue (name must end with .fifo)", queueURL)
		}

		groupID := msg.GroupID
		if groupID == "" && msg.Payload != nil {
			if wfID, ok := msg.Payload["workflow_id"].(string); ok && wfID != "" {
				groupID = wfID
			}
		}
		if groupID == "" {
			return nil, fmt.Errorf("MessageGroupId (workflow_id) is missing for message %s on FIFO queue %s", msg.ID, queueURL)
		}
		entry.MessageGroupId = aws.String(groupID)

		if msg.DeduplicationID == "" {
			return nil, fmt.Errorf("MessageDeduplicationId is missing for message %s on FIFO queue %s", msg.ID, queueURL)
		}
		entry.MessageDeduplicationId = aws.String(msg.DeduplicationID)

		entries[i] = entry
	}

	input := &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(queueURL),
		Entries:  entries,
	}

	res, err := producer.client.SendMessageBatch(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("Error sending batch message to SQS: %w", err)
	}

	batchRes := &BatchSendResult{
		Successful: make([]string, len(res.Successful)),
		Failed:     make([]BatchSendError, len(res.Failed)),
	}

	for i, s := range res.Successful {
		batchRes.Successful[i] = *s.Id
	}

	for i, f := range res.Failed {
		batchRes.Failed[i] = BatchSendError{
			MessageID: *f.Id,
			Code:      *f.Code,
			Message:   *f.Message,
		}
	}

	return batchRes, nil
}

func (producer *Producer) SendBatch(
	ctx context.Context,
	queueURL string,
	messages []*Message,
) (*BatchSendResult, error) {
	if len(messages) == 0 {
		return &BatchSendResult{}, nil
	}

	targetURL := queueURL
	if targetURL == "" {
		targetURL = producer.queueURL
	}
	if targetURL == "" {
		return nil, fmt.Errorf("queue URL cannot be empty")
	}
	if !strings.Contains(targetURL, ".fifo") {
		return nil, fmt.Errorf("queue %s must be a FIFO queue (name must end with .fifo)", targetURL)
	}

	aggregated := &BatchSendResult{}

	for i := 0; i < len(messages); i += 10 {
		end := min(i+10, len(messages))
		chunk := messages[i:end]

		res, err := producer.sendSingleBatch(ctx, targetURL, chunk)
		if err != nil {
			return aggregated, err
		}

		aggregated.Successful = append(aggregated.Successful, res.Successful...)
		aggregated.Failed = append(aggregated.Failed, res.Failed...)
	}

	return aggregated, nil
}
