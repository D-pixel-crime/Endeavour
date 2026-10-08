package producer

import (
	"context"
	"encoding/json"
	"fmt"
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

func (producer *Producer) SendBatchMessage(
	ctx context.Context,
	messages []*Message,
) (*BatchSendResult, error) {
	if len(messages) == 0 {
		return &BatchSendResult{}, nil
	}

	if len(messages) > 10 {
		return nil, fmt.Errorf("Exceeded limit of AWS SQS Batch send of 10 messages")
	}

	entries := make([]types.SendMessageBatchRequestEntry, len(messages))

	for i, msg := range messages {
		if msg.ID == "" {
			msg.ID = uuid.New().String()
		}
		msg.Timestamp = time.Now().UTC()

		body, err := json.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("Error Marshalling Message %d: %w", i, err)
		}

		entries[i] = types.SendMessageBatchRequestEntry{
			Id:          aws.String(msg.ID),
			MessageBody: aws.String(string(body)),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"MessageType": {
					DataType:    aws.String("String"),
					StringValue: aws.String(msg.Type),
				},
			},
		}

	}

	input := &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(producer.queueURL),
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
		batchRes.Successful[i] = *s.MessageId
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
