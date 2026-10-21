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
)

type Message struct {
	ID            string            `json:"id"`
	Type          string            `json:"type"`
	Payload       map[string]any    `json:"payload"`
	Timestamp     time.Time         `json:"timestamp"`
	CreatedAt     time.Time         `json:"created_at,omitempty"`
	ReceiptHandle string            `json:"-"`
	Attributes    map[string]string `json:"-"`
}

func (msg *Message) GetCreatedAt() time.Time {
	if !msg.CreatedAt.IsZero() {
		return msg.CreatedAt
	}
	if !msg.Timestamp.IsZero() {
		return msg.Timestamp
	}
	return time.Now()
}

func ReceiveMessages(ctx context.Context, client *sqs.Client, queueURL string, maxMessages int32, waitTimeSeconds int32, visibilityTimeout int32) ([]*Message, error) {
	if !strings.Contains(queueURL, ".fifo") {
		return nil, fmt.Errorf(" queue %s must be a FIFO queue (name must end with .fifo)", queueURL)
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
			log.Printf("Error Unmarshalling Message %s: %v", *sqsMsg.MessageId, err)
			continue
		}

		msg.ReceiptHandle = *sqsMsg.ReceiptHandle
		msg.Attributes = sqsMsg.Attributes

		if msg.CreatedAt.IsZero() {
			if !msg.Timestamp.IsZero() {
				msg.CreatedAt = msg.Timestamp
			} else if msg.Payload != nil {
				if caStr, ok := msg.Payload["created_at"].(string); ok && caStr != "" {
					if t, err := time.Parse(time.RFC3339, caStr); err == nil {
						msg.CreatedAt = t
					}
				}
			}
			if msg.CreatedAt.IsZero() && msg.Attributes != nil {
				if sentTsStr, ok := msg.Attributes["SentTimestamp"]; ok && sentTsStr != "" {
					if ms, err := strconv.ParseInt(sentTsStr, 10, 64); err == nil {
						msg.CreatedAt = time.UnixMilli(ms)
					}
				}
			}
			if msg.CreatedAt.IsZero() {
				msg.CreatedAt = time.Now()
			}
		}

		messages = append(messages, &msg)
	}

	return messages, nil
}
