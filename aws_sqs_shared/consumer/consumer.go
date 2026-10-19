package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	ReceiptHandle string            `json:"-"`
	Attributes    map[string]string `json:"-"`
}

func ReceiveMessages(ctx context.Context, client *sqs.Client, queueURL string, maxMessages int32, waitTimeSeconds int32, visibilityTimeout int32) ([]*Message, error) {
	if maxMessages <= 0 || maxMessages > 10 {
		maxMessages = 10
	}
	if visibilityTimeout <= 0 {
		visibilityTimeout = 30
	}

	input := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		MaxNumberOfMessages:   maxMessages,
		VisibilityTimeout:     visibilityTimeout,
		WaitTimeSeconds:       waitTimeSeconds,
		MessageAttributeNames: []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameAll,
		},
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
		messages = append(messages, &msg)
	}

	return messages, nil
}

func ReceiveBatch(ctx context.Context, client *sqs.Client, queueURL string, maxMessages int32) ([]*Message, error) {
	return ReceiveMessages(ctx, client, queueURL, maxMessages, 0, 30)
}
