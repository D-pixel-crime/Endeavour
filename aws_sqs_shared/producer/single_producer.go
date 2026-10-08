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

type Producer struct {
	client   *sqs.Client
	queueURL string
}

func NewProducer(client *sqs.Client, queueURL string) *Producer {
	return &Producer{
		client:   client,
		queueURL: queueURL,
	}
}

type Message struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	Timestamp time.Time      `json:"timestamp"`
}

func (producer *Producer) SendMessage(
	ctx context.Context,
	msg *Message,
	delaySeconds int32,
) (string, error) {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	msg.Timestamp = time.Now().UTC()

	body, err := json.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("Error Marshalling Message: %w", err)
	}

	input := &sqs.SendMessageInput{
		QueueUrl:     aws.String(producer.queueURL),
		MessageBody:  aws.String(string(body)),
		DelaySeconds: delaySeconds,
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

	res, err := producer.client.SendMessage(ctx, input)
	if err != nil {
		return "", fmt.Errorf("Error sending message to SQS: %w", err)
	}

	return *res.MessageId, nil
}

func (producer *Producer) SendFIFOMessage(
	ctx context.Context,
	msg *Message,
	msgGrpID, deduplicationID string,
) (string, error) {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	msg.Timestamp = time.Now().UTC()

	body, err := json.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("Error Marshalling Message: %w", err)
	}

	if deduplicationID == "" {
		deduplicationID = msg.ID
	}

	input := &sqs.SendMessageInput{
		QueueUrl:               &producer.queueURL,
		MessageBody:            aws.String(string(body)),
		MessageGroupId:         aws.String(msgGrpID),
		MessageDeduplicationId: aws.String(deduplicationID),
	}

	res, err := producer.client.SendMessage(ctx, input)
	if err != nil {
		return "", fmt.Errorf("Error sending message to SQS: %w", err)
	}

	return *res.MessageId, nil
}
