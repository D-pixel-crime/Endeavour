package queue

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type QueueManager struct {
	client *sqs.Client
}

func NewQueueManager(client *sqs.Client) *QueueManager {
	return &QueueManager{
		client: client,
	}
}

func (manager *QueueManager) CreateStandardQueue(ctx context.Context, queueName string, visibilityTimeout, messageRetention, receiveMsgWaitTimeSeconds int) (string, error) {
	input := &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"VisibilityTimeout":             strconv.Itoa(visibilityTimeout),
			"MessageRetentionPeriod":        strconv.Itoa(messageRetention),
			"ReceiveMessageWaitTimeSeconds": strconv.Itoa(receiveMsgWaitTimeSeconds),
		},
	}

	result, err := manager.client.CreateQueue(ctx, input)
	if err != nil {
		return "", fmt.Errorf("Error creating Standard Queue: %w", err)
	}

	return *result.QueueUrl, nil
}

func (manager *QueueManager) CreateFIFOQueue(ctx context.Context, queueName string, contentBasedDedup bool, visibilityTimeout, messageRetention, receiveMsgWaitTimeSeconds int) (string, error) {
	fifoQueueName := queueName + ".fifo"

	attributes := map[string]string{
		"FifoQueue":                     "true",
		"VisibilityTimeout":             strconv.Itoa(visibilityTimeout),
		"MessageRetentionPeriod":        strconv.Itoa(messageRetention),
		"ReceiveMessageWaitTimeSeconds": strconv.Itoa(receiveMsgWaitTimeSeconds),
	}

	if contentBasedDedup {
		attributes["ContentBasedDeduplication"] = "true"
	}

	input := &sqs.CreateQueueInput{
		QueueName:  aws.String(fifoQueueName),
		Attributes: attributes,
	}

	result, err := manager.client.CreateQueue(ctx, input)
	if err != nil {
		return "", fmt.Errorf("Error creating FIFO Queue: %w", err)
	}

	return *result.QueueUrl, nil
}

func (manager *QueueManager) GetQueueUrl(ctx context.Context, queueName string) (string, error) {
	if len(queueName) == 0 {
		return "", fmt.Errorf("Queue Name cannot be empty!")
	}

	input := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}

	result, err := manager.client.GetQueueUrl(ctx, input)
	if err != nil {
		return "", fmt.Errorf("Error fetching queue url: %w", err)
	}

	return *result.QueueUrl, nil
}

func (manager *QueueManager) DeleteQueue(ctx context.Context, queueURL string) error {
	if len(queueURL) == 0 {
		return fmt.Errorf("Queue URL cannot be empty!")
	}

	input := &sqs.DeleteQueueInput{
		QueueUrl: aws.String(queueURL),
	}

	_, err := manager.client.DeleteQueue(ctx, input)
	if err != nil {
		return fmt.Errorf("Error deleting queue: %w", err)
	}

	return nil
}

func (manager *QueueManager) PurgeQueue(ctx context.Context, queueURL string) error {
	if len(queueURL) == 0 {
		return fmt.Errorf("Queue URL cannot be empty!")
	}

	input := &sqs.PurgeQueueInput{
		QueueUrl: aws.String(queueURL),
	}

	_, err := manager.client.PurgeQueue(ctx, input)
	if err != nil {
		return fmt.Errorf("Error purging queue: %w", err)
	}

	return nil
}

func (manager *QueueManager) ListQueues(ctx context.Context, prefix string) ([]string, error) {
	input := &sqs.ListQueuesInput{
		QueueNamePrefix: aws.String(prefix),
	}

	result, err := manager.client.ListQueues(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("Error listing queues: %w", err)
	}

	return result.QueueUrls, nil
}
