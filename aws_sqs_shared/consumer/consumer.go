package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
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

type Handler func(ctx context.Context, msg *Message) error

type Consumer struct {
	client            *sqs.Client
	queueURL          string
	handler           Handler
	maxMessages       int32
	visibilityTimeout int32
	waitTimeSeconds   int32
	workerCount       int
}

type ConsumerConfig struct {
	QueueURL          string
	MaxMessages       int32
	VisibilityTimeout int32
	WaitTimeSeconds   int32
	WorkerCount       int
}

func NewConsumer(client *sqs.Client, cfg ConsumerConfig, handler Handler) *Consumer {
	if cfg.MaxMessages <= 0 || cfg.MaxMessages > 10 {
		cfg.MaxMessages = 10
	}
	if cfg.VisibilityTimeout <= 0 || cfg.VisibilityTimeout > 43200 {
		cfg.VisibilityTimeout = 30
	}
	if cfg.WaitTimeSeconds != 0 {
		cfg.WaitTimeSeconds = 0
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 5
	}

	return &Consumer{
		client:            client,
		queueURL:          cfg.QueueURL,
		handler:           handler,
		maxMessages:       cfg.MaxMessages,
		visibilityTimeout: cfg.VisibilityTimeout,
		waitTimeSeconds:   cfg.WaitTimeSeconds,
		workerCount:       cfg.WorkerCount,
	}
}

func DeleteMessage(ctx context.Context, client *sqs.Client, queueURL string, receiptHandle string) error {
	input := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	}

	_, err := client.DeleteMessage(ctx, input)
	if err != nil {
		return fmt.Errorf("Error Deleting Message: %w", err)
	}

	return nil
}

func (consumer *Consumer) DeleteMessage(ctx context.Context, receiptHandle string) error {
	return DeleteMessage(ctx, consumer.client, consumer.queueURL, receiptHandle)
}

func (consumer *Consumer) processMessage(ctx context.Context, msg *Message) {
	processCtx, cancel := context.WithTimeout(ctx, time.Duration(consumer.visibilityTimeout-5)*time.Second)
	defer cancel()

	err := consumer.handler(processCtx, msg)
	if err != nil {
		log.Printf("Error Processing Message %s: %v", msg.ID, err)
		return
	}

	if err := consumer.DeleteMessage(ctx, msg.ReceiptHandle); err != nil {
		log.Printf("Error Deleting Message %s: %v", msg.ID, err)
	}
}

func (consumer *Consumer) worker(ctx context.Context, id int, msgChan <-chan *Message) {
	for msg := range msgChan {
		select {
		case <-ctx.Done():
			return
		default:
			consumer.processMessage(ctx, msg)
		}
	}
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

func (consumer *Consumer) ReceiveMessages(ctx context.Context) ([]*Message, error) {
	return ReceiveMessages(ctx, consumer.client, consumer.queueURL, consumer.maxMessages, consumer.waitTimeSeconds, consumer.visibilityTimeout)
}

func (consumer *Consumer) poll(ctx context.Context, msgChan chan<- *Message) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			messages, err := consumer.ReceiveMessages(ctx)
			if err != nil {
				log.Printf("Error Receiving Messages: %v", err)
				time.Sleep(time.Second)
				continue
			}

			for _, msg := range messages {
				select {
				case msgChan <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (consumer *Consumer) StartConsumer(ctx context.Context) error {
	msgChan := make(chan *Message, consumer.workerCount*2)

	var wg sync.WaitGroup
	for i := 0; i < consumer.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			consumer.worker(ctx, workerID, msgChan)
		}(i)
	}

	go func() {
		consumer.poll(ctx, msgChan)
		close(msgChan)
	}()

	wg.Wait()
	return ctx.Err()
}
