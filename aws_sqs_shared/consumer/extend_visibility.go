package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type VisibilityExtender struct {
	client   *sqs.Client
	queueURL string
}

func NewVisibilityExtender(client *sqs.Client, queueURL string) *VisibilityExtender {
	return &VisibilityExtender{
		client:   client,
		queueURL: queueURL,
	}
}

func (extender *VisibilityExtender) ExtendVisibility(ctx context.Context, receiptHandle string, visibilityTimeout int32) error {
	input := &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(extender.queueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: visibilityTimeout,
	}

	_, err := extender.client.ChangeMessageVisibility(ctx, input)
	if err != nil {
		return fmt.Errorf("Error changing message visibility: %w", err)
	}

	return nil
}

func (extender *VisibilityExtender) StartVisibilityHeartBeat(ctx context.Context, receiptHandle string, interval time.Duration, visibilityTimeout int32) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if err := extender.ExtendVisibility(ctx, receiptHandle, visibilityTimeout); err != nil {
				log.Printf("Failed to Extend Message Visibility: %v", err)
				return
			}
		}
	}
}
