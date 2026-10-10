package monitoring

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type QueueStats struct {
	ApproximateMessages           int64
	ApproximateMessagesNotVisible int64
	ApproximateMessagesDelayed    int64
}

type QueueMonitor struct {
	client *sqs.Client
}

func NewQueueMonitor(client *sqs.Client) *QueueMonitor {
	return &QueueMonitor{
		client: client,
	}
}

func (monitor *QueueMonitor) GetQueueStats(ctx context.Context, queueURL string) (*QueueStats, error) {
	input := &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		},
	}

	result, err := monitor.client.GetQueueAttributes(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("Error getting queue attributes: %w", err)
	}

	stats := &QueueStats{}

	if val, ok := result.Attributes["ApproximateNumberOfMessages"]; ok {
		stats.ApproximateMessages, _ = strconv.ParseInt(val, 10, 64)
	}

	if val, ok := result.Attributes["ApproximateNumberOfMessagesNotVisible"]; ok {
		stats.ApproximateMessagesNotVisible, _ = strconv.ParseInt(val, 10, 64)
	}

	if val, ok := result.Attributes["ApproximateNumberOfMessagesDelayed"]; ok {
		stats.ApproximateMessagesDelayed, _ = strconv.ParseInt(val, 10, 64)
	}

	return stats, nil
}

func (monitor *QueueMonitor) HealthCheck(ctx context.Context, queueURL string) error {
	_, err := monitor.GetQueueStats(ctx, queueURL)
	if err != nil {
		return fmt.Errorf("Queue is not healthy: %w", err)
	}

	return nil
}
