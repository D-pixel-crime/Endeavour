package producer

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
)

type Producer struct {
	client   *sqs.Client
	queueURL string
}

func NewProducer(client *sqs.Client, queueURL ...string) *Producer {
	var qURL string
	if len(queueURL) > 0 {
		qURL = queueURL[0]
		if qURL != "" && !strings.Contains(qURL, ".fifo") {
			panic(fmt.Sprintf(" producer queue %s must be a FIFO queue (must contain .fifo)", qURL))
		}
	}
	return &Producer{
		client:   client,
		queueURL: qURL,
	}
}

type Message struct {
	ID              uuid.UUID      `json:"id"`
	Type            string         `json:"type"`
	Payload         map[string]any `json:"payload"`
	Timestamp       time.Time      `json:"timestamp"`
	GroupID         string         `json:"group_id,omitempty"`
	DeduplicationID string         `json:"deduplication_id,omitempty"`
}
