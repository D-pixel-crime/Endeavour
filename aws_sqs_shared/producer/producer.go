package producer

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Producer struct {
	client   *sqs.Client
	queueURL string
}

func NewProducer(client *sqs.Client, queueURL ...string) *Producer {
	var qURL string
	if len(queueURL) > 0 {
		qURL = queueURL[0]
	}
	return &Producer{
		client:   client,
		queueURL: qURL,
	}
}

type Message struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	Timestamp time.Time      `json:"timestamp"`
}
