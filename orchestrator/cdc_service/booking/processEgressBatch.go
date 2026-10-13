package booking

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/producer"
	cdc_shared "github.com/D-pixel-crime/Endeavour/orchestrator/cdc_service/shared"
	"github.com/D-pixel-crime/Endeavour/orchestrator/db/models"
	"github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

var (
	BookingSQSClient      *sqs.Client
	BookingEgressQueueURL string
)

func getSQSClient() *sqs.Client {
	return BookingSQSClient
}

func getQueueURL() string {
	if BookingEgressQueueURL != "" {
		return BookingEgressQueueURL
	}
	return os.Getenv(string(shared.BOOKING_WORKER) + "_INGRESS_SQS_URL")
}

func pushBatchToSQS(messages []models.OutboxMessage) (*producer.BatchSendResult, error) {
	ctx := context.Background()

	queueURL := getQueueURL()
	if queueURL == "" {
		return nil, fmt.Errorf("queue URL not configured for worker type: %s", shared.BOOKING_WORKER)
	}

	client := getSQSClient()
	if client == nil {
		return nil, fmt.Errorf("SQS client not configured for worker type: %s", shared.BOOKING_WORKER)
	}

	var producerMessages []*producer.Message
	for _, msg := range messages {
		var payloadMap map[string]any
		if len(msg.Payload) > 0 {
			if err := json.Unmarshal(msg.Payload, &payloadMap); err != nil {
				return nil, fmt.Errorf("failed to unmarshal payload for message %s: %w", msg.ID, err)
			}
		}

		producerMessages = append(producerMessages, &producer.Message{
			ID:      msg.ID.String(),
			Type:    string(msg.WorkerType),
			Payload: payloadMap,
		})
	}

	return producer.SendBatchMessage(ctx, client, queueURL, producerMessages)
}

func ProcessEgressBatch() {
	ctx := context.Background()

	if cdc_shared.DBPool == nil {
		log.Println("Database pool is not initialized")
		return
	}

	tx, err := cdc_shared.DBPool.Begin(ctx)
	if err != nil {
		log.Println("Failed to begin transaction: ", err)
		return
	}
	defer tx.Rollback(ctx)

	query := `SELECT * FROM outbox WHERE processed=false AND worker_type='BOOKING' ORDER BY created_at LIMIT 10 FOR UPDATE SKIP LOCKED`
	rows, err := tx.Query(ctx, query)
	if err != nil {
		log.Println("Failed to query outbox: ", err)
		return
	}
	defer rows.Close()

	var messages []models.OutboxMessage

	for rows.Next() {
		var msg models.OutboxMessage
		if err := rows.Scan(&msg.ID, &msg.Processed, &msg.WorkflowID, &msg.WorkerType, &msg.Payload, &msg.TimeoutSeconds, &msg.CreatedAt); err != nil {
			log.Println("Failed to scan outbox message: ", err)
			continue
		}
		messages = append(messages, msg)
	}
	rows.Close()

	if len(messages) == 0 {
		return
	}

	res, err := pushBatchToSQS(messages)
	if err != nil {
		log.Printf("Failed to push batch to SQS for worker type %s: %v", shared.BOOKING_WORKER, err)
		return
	}

	failedIDs := make(map[string]bool)
	for _, f := range res.Failed {
		failedIDs[f.MessageID] = true
		log.Printf("Failed to send message %s to SQS: %s - %s", f.MessageID, f.Code, f.Message)
	}

	for _, msgID := range res.Successful {
		_, err = tx.Exec(ctx, `UPDATE outbox SET processed = true WHERE id = $1`, msgID)
		if err != nil {
			log.Printf("Failed to update outbox message %s: %v", msgID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Println("Failed to commit transaction: ", err)
		return
	}

	log.Printf("Successfully processed batch of %d messages!", len(messages))
}
