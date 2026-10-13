package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/producer"
	"github.com/D-pixel-crime/Endeavour/booking/db/models"
)

func pushBatchToSQS(messages []models.OutboxMessage) (*producer.BatchSendResult, error) {
	ctx := context.Background()

	queueURL := egressQueueURL
	if queueURL == "" {
		queueURL = os.Getenv("BOOKING_EGRESS_SQS_URL")
	}
	if queueURL == "" {
		return nil, fmt.Errorf("BOOKING_EGRESS_SQS_URL is not configured")
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
			Type:    msg.WorkerType,
			Payload: payloadMap,
		})
	}

	return producer.SendBatchMessage(ctx, sqsClient, queueURL, producerMessages)
}

func ProcessEgressBatch() {
	ctx := context.Background()

	tx, err := cdc_booking_db_pool.Begin(ctx)
	if err != nil {
		log.Println("Failed to begin transaction: ", err)
		return
	}
	defer tx.Rollback(ctx)

	query := `SELECT id, workflow_id, worker_type, payload, created_at FROM outbox ORDER BY created_at LIMIT 10 FOR UPDATE SKIP LOCKED`
	rows, err := tx.Query(ctx, query)
	if err != nil {
		log.Println("Failed to query outbox: ", err)
		return
	}
	defer rows.Close()

	var messages []models.OutboxMessage

	for rows.Next() {
		var msg models.OutboxMessage
		if err := rows.Scan(&msg.ID, &msg.WorkflowID, &msg.WorkerType, &msg.Payload, &msg.CreatedAt); err != nil {
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
		log.Printf("Failed to push batch to SQS: %v", err)
		return
	}

	failedIDs := make(map[string]bool)
	for _, f := range res.Failed {
		failedIDs[f.MessageID] = true
		log.Printf("Failed to send message %s to SQS: %s - %s", f.MessageID, f.Code, f.Message)
	}

	for _, msg := range messages {
		msgID := msg.ID.String()
		if failedIDs[msgID] {
			continue
		}
		_, err = tx.Exec(ctx, `DELETE FROM outbox WHERE id = $1`, msg.ID)
		if err != nil {
			log.Printf("Failed to delete outbox message %s: %v", msgID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Println("Failed to commit transaction: ", err)
		return
	}

	log.Printf("Successfully processed batch of %d messages!", len(messages))
}
