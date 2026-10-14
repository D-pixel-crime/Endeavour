package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/producer"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Poller struct {
	dbPool    *pgxpool.Pool
	sqsClient *sqs.Client
	router    *Router
	interval  time.Duration
	batchSize int
}

func NewPoller(dbPool *pgxpool.Pool, sqsClient *sqs.Client, router *Router, interval time.Duration, batchSize int) *Poller {
	return &Poller{
		dbPool:    dbPool,
		sqsClient: sqsClient,
		router:    router,
		interval:  interval,
		batchSize: batchSize,
	}
}

func (p *Poller) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	log.Printf("CDC Poller started (interval: %v, batch size: %d)", p.interval, p.batchSize)

	for {
		select {
		case <-ctx.Done():
			log.Println("CDC Poller is stopping...")
			return
		case <-ticker.C:
			go p.processBatch(ctx)
		}
	}
}

type queuedItem struct {
	outboxID string
	msg      *producer.Message
}

func (p *Poller) processBatch(ctx context.Context) {
	tx, err := p.dbPool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	query := fmt.Sprintf(`SELECT id, workflow_id, worker_type, payload, created_at FROM outbox ORDER BY created_at LIMIT %d FOR UPDATE SKIP LOCKED`, p.batchSize)
	rows, err := tx.Query(ctx, query)
	if err != nil {
		log.Printf("Failed to query outbox: %v", err)
		return
	}
	defer rows.Close()

	var messages []OutboxMessage
	for rows.Next() {
		var msg OutboxMessage
		if err := rows.Scan(&msg.ID, &msg.WorkflowID, &msg.WorkerType, &msg.Payload, &msg.CreatedAt); err != nil {
			log.Printf("Failed to scan outbox message: %v", err)
			continue
		}
		messages = append(messages, msg)
	}
	rows.Close()

	if len(messages) == 0 {
		return
	}

	batchesByQueue := make(map[string][]*queuedItem)
	failedIDs := make(map[string]bool)

	for _, msg := range messages {
		queueURL, err := p.router.ResolveQueue(msg.WorkerType)
		if err != nil {
			log.Printf("Failed to route message %s (worker_type: %s): %v", msg.ID, msg.WorkerType, err)
			failedIDs[msg.ID.String()] = true
			continue
		}

		var payloadMap map[string]any
		if len(msg.Payload) > 0 {
			if err := json.Unmarshal(msg.Payload, &payloadMap); err != nil {
				log.Printf("Failed to unmarshal payload for message %s: %v", msg.ID, err)
				failedIDs[msg.ID.String()] = true
				continue
			}
		}

		batchesByQueue[queueURL] = append(batchesByQueue[queueURL], &queuedItem{
			outboxID: msg.ID.String(),
			msg: &producer.Message{
				ID:      msg.ID.String(),
				Type:    msg.WorkerType,
				Payload: payloadMap,
			},
		})
	}

	for queueURL, batch := range batchesByQueue {
		producerMessages := make([]*producer.Message, len(batch))
		for i, item := range batch {
			producerMessages[i] = item.msg
		}

		res, err := producer.SendBatchMessage(ctx, p.sqsClient, queueURL, producerMessages)
		if err != nil {
			log.Printf("Failed to push batch of %d messages to SQS queue %s: %v", len(batch), queueURL, err)
			for _, item := range batch {
				failedIDs[item.outboxID] = true
			}
			continue
		}

		for _, f := range res.Failed {
			failedIDs[f.MessageID] = true
			log.Printf("Failed to send message %s to SQS %s: %s - %s", f.MessageID, queueURL, f.Code, f.Message)
		}
	}

	deletedCount := 0
	for _, msg := range messages {
		msgID := msg.ID.String()
		if failedIDs[msgID] {
			continue
		}

		_, err := tx.Exec(ctx, `DELETE FROM outbox WHERE id = $1`, msg.ID)
		if err != nil {
			log.Printf("Failed to delete outbox message %s: %v", msgID, err)
		} else {
			deletedCount++
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return
	}

	if deletedCount > 0 {
		log.Printf("Successfully processed and deleted %d/%d messages from outbox", deletedCount, len(messages))
	}
}
