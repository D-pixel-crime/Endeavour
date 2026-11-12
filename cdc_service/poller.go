package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/producer"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Poller struct {
	dbPool            *pgxpool.Pool
	producer          *producer.Producer
	router            *Router
	interval          time.Duration
	batchSize         int
	maxConcurrency    int
	visibilityTimeout int
}

func NewPoller(dbPool *pgxpool.Pool, prod *producer.Producer, router *Router, interval time.Duration, batchSize int, visibilityTimeout int, maxConcurrency ...int) *Poller {
	limit := 5
	if len(maxConcurrency) > 0 && maxConcurrency[0] > 0 {
		limit = maxConcurrency[0]
	}
	return &Poller{
		dbPool:            dbPool,
		producer:          prod,
		router:            router,
		interval:          interval,
		batchSize:         batchSize,
		maxConcurrency:    limit,
		visibilityTimeout: visibilityTimeout,
	}
}

func (p *Poller) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	sem := make(chan struct{}, p.maxConcurrency)
	var workerWg sync.WaitGroup

	log.Printf("CDC Poller started (interval: %v, batch size: %d, maxConcurrency: %d)", p.interval, p.batchSize, p.maxConcurrency)

	for {
		select {
		case <-ctx.Done():
			log.Println("CDC Poller is stopping, waiting for in-flight tasks...")
			workerWg.Wait()
			log.Println("CDC Poller stopped cleanly")
			return
		case <-ticker.C:
			select {
			case sem <- struct{}{}:
			default:
				// Concurrency limit reached; skip this tick
				continue
			}

			workerWg.Add(1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Recovered from panic in CDC poller worker: %v", r)
					}
					<-sem
					workerWg.Done()
				}()
				p.processBatch(ctx)
			}()
		}
	}
}

type queuedItem struct {
	outboxID uuid.UUID
	msg      *producer.Message
}

// PrepareMessagePayload enriches and validates the outbox payload, ensuring
// updated_at and timeout_seconds are always attached to the workflow task.
func (p *Poller) PrepareMessagePayload(msg OutboxMessage) (map[string]any, time.Time, error) {
	var payloadMap map[string]any
	if len(msg.Payload) > 0 {
		if err := json.Unmarshal(msg.Payload, &payloadMap); err != nil {
			return nil, time.Time{}, fmt.Errorf("failed to unmarshal payload: %w", err)
		}
	}
	if payloadMap == nil {
		payloadMap = make(map[string]any)
	}

	if _, ok := payloadMap["workflow_id"]; !ok && msg.WorkflowID != uuid.Nil {
		payloadMap["workflow_id"] = msg.WorkflowID.String()
	}

	msgTimestamp := msg.UpdatedAt.UTC()
	if msgTimestamp.IsZero() {
		msgTimestamp = time.Now().UTC()
	}
	if _, ok := payloadMap["updated_at"]; !ok {
		payloadMap["updated_at"] = msgTimestamp.Format(time.RFC3339)
	}

	timeoutSec := msg.TimeoutSeconds
	if timeoutSec <= 0 {
		if ts, ok := payloadMap["timeout_seconds"].(float64); ok && ts > 0 {
			timeoutSec = int(ts)
		} else if tp, ok := payloadMap["timeout_parameter"].(float64); ok && tp > 0 {
			timeoutSec = int(tp)
		} else if p.visibilityTimeout > 0 {
			timeoutSec = p.visibilityTimeout
		} else {
			timeoutSec = 15
		}
	}
	payloadMap["timeout_seconds"] = timeoutSec

	return payloadMap, msgTimestamp, nil
}

func (p *Poller) processBatch(ctx context.Context) {
	batchCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()

	tx, err := p.dbPool.Begin(batchCtx)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(batchCtx)

	const claimQuery = `
		WITH targets AS (
			SELECT id
			FROM outbox
			WHERE (status IS NULL OR status != 'FAILED')
			  AND (processing = false OR (processing = true AND updated_at < NOW() - INTERVAL '30 seconds'))
			  AND (retry_count < 5)
			ORDER BY updated_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox
		SET processing = true, updated_at = NOW()
		WHERE id IN (SELECT id FROM targets)
		RETURNING outbox.id, outbox.workflow_id, outbox.worker_type, outbox.payload, outbox.timeout_seconds, outbox.updated_at`

	rows, err := tx.Query(batchCtx, claimQuery, p.batchSize)
	if err != nil {
		log.Printf("Failed to query outbox: %v", err)
		return
	}
	defer rows.Close()

	failedIDs := make(map[uuid.UUID]bool)
	var messages []OutboxMessage

	for rows.Next() {
		var msg OutboxMessage
		if err := rows.Scan(&msg.ID, &msg.WorkflowID, &msg.WorkerType, &msg.Payload, &msg.TimeoutSeconds, &msg.UpdatedAt); err != nil {
			log.Printf("Failed to scan outbox message: %v", err)
			failedIDs[msg.ID] = true
			continue
		}
		messages = append(messages, msg)
	}
	rows.Close()

	if err := tx.Commit(batchCtx); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return
	}

	if len(messages) == 0 {
		return
	}

	batchesByQueue := make(map[string][]*queuedItem)

	for _, msg := range messages {
		queueURL, err := p.router.ResolveQueue(msg.WorkerType)
		if err != nil {
			log.Printf("Failed to route message %s (worker_type: %s): %v", msg.ID, msg.WorkerType, err)
			failedIDs[msg.ID] = true
			continue
		}

		if !strings.Contains(queueURL, ".fifo") {
			log.Printf("Skipping message %s (worker_type: %s): target queue '%s' is not a FIFO queue (must contain .fifo)", msg.ID, msg.WorkerType, queueURL)
			failedIDs[msg.ID] = true
			continue
		}

		payloadMap, msgTimestamp, err := p.PrepareMessagePayload(msg)
		if err != nil {
			log.Printf("Failed to prepare payload for message %s: %v", msg.ID, err)
			failedIDs[msg.ID] = true
			continue
		}

		workflowUUID := msg.WorkflowID
		if workflowUUID == uuid.Nil {
			if wfIDStr, ok := payloadMap["workflow_id"].(string); ok && wfIDStr != "" {
				if parsed, err := uuid.Parse(wfIDStr); err == nil {
					workflowUUID = parsed
				}
			}
		}
		if workflowUUID == uuid.Nil {
			log.Printf("message %s (worker_type: %s) has no valid workflow_id; cannot determine MessageGroupId", msg.ID, msg.WorkerType)
			failedIDs[msg.ID] = true
			continue
		}

		batchesByQueue[queueURL] = append(batchesByQueue[queueURL], &queuedItem{
			outboxID: msg.ID,
			msg: &producer.Message{
				ID:              msg.ID,
				Type:            msg.WorkerType,
				Payload:         payloadMap,
				Timestamp:       msgTimestamp,
				GroupID:         workflowUUID.String(),
				DeduplicationID: msg.ID.String(),
			},
		})
	}

	for queueURL, batch := range batchesByQueue {
		producerMessages := make([]*producer.Message, len(batch))
		for i, item := range batch {
			producerMessages[i] = item.msg
		}

		res, err := p.producer.SendBatch(batchCtx, queueURL, producerMessages)
		if err != nil {
			log.Printf("Failed to push batch of %d messages to SQS queue %s: %v", len(batch), queueURL, err)
			for _, item := range batch {
				failedIDs[item.outboxID] = true
			}
			continue
		}

		for _, f := range res.Failed {
			if failedUUID, err := uuid.Parse(f.MessageID); err == nil {
				failedIDs[failedUUID] = true
			}
			log.Printf("Failed to send message %s to SQS %s: %s - %s", f.MessageID, queueURL, f.Code, f.Message)
		}
	}

	tx, err = p.dbPool.Begin(batchCtx)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(batchCtx)

	var toUpdate, toDelete []uuid.UUID
	for _, msg := range messages {
		if failedIDs[msg.ID] {
			toUpdate = append(toUpdate, msg.ID)
		} else {
			toDelete = append(toDelete, msg.ID)
		}
	}

	if len(toUpdate) > 0 || len(toDelete) > 0 {
		const finalizeQuery = `
			WITH deletes AS (
				DELETE FROM outbox
				WHERE id = ANY($1::uuid[])
				RETURNING id
			)
			UPDATE outbox
			SET processing = false,
			    retry_count = retry_count + 1,
			    status = CASE WHEN retry_count + 1 >= 5 THEN 'FAILED' ELSE status END,
			    updated_at = NOW()
			WHERE id = ANY($2::uuid[])`

		if _, err := tx.Exec(batchCtx, finalizeQuery, toDelete, toUpdate); err != nil {
			log.Printf("Failed to update outbox messages: %v", err)
			return
		}
	}

	if err := tx.Commit(batchCtx); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return
	}

	if len(toDelete) > 0 {
		log.Printf("Successfully processed and deleted %d/%d messages from outbox", len(toDelete), len(messages))
	}
}
