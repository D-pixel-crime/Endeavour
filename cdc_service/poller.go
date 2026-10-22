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
	checkColOnce      sync.Once
	hasTimeoutCol     bool
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

func (p *Poller) checkTimeoutColumn(ctx context.Context) {
	p.checkColOnce.Do(func() {
		var exists bool
		err := p.dbPool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns 
				WHERE table_name = 'outbox' AND column_name = 'timeout_seconds'
			)
		`).Scan(&exists)
		if err == nil {
			p.hasTimeoutCol = exists
			if exists {
				log.Println("CDC Poller detected 'timeout_seconds' column in outbox table")
			} else {
				log.Println("CDC Poller: 'timeout_seconds' column not detected in outbox table, using fallback")
			}
		} else {
			log.Printf("Warning: failed to check for timeout_seconds column in outbox: %v", err)
		}
	})
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
	outboxID string
	msg      *producer.Message
}

// PrepareMessagePayload enriches and validates the outbox payload, ensuring
// updated_at and timeout_parameter/timeout_seconds are always attached to the workflow task.
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

	// Ensure updated_at is present in the task payload
	updatedAtStr := msg.UpdatedAt.UTC().Format(time.RFC3339)
	if msg.UpdatedAt.IsZero() {
		updatedAtStr = time.Now().UTC().Format(time.RFC3339)
	}
	if _, ok := payloadMap["updated_at"]; !ok {
		payloadMap["updated_at"] = updatedAtStr
	}
	// Also ensure created_at is present for backwards compatibility
	if _, ok := payloadMap["created_at"]; !ok {
		payloadMap["created_at"] = updatedAtStr
	}

	// Ensure timeout_parameter and timeout_seconds are present in the task payload
	timeoutSec := msg.TimeoutSeconds
	if timeoutSec <= 0 {
		if tp, ok := payloadMap["timeout_parameter"].(float64); ok && tp > 0 {
			timeoutSec = int(tp)
		} else if ts, ok := payloadMap["timeout_seconds"].(float64); ok && ts > 0 {
			timeoutSec = int(ts)
		} else if p.visibilityTimeout > 0 {
			timeoutSec = p.visibilityTimeout
		} else {
			timeoutSec = 15 // default timeout parameter in seconds
		}
	}
	payloadMap["timeout_parameter"] = timeoutSec
	payloadMap["timeout_seconds"] = timeoutSec

	msgTimestamp := msg.UpdatedAt.UTC()
	if msgTimestamp.IsZero() {
		msgTimestamp = time.Now().UTC()
	}

	return payloadMap, msgTimestamp, nil
}

func (p *Poller) processBatch(ctx context.Context) {
	tx, err := p.dbPool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	p.checkTimeoutColumn(ctx)

	var query string
	if p.hasTimeoutCol {
		query = `
				WITH targets AS (
					SELECT id FROM outbox WHERE processing = false OR (processing = true AND updated_at < NOW() - INTERVAL '2 minutes') ORDER BY updated_at LIMIT $1 FOR UPDATE SKIP LOCKED
				) 
				UPDATE outbox SET processing = true, updated_at = NOW()
				WHERE id IN (SELECT id FROM targets)
				RETURNING outbox.id, outbox.workflow_id, outbox.worker_type, outbox.payload, outbox.timeout_seconds, outbox.updated_at`
	} else {
		query = `
				WITH targets AS (
					SELECT id FROM outbox WHERE processing = false OR (processing = true AND updated_at < NOW() - INTERVAL '2 minutes') ORDER BY updated_at LIMIT $1 FOR UPDATE SKIP LOCKED
				) 
				UPDATE outbox SET processing = true, updated_at = NOW()
				WHERE id IN (SELECT id FROM targets)
				RETURNING outbox.id, outbox.workflow_id, outbox.worker_type, outbox.payload, outbox.updated_at`
	}

	rows, err := tx.Query(ctx, query, p.batchSize)
	if err != nil {
		log.Printf("Failed to query outbox: %v", err)
		return
	}
	defer rows.Close()

	var failedIDs = make(map[string]bool)

	var messages []OutboxMessage
	for rows.Next() {
		var msg OutboxMessage
		if p.hasTimeoutCol {
			if err := rows.Scan(&msg.ID, &msg.WorkflowID, &msg.WorkerType, &msg.Payload, &msg.TimeoutSeconds, &msg.UpdatedAt); err != nil {
				log.Printf("Failed to scan outbox message: %v", err)
				failedIDs[msg.ID.String()] = true
				continue
			}
		} else {
			if err := rows.Scan(&msg.ID, &msg.WorkflowID, &msg.WorkerType, &msg.Payload, &msg.UpdatedAt); err != nil {
				log.Printf("Failed to scan outbox message: %v", err)
				failedIDs[msg.ID.String()] = true
				continue
			}
		}
		messages = append(messages, msg)
	}
	rows.Close()

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return // Rolls back here
	}

	if len(messages) == 0 {
		return
	}

	batchesByQueue := make(map[string][]*queuedItem)

	for _, msg := range messages {
		queueURL, err := p.router.ResolveQueue(msg.WorkerType)
		if err != nil {
			log.Printf("Failed to route message %s (worker_type: %s): %v", msg.ID, msg.WorkerType, err)
			failedIDs[msg.ID.String()] = true
			continue
		}

		if !strings.Contains(queueURL, ".fifo") {
			log.Printf("Skipping message %s (worker_type: %s): target queue '%s' is not a FIFO queue (must contain .fifo)", msg.ID, msg.WorkerType, queueURL)
			failedIDs[msg.ID.String()] = true
			continue
		}

		payloadMap, msgTimestamp, err := p.PrepareMessagePayload(msg)
		if err != nil {
			log.Printf("Failed to prepare payload for message %s: %v", msg.ID, err)
			failedIDs[msg.ID.String()] = true
			continue
		}

		groupID := ""
		if msg.WorkflowID != uuid.Nil {
			groupID = msg.WorkflowID.String()
		} else if wfID, ok := payloadMap["workflow_id"].(string); ok && wfID != "" {
			groupID = wfID
		}
		if groupID == "" {
			log.Printf("message %s (worker_type: %s) has no workflow_id; cannot determine MessageGroupId", msg.ID, msg.WorkerType)
			failedIDs[msg.ID.String()] = true
			continue
		}

		batchesByQueue[queueURL] = append(batchesByQueue[queueURL], &queuedItem{
			outboxID: msg.ID.String(),
			msg: &producer.Message{
				ID:              msg.ID.String(),
				Type:            msg.WorkerType,
				Payload:         payloadMap,
				Timestamp:       msgTimestamp,
				GroupID:         groupID,
				DeduplicationID: msg.ID.String(),
			},
		})
	}

	for queueURL, batch := range batchesByQueue {
		producerMessages := make([]*producer.Message, len(batch))
		for i, item := range batch {
			producerMessages[i] = item.msg
		}

		res, err := p.producer.SendBatch(ctx, queueURL, producerMessages)
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

	tx, err = p.dbPool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	deletedCount := 0
	for _, msg := range messages {
		msgID := msg.ID.String()
		if failedIDs[msgID] {
			_, err := tx.Exec(ctx, `UPDATE outbox SET processing = false, updated_at = NOW() WHERE id = $1`, msgID)
			if err != nil {
				log.Printf("Failed to mark outbox message %s as not processing: %v", msgID, err)
			}
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
