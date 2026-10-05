package main

import (
	"context"
	"log"

	"github.com/D-pixel-crime/Endeavour/orchestrator/db/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

func pushToSQS(msg models.OutboxMessage) error {
	return nil
}

func ProcessBatch(cdc_orchestrator_db_pool *pgxpool.Pool) {
	ctx := context.Background()

	tx, err := cdc_orchestrator_db_pool.Begin(ctx)
	if err != nil {
		log.Println("Failed to begin transaction: ", err)
		return
	}
	defer tx.Rollback(ctx)

	query := `SELECT * FROM outbox WHERE processed=false ORDER BY created_at LIMIT 10 FOR UPDATE SKIP LOCKED`
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

	for _, msg := range messages {
		err := pushToSQS(msg)
		if err != nil {
			log.Println("Failed to push to SQS: ", err)
			continue
		}

		_, err = tx.Exec(ctx, `UPDATE outbox SET processed = true WHERE id = $1`, msg.ID)
		if err != nil {
			log.Println("Failed to update outbox message: ", err)
			continue
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Println("Failed to commit transaction: ", err)
		return
	}

	log.Printf("Successfully processed batch of %d messages!", len(messages))
}
