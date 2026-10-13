package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	"github.com/D-pixel-crime/Endeavour/booking/shared"
	"github.com/google/uuid"
)

func processMessage(ctx context.Context, msg *consumer.Message) error {
	numSeatsRaw, ok := msg.Payload["num_seats"]
	if !ok {
		return fmt.Errorf("missing num_seats in message payload (id=%s)", msg.ID)
	}

	numSeatsFloat, ok := numSeatsRaw.(float64)
	if !ok || numSeatsFloat <= 0 {
		return fmt.Errorf("invalid num_seats value %v in message (id=%s)", numSeatsRaw, msg.ID)
	}
	numSeats := int(numSeatsFloat)

	workflowID, err := uuid.Parse(msg.ID)
	if err != nil {
		return fmt.Errorf("message ID is not a valid UUID (id=%s): %w", msg.ID, err)
	}

	tx, err := shared.DBPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx,
		`SELECT id FROM seat
		 WHERE is_booked = false
		 ORDER BY seat_number
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		numSeats,
	)
	if err != nil {
		return fmt.Errorf("failed to query available seats: %w", err)
	}

	var seatIDs []uuid.UUID
	for rows.Next() {
		var seatID uuid.UUID
		if err := rows.Scan(&seatID); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan seat id: %w", err)
		}
		seatIDs = append(seatIDs, seatID)
	}
	rows.Close()

	workflowState := shared.BOOKING_WORKFLOW_COMPLETED
	var outboxPayload map[string]any

	if len(seatIDs) < numSeats {
		log.Printf("insufficient seats for workflow %s: requested=%d available=%d",
			workflowID, numSeats, len(seatIDs))
		workflowState = shared.BOOKING_WORKFLOW_FAILED
		outboxPayload = map[string]any{
			"workflow_id": workflowID.String(),
			"status":      "FAILED",
			"reason":      fmt.Sprintf("insufficient seats: requested %d, available %d", numSeats, len(seatIDs)),
		}
	} else {
		seatIDStrings := make([]string, len(seatIDs))
		for i, id := range seatIDs {
			seatIDStrings[i] = id.String()
		}

		_, err = tx.Exec(ctx,
			`UPDATE seat SET is_booked = true WHERE id = ANY($1::uuid[])`,
			seatIDStrings,
		)
		if err != nil {
			return fmt.Errorf("failed to update seats: %w", err)
		}

		outboxPayload = map[string]any{
			"workflow_id": workflowID.String(),
			"status":      "COMPLETED",
			"seat_ids":    seatIDStrings,
			"num_seats":   numSeats,
		}
	}

	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal workflow payload: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO workflows (id, workflow_state, payload)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO UPDATE
		   SET workflow_state = EXCLUDED.workflow_state`,
		workflowID, string(workflowState), payloadBytes,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert workflow: %w", err)
	}

	outboxID := uuid.New()
	outboxPayloadBytes, err := json.Marshal(outboxPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal outbox payload: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (id, workflow_id, worker_type, payload)
		 VALUES ($1, $2, 'BOOKING', $3)`,
		outboxID, workflowID, outboxPayloadBytes,
	)
	if err != nil {
		return fmt.Errorf("failed to insert outbox entry: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("workflow %s processed: state=%s seats_booked=%d",
		workflowID, workflowState, len(seatIDs))

	return nil
}

func deleteFromIngress(ctx context.Context, msg *consumer.Message) {
	if err := consumer.DeleteMessage(ctx, shared.IngressSQSClient, shared.IngressQueueURL, msg.ReceiptHandle); err != nil {
		log.Printf("failed to delete SQS message %s: %v", msg.ID, err)
	}
}

func ProcessIngressBatch(ctx context.Context, messages []*consumer.Message) {
	for _, msg := range messages {
		if msg == nil {
			continue
		}

		err := processMessage(ctx, msg)
		if err != nil {
			log.Printf("error processing message %s: %v", msg.ID, err)
			continue
		}

		deleteFromIngress(ctx, msg)
	}
}
