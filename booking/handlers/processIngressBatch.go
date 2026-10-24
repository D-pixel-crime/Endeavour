package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	"github.com/D-pixel-crime/Endeavour/booking/shared"
	"github.com/google/uuid"
)

type BookingIngressPayload struct {
	WorkflowID       uuid.UUID `json:"workflow_id"`
	NumSeats         int       `json:"num_seats"`
	Action           string    `json:"action,omitempty"`
	SeatIDs          []string  `json:"seat_ids,omitempty"`
	UpdatedAt        string    `json:"updated_at,omitempty"`
	CreatedAt        string    `json:"created_at,omitempty"`
	TimeoutParameter int       `json:"timeout_parameter,omitempty"`
	TimeoutSeconds   int       `json:"timeout_seconds,omitempty"`
}

func ProcessBookingIngressMessage(ctx context.Context, msg *consumer.Message) (bool, error) {
	if msg == nil {
		return false, fmt.Errorf("nil message received")
	}

	if msg.Payload == nil {
		return false, fmt.Errorf("message payload is nil (id=%s)", msg.ID)
	}

	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal message payload (id=%s): %w", msg.ID, err)
	}

	var ingressPayload BookingIngressPayload
	if err := json.Unmarshal(payloadBytes, &ingressPayload); err != nil {
		return false, fmt.Errorf("failed to unmarshal booking ingress payload (id=%s): %w", msg.ID, err)
	}

	if ingressPayload.WorkflowID == uuid.Nil {
		return false, fmt.Errorf("missing or invalid workflow ID in message payload (id=%s)", msg.ID)
	}

	workflowUUID := ingressPayload.WorkflowID

	if shared.DBPool == nil {
		return false, fmt.Errorf("database pool is not initialized")
	}

	timeoutParam := ingressPayload.TimeoutParameter
	if timeoutParam <= 0 {
		timeoutParam = ingressPayload.TimeoutSeconds
	}
	if timeoutParam <= 0 {
		timeoutParam = 15
	}

	// 1. Handle Compensation
	if ingressPayload.Action == shared.ACTION_COMPENSATE {
		tx, err := shared.DBPool.Begin(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx)

		if len(ingressPayload.SeatIDs) > 0 {
			_, err = tx.Exec(ctx,
				`UPDATE seat SET is_booked = false, booked_by_workflow_id = NULL
				 WHERE booked_by_workflow_id = $1 OR id = ANY($2::uuid[])`,
				workflowUUID, ingressPayload.SeatIDs,
			)
		} else {
			_, err = tx.Exec(ctx,
				`UPDATE seat SET is_booked = false, booked_by_workflow_id = NULL
				 WHERE booked_by_workflow_id = $1`,
				workflowUUID,
			)
		}
		if err != nil {
			return false, fmt.Errorf("failed to release seats during compensation for workflow %s: %w", workflowUUID, err)
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO workflows (id, workflow_state, payload)
			 VALUES ($1, $2, $3)
			 ON CONFLICT (id) DO UPDATE
			   SET workflow_state = EXCLUDED.workflow_state,
			       payload = EXCLUDED.payload`,
			workflowUUID, string(shared.BOOKING_WORKFLOW_FAILED), payloadBytes,
		)
		if err != nil {
			return false, fmt.Errorf("failed to update workflow state during compensation: %w", err)
		}

		outboxPayload := map[string]any{
			"workflow_id":       workflowUUID.String(),
			"status":            shared.STATUS_COMPENSATED,
			"action":            shared.ACTION_COMPENSATE,
			"updated_at":        time.Now().UTC().Format(time.RFC3339),
			"timeout_parameter": timeoutParam,
			"timeout_seconds":   timeoutParam,
		}
		outboxPayloadBytes, err := json.Marshal(outboxPayload)
		if err != nil {
			return false, fmt.Errorf("failed to marshal compensation outbox payload: %w", err)
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
			 VALUES ($1, $2, 'BOOKING', $3, NOW())`,
			uuid.New(), workflowUUID, outboxPayloadBytes,
		)
		if err != nil {
			return false, fmt.Errorf("failed to insert compensation outbox entry: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("failed to commit compensation transaction: %w", err)
		}

		log.Printf("[BOOKING COMPENSATION] Released seats for workflow %s and sent COMPENSATED to outbox", workflowUUID)
		return true, nil
	}

	// 2. Normal Booking with 2 retries on failure
	if ingressPayload.NumSeats <= 0 {
		if rawNumSeats, ok := msg.Payload["num_seats"]; ok {
			if numSeatsFloat, ok := rawNumSeats.(float64); ok && numSeatsFloat > 0 {
				ingressPayload.NumSeats = int(numSeatsFloat)
			}
		}
	}
	if ingressPayload.NumSeats <= 0 {
		return false, fmt.Errorf("missing or invalid num_seats in message payload (id=%s)", msg.ID)
	}

	const maxRetries = 2
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
		}

		tx, err := shared.DBPool.Begin(ctx)
		if err != nil {
			lastErr = fmt.Errorf("failed to begin transaction: %w", err)
			continue
		}

		var existingState string
		err = tx.QueryRow(ctx, `SELECT workflow_state FROM workflows WHERE id = $1`, workflowUUID).Scan(&existingState)
		if err == nil && existingState == string(shared.BOOKING_WORKFLOW_COMPLETED) {
			tx.Rollback(ctx)
			log.Printf("Workflow %s is already COMPLETED; skipping duplicate processing", workflowUUID)
			return true, nil
		}

		rows, err := tx.Query(ctx,
			`SELECT id FROM seat
			 WHERE is_booked = false
			 ORDER BY seat_number
			 LIMIT $1
			 FOR UPDATE SKIP LOCKED`,
			ingressPayload.NumSeats,
		)
		if err != nil {
			tx.Rollback(ctx)
			lastErr = fmt.Errorf("failed to query available seats: %w", err)
			continue
		}

		var seatIDs []uuid.UUID
		for rows.Next() {
			var seatID uuid.UUID
			if err := rows.Scan(&seatID); err != nil {
				rows.Close()
				tx.Rollback(ctx)
				lastErr = fmt.Errorf("failed to scan seat id: %w", err)
				break
			}
			seatIDs = append(seatIDs, seatID)
		}
		rows.Close()

		if lastErr != nil {
			continue
		}

		if len(seatIDs) < ingressPayload.NumSeats {
			tx.Rollback(ctx)
			lastErr = fmt.Errorf("insufficient seats: requested %d, available %d", ingressPayload.NumSeats, len(seatIDs))
			continue
		}

		seatIDStrings := make([]string, len(seatIDs))
		for i, id := range seatIDs {
			seatIDStrings[i] = id.String()
		}

		_, err = tx.Exec(ctx,
			`UPDATE seat SET is_booked = true, booked_by_workflow_id = $1 WHERE id = ANY($2::uuid[])`,
			workflowUUID, seatIDStrings,
		)
		if err != nil {
			tx.Rollback(ctx)
			lastErr = fmt.Errorf("failed to update seats: %w", err)
			continue
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO workflows (id, workflow_state, payload)
			 VALUES ($1, $2, $3)
			 ON CONFLICT (id) DO UPDATE
			   SET workflow_state = EXCLUDED.workflow_state,
			       payload = EXCLUDED.payload`,
			workflowUUID, string(shared.BOOKING_WORKFLOW_COMPLETED), payloadBytes,
		)
		if err != nil {
			tx.Rollback(ctx)
			lastErr = fmt.Errorf("failed to upsert workflow: %w", err)
			continue
		}

		outboxPayload := map[string]any{
			"workflow_id":       workflowUUID.String(),
			"status":            shared.STATUS_COMPLETED,
			"seat_ids":          seatIDStrings,
			"num_seats":         ingressPayload.NumSeats,
			"updated_at":        time.Now().UTC().Format(time.RFC3339),
			"timeout_parameter": timeoutParam,
			"timeout_seconds":   timeoutParam,
		}
		outboxPayloadBytes, err := json.Marshal(outboxPayload)
		if err != nil {
			tx.Rollback(ctx)
			lastErr = fmt.Errorf("failed to marshal outbox payload: %w", err)
			continue
		}

		outboxID := uuid.New()
		_, err = tx.Exec(ctx,
			`INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
			 VALUES ($1, $2, 'BOOKING', $3, NOW())`,
			outboxID, workflowUUID, outboxPayloadBytes,
		)
		if err != nil {
			tx.Rollback(ctx)
			lastErr = fmt.Errorf("failed to insert outbox entry: %w", err)
			continue
		}

		if err := tx.Commit(ctx); err != nil {
			lastErr = fmt.Errorf("failed to commit transaction: %w", err)
			continue
		}

		log.Printf("Workflow %s processed: state=COMPLETED seats_booked=%d; outbox message %s created",
			workflowUUID, len(seatIDs), outboxID)
		return true, nil
	}

	// 3. Retries exhausted: Record FAILED in DB, emit FAILED outbox message, acknowledge from SQS
	tx, err := shared.DBPool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction for recording failure: %w", err)
	}
	defer tx.Rollback(ctx)

	_, _ = tx.Exec(ctx,
		`INSERT INTO workflows (id, workflow_state, payload)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO UPDATE
		   SET workflow_state = EXCLUDED.workflow_state,
		       payload = EXCLUDED.payload`,
		workflowUUID, string(shared.BOOKING_WORKFLOW_FAILED), payloadBytes,
	)

	failOutbox := map[string]any{
		"workflow_id":       workflowUUID.String(),
		"status":            shared.STATUS_FAILED,
		"reason":            lastErr.Error(),
		"updated_at":        time.Now().UTC().Format(time.RFC3339),
		"timeout_parameter": timeoutParam,
		"timeout_seconds":   timeoutParam,
	}
	failBytes, _ := json.Marshal(failOutbox)

	_, _ = tx.Exec(ctx,
		`INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
		 VALUES ($1, $2, 'BOOKING', $3, NOW())`,
		uuid.New(), workflowUUID, failBytes,
	)

	_ = tx.Commit(ctx)
	log.Printf("Workflow %s booking failed after %d retries: %v; sent FAILED outbox message", workflowUUID, maxRetries, lastErr)
	return true, nil
}

func ProcessIngressBatch(ctx context.Context, messages []*consumer.Message) {
	var toDelete []*consumer.Message

	for _, msg := range messages {
		if msg == nil {
			continue
		}

		shouldDelete, err := ProcessBookingIngressMessage(ctx, msg)
		if err != nil {
			log.Printf("Error processing message %s: %v", msg.ID, err)
			continue
		}

		if shouldDelete {
			toDelete = append(toDelete, msg)
		}
	}

	if len(toDelete) > 0 {
		consumer.DeleteAllBatches(ctx, shared.IngressSQSClient, shared.IngressQueueURL, toDelete)
	}
}
