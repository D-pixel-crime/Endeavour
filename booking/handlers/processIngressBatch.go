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

type BookingIngressPayload struct {
	WorkflowID       string `json:"workflow_id"`
	NumSeats         int    `json:"num_seats"`
	CreatedAt        string `json:"created_at,omitempty"`
	TimeoutParameter int    `json:"timeout_parameter,omitempty"`
	TimeoutSeconds   int    `json:"timeout_seconds,omitempty"`
}

func deleteBatchFromIngress(ctx context.Context, messages []*consumer.Message) {
	consumer.DeleteAllBatches(ctx, shared.IngressSQSClient, shared.IngressQueueURL, messages)
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

	if ingressPayload.WorkflowID == "" && msg.ID != "" {
		ingressPayload.WorkflowID = msg.ID
	}

	workflowUUID, err := uuid.Parse(ingressPayload.WorkflowID)
	if err != nil {
		return false, fmt.Errorf("invalid workflow ID '%s' in message (id=%s): %w", ingressPayload.WorkflowID, msg.ID, err)
	}

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

	if shared.DBPool == nil {
		return false, fmt.Errorf("database pool is not initialized")
	}

	tx, err := shared.DBPool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var existingState string
	err = tx.QueryRow(ctx, `SELECT workflow_state FROM workflows WHERE id = $1 FOR UPDATE`, workflowUUID).Scan(&existingState)
	if err == nil {
		if existingState == string(shared.BOOKING_WORKFLOW_COMPLETED) {
			log.Printf("Workflow %s is already in state %s; skipping duplicate processing", workflowUUID, existingState)
			return true, nil
		}
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
		return false, fmt.Errorf("failed to query available seats: %w", err)
	}

	var seatIDs []uuid.UUID
	for rows.Next() {
		var seatID uuid.UUID
		if err := rows.Scan(&seatID); err != nil {
			rows.Close()
			return false, fmt.Errorf("failed to scan seat id: %w", err)
		}
		seatIDs = append(seatIDs, seatID)
	}
	rows.Close()

	workflowState := shared.BOOKING_WORKFLOW_COMPLETED
	var outboxPayload map[string]any

	timeoutParam := ingressPayload.TimeoutParameter
	if timeoutParam <= 0 {
		timeoutParam = ingressPayload.TimeoutSeconds
	}
	if timeoutParam <= 0 {
		timeoutParam = 15
	}

	if len(seatIDs) < ingressPayload.NumSeats {
		log.Printf("insufficient seats for workflow %s: requested=%d available=%d",
			workflowUUID, ingressPayload.NumSeats, len(seatIDs))
		workflowState = shared.BOOKING_WORKFLOW_FAILED
		outboxPayload = map[string]any{
			"workflow_id":       workflowUUID.String(),
			"status":            "FAILED",
			"reason":            fmt.Sprintf("insufficient seats: requested %d, available %d", ingressPayload.NumSeats, len(seatIDs)),
			"timeout_parameter": timeoutParam,
			"timeout_seconds":   timeoutParam,
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
			return false, fmt.Errorf("failed to update seats: %w", err)
		}

		outboxPayload = map[string]any{
			"workflow_id":       workflowUUID.String(),
			"status":            "COMPLETED",
			"seat_ids":          seatIDStrings,
			"num_seats":         ingressPayload.NumSeats,
			"timeout_parameter": timeoutParam,
			"timeout_seconds":   timeoutParam,
		}
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO workflows (id, workflow_state, payload)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO UPDATE
		   SET workflow_state = EXCLUDED.workflow_state,
		       payload = EXCLUDED.payload`,
		workflowUUID, string(workflowState), payloadBytes,
	)
	if err != nil {
		return false, fmt.Errorf("failed to upsert workflow: %w", err)
	}

	outboxID := uuid.New()
	outboxPayloadBytes, err := json.Marshal(outboxPayload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal outbox payload: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (id, workflow_id, worker_type, payload, created_at)
		 VALUES ($1, $2, 'BOOKING', $3, NOW())`,
		outboxID, workflowUUID, outboxPayloadBytes,
	)
	if err != nil {
		return false, fmt.Errorf("failed to insert outbox entry: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Workflow %s processed: state=%s seats_booked=%d; outbox message %s created",
		workflowUUID, workflowState, len(seatIDs), outboxID)
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
		deleteBatchFromIngress(ctx, toDelete)
	}
}
