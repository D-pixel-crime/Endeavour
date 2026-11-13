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
	"github.com/jackc/pgx/v5"
)

type BookingIngressPayload struct {
	WorkflowID uuid.UUID `json:"workflow_id"`
	NumSeats   int       `json:"num_seats"`
	Action     string    `json:"action,omitempty"`
}

func emitBookingFailure(ctx context.Context, workflowUUID uuid.UUID, action string, reason string, payloadBytes []byte) {
	tx, err := shared.DBPool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction for recording booking failure: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	failOutbox := map[string]any{
		"workflow_id": workflowUUID.String(),
		"status":      shared.STATUS_FAILED,
		"reason":      reason,
		"updated_at":  time.Now().UTC().Format(time.RFC3339),
	}
	if action != "" {
		failOutbox["action"] = action
	}
	failBytes, _ := json.Marshal(failOutbox)

	const failQuery = `
		WITH upserted_workflow AS (
			INSERT INTO workflows (id, workflow_state, payload)
			VALUES ($1, $2, $3)
			ON CONFLICT (id) DO UPDATE
			  SET workflow_state = EXCLUDED.workflow_state,
			      payload = EXCLUDED.payload
		)
		INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
		VALUES ($4, $1, 'BOOKING', $5, NOW())`

	if _, err := tx.Exec(ctx, failQuery, workflowUUID, string(shared.BOOKING_WORKFLOW_FAILED), payloadBytes, uuid.New(), failBytes); err != nil {
		log.Printf("Failed to execute failure query for workflow %s: %v", workflowUUID, err)
		return
	}
	_ = tx.Commit(ctx)
}

func writeOutboxResponse(ctx context.Context, tx pgx.Tx, workflowUUID uuid.UUID, payload map[string]any, label string) (bool, error) {
	outboxBytes, _ := json.Marshal(payload)
	_, _ = tx.Exec(ctx,
		`INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
		 VALUES ($1, $2, 'BOOKING', $3, NOW())`,
		uuid.New(), workflowUUID, outboxBytes,
	)
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit %s booking outbox: %w", label, err)
	}
	log.Printf("[IDEMPOTENCY] Workflow %s booking is already %s; writing outbox message for duplicate delivery", workflowUUID, label)
	return true, nil
}

func handleBookingCompensation(ctx context.Context, workflowUUID uuid.UUID, payloadBytes []byte) (bool, error) {
	tx, err := shared.DBPool.Begin(ctx)
	if err != nil {
		emitBookingFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), payloadBytes)
		return true, nil
	}
	defer tx.Rollback(ctx)

	outboxPayload := map[string]any{
		"workflow_id": workflowUUID.String(),
		"status":      shared.STATUS_COMPENSATED,
		"action":      shared.ACTION_COMPENSATE,
		"updated_at":  time.Now().UTC().Format(time.RFC3339),
	}
	outboxPayloadBytes, err := json.Marshal(outboxPayload)
	if err != nil {
		emitBookingFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), payloadBytes)
		return true, nil
	}

	const compensateQuery = `
		WITH released_seats AS (
			UPDATE seat SET is_booked = false, booked_by_workflow_id = NULL
			WHERE booked_by_workflow_id = $1
		),
		upserted_workflow AS (
			INSERT INTO workflows (id, workflow_state, payload)
			VALUES ($1, $2, $3)
			ON CONFLICT (id) DO UPDATE
			  SET workflow_state = EXCLUDED.workflow_state,
			      payload = EXCLUDED.payload
		)
		INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
		VALUES ($4, $1, 'BOOKING', $5, NOW())`
	_, err = tx.Exec(ctx, compensateQuery,
		workflowUUID, string(shared.BOOKING_WORKFLOW_FAILED), payloadBytes,
		uuid.New(), outboxPayloadBytes,
	)
	if err != nil {
		tx.Rollback(ctx)
		emitBookingFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), payloadBytes)
		return true, nil
	}

	if err := tx.Commit(ctx); err != nil {
		emitBookingFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), payloadBytes)
		return true, nil
	}

	log.Printf("[BOOKING COMPENSATION] Released seats for workflow %s and sent COMPENSATED to outbox", workflowUUID)
	return true, nil
}

func ProcessBookingIngressMessage(ctx context.Context, msg *consumer.Message) (bool, error) {
	ingressPayload, payloadBytes, ok := consumer.DecodePayload[BookingIngressPayload](
		msg, "booking", func(p *BookingIngressPayload) uuid.UUID { return p.WorkflowID },
	)
	if !ok {
		return true, nil
	}

	if shared.DBPool == nil {
		return false, fmt.Errorf("database pool is not initialized")
	}

	workflowUUID := ingressPayload.WorkflowID

	// 1. Handle Compensation
	if ingressPayload.Action == shared.ACTION_COMPENSATE {
		return handleBookingCompensation(ctx, workflowUUID, payloadBytes)
	}

	// 2. Normal Booking (Worker performs single attempt; Orchestrator manages retries)
	if ingressPayload.NumSeats <= 0 {
		log.Printf("[POISON PILL DETECTED] Missing or invalid num_seats (%d) in booking message %s for workflow %s; recording failure", ingressPayload.NumSeats, msg.ID, workflowUUID)
		emitBookingFailure(ctx, workflowUUID, "", fmt.Sprintf("Invalid num_seats: %d", ingressPayload.NumSeats), payloadBytes)
		return true, nil
	}

	tx, err := shared.DBPool.Begin(ctx)
	if err != nil {
		emitBookingFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to begin transaction: %v", err), payloadBytes)
		return true, nil
	}
	defer tx.Rollback(ctx)

	var existingState string
	err = tx.QueryRow(ctx, `SELECT workflow_state FROM workflows WHERE id = $1 FOR UPDATE`, workflowUUID).Scan(&existingState)
	if err == nil {
		switch existingState {
		case string(shared.BOOKING_WORKFLOW_COMPLETED):
			var seatIDStrings []string
			seatRows, sErr := tx.Query(ctx, `SELECT id FROM seat WHERE booked_by_workflow_id = $1`, workflowUUID)
			if sErr == nil {
				for seatRows.Next() {
					var sID uuid.UUID
					if seatRows.Scan(&sID) == nil {
						seatIDStrings = append(seatIDStrings, sID.String())
					}
				}
				seatRows.Close()
			}
			return writeOutboxResponse(ctx, tx, workflowUUID, map[string]any{
				"workflow_id": workflowUUID.String(),
				"status":      shared.STATUS_COMPLETED,
				"seat_ids":    seatIDStrings,
				"num_seats":   len(seatIDStrings),
				"updated_at":  time.Now().UTC().Format(time.RFC3339),
			}, "COMPLETED")

		case string(shared.BOOKING_WORKFLOW_FAILED):
			return writeOutboxResponse(ctx, tx, workflowUUID, map[string]any{
				"workflow_id": workflowUUID.String(),
				"status":      shared.STATUS_FAILED,
				"reason":      "Booking previously failed",
				"updated_at":  time.Now().UTC().Format(time.RFC3339),
			}, "FAILED")

		case string(shared.BOOKING_WORKFLOW_COMPENSATING):
			tx.Rollback(ctx)
			log.Printf("Workflow %s is in compensating state; skipping forward execution", workflowUUID)
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
		tx.Rollback(ctx)
		emitBookingFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to query available seats: %v", err), payloadBytes)
		return true, nil
	}

	var seatIDs []uuid.UUID
	for rows.Next() {
		var seatID uuid.UUID
		if err := rows.Scan(&seatID); err != nil {
			rows.Close()
			tx.Rollback(ctx)
			emitBookingFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to scan seat id: %v", err), payloadBytes)
			return true, nil
		}
		seatIDs = append(seatIDs, seatID)
	}
	rows.Close()

	if len(seatIDs) < ingressPayload.NumSeats {
		tx.Rollback(ctx)
		emitBookingFailure(ctx, workflowUUID, "", fmt.Sprintf("insufficient seats: requested %d, available %d", ingressPayload.NumSeats, len(seatIDs)), payloadBytes)
		return true, nil
	}

	seatIDStrings := make([]string, len(seatIDs))
	for i, id := range seatIDs {
		seatIDStrings[i] = id.String()
	}

	outboxPayload := map[string]any{
		"workflow_id": workflowUUID.String(),
		"status":      shared.STATUS_COMPLETED,
		"seat_ids":    seatIDStrings,
		"num_seats":   ingressPayload.NumSeats,
		"updated_at":  time.Now().UTC().Format(time.RFC3339),
	}
	outboxPayloadBytes, err := json.Marshal(outboxPayload)
	if err != nil {
		tx.Rollback(ctx)
		emitBookingFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to marshal outbox payload: %v", err), payloadBytes)
		return true, nil
	}

	outboxID := uuid.New()
	const completeBookingQuery = `
		WITH updated_seats AS (
			UPDATE seat SET is_booked = true, booked_by_workflow_id = $1 WHERE id = ANY($2::uuid[])
		),
		upserted_workflow AS (
			INSERT INTO workflows (id, workflow_state, payload)
			VALUES ($1, $3, $4)
			ON CONFLICT (id) DO UPDATE
			  SET workflow_state = EXCLUDED.workflow_state,
			      payload = EXCLUDED.payload
		)
		INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
		VALUES ($5, $1, 'BOOKING', $6, NOW())`
	_, err = tx.Exec(ctx, completeBookingQuery,
		workflowUUID,
		seatIDStrings,
		string(shared.BOOKING_WORKFLOW_COMPLETED),
		payloadBytes,
		outboxID,
		outboxPayloadBytes,
	)
	if err != nil {
		tx.Rollback(ctx)
		emitBookingFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to complete booking and insert outbox: %v", err), payloadBytes)
		return true, nil
	}

	if err := tx.Commit(ctx); err != nil {
		emitBookingFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to commit transaction: %v", err), payloadBytes)
		return true, nil
	}

	log.Printf("Workflow %s processed: state=COMPLETED seats_booked=%d; outbox message %s created",
		workflowUUID, len(seatIDs), outboxID)
	return true, nil
}
