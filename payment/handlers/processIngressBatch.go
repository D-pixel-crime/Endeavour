package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	"github.com/D-pixel-crime/Endeavour/payment/shared"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type CustomerDetails struct {
	Email         string `json:"email,omitempty"`
	FullName      string `json:"full_name,omitempty"`
	Contact       any    `json:"contact,omitempty"`
	PaymentMethod string `json:"payment_method,omitempty"`
}

type PaymentIngressPayload struct {
	WorkflowID      uuid.UUID        `json:"workflow_id"`
	Action          string           `json:"action,omitempty"`
	Amount          int              `json:"amount"`
	NumSeats        int              `json:"num_seats"`
	SeatIDs         []string         `json:"seat_ids"`
	PaymentMethod   string           `json:"payment_method"`
	CustomerDetails *CustomerDetails `json:"customer_details,omitempty"`
}

func emitPaymentFailure(ctx context.Context, workflowUUID uuid.UUID, action string, reason string, seatIDs []string, numSeats int, payloadBytes []byte) {
	tx, err := shared.DBPool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction for recording payment failure: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	failOutbox := map[string]any{
		"workflow_id": workflowUUID.String(),
		"status":      shared.STATUS_FAILED,
		"reason":      reason,
		"seat_ids":    seatIDs,
		"num_seats":   numSeats,
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
		VALUES ($4, $1, 'PAYMENT', $5, NOW())`

	if _, err := tx.Exec(ctx, failQuery, workflowUUID, string(shared.PAYMENT_WORKFLOW_FAILED), payloadBytes, uuid.New(), failBytes); err != nil {
		log.Printf("Failed to execute payment failure query for workflow %s: %v", workflowUUID, err)
		return
	}
	_ = tx.Commit(ctx)
}

func writeOutboxResponse(ctx context.Context, tx pgx.Tx, workflowUUID uuid.UUID, payload map[string]any, label string) (bool, error) {
	outboxBytes, _ := json.Marshal(payload)
	_, _ = tx.Exec(ctx,
		`INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
		 VALUES ($1, $2, 'PAYMENT', $3, NOW())`,
		uuid.New(), workflowUUID, outboxBytes,
	)
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit %s payment outbox: %w", label, err)
	}
	log.Printf("[IDEMPOTENCY] Workflow %s payment is already %s; writing outbox message for duplicate delivery", workflowUUID, label)
	return true, nil
}

func handlePaymentCompensation(ctx context.Context, ingress PaymentIngressPayload, payloadBytes []byte) (bool, error) {
	workflowUUID := ingress.WorkflowID
	tx, err := shared.DBPool.Begin(ctx)
	if err != nil {
		emitPaymentFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), ingress.SeatIDs, ingress.NumSeats, payloadBytes)
		return true, nil
	}
	defer tx.Rollback(ctx)

	var paymentID uuid.UUID
	var amount int
	var currentStatus string
	queryErr := tx.QueryRow(ctx,
		`SELECT id, amount, status FROM payments WHERE workflow_id = $1 FOR UPDATE`,
		workflowUUID,
	).Scan(&paymentID, &amount, &currentStatus)

	if queryErr == nil {
		if currentStatus == "COMPLETED" && amount > 0 {
			_, err = tx.Exec(ctx, `UPDATE payments SET status = 'REFUNDED' WHERE workflow_id = $1`, workflowUUID)
			if err != nil {
				tx.Rollback(ctx)
				emitPaymentFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), ingress.SeatIDs, ingress.NumSeats, payloadBytes)
				return true, nil
			}
			log.Printf("[PAYMENT COMPENSATION] Refunded amount %d for workflow %s", amount, workflowUUID)
		} else {
			log.Printf("[PAYMENT COMPENSATION] Payment status is '%s' for workflow %s; no money taken to refund", currentStatus, workflowUUID)
		}
	} else {
		log.Printf("[PAYMENT COMPENSATION] No payment record found for workflow %s; no money taken", workflowUUID)
	}

	outboxPayload := map[string]any{
		"workflow_id": workflowUUID.String(),
		"status":      shared.STATUS_COMPENSATED,
		"action":      shared.ACTION_COMPENSATE,
		"seat_ids":    ingress.SeatIDs,
		"num_seats":   ingress.NumSeats,
		"updated_at":  time.Now().UTC().Format(time.RFC3339),
	}
	outboxPayloadBytes, err := json.Marshal(outboxPayload)
	if err != nil {
		tx.Rollback(ctx)
		emitPaymentFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), ingress.SeatIDs, ingress.NumSeats, payloadBytes)
		return true, nil
	}

	const compensateQuery = `
		WITH upserted_workflow AS (
			INSERT INTO workflows (id, workflow_state, payload)
			VALUES ($1, $2, $3)
			ON CONFLICT (id) DO UPDATE
			  SET workflow_state = EXCLUDED.workflow_state,
			      payload = EXCLUDED.payload
		)
		INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
		VALUES ($4, $1, 'PAYMENT', $5, NOW())`
	_, err = tx.Exec(ctx, compensateQuery,
		workflowUUID, string(shared.PAYMENT_WORKFLOW_FAILED), payloadBytes,
		uuid.New(), outboxPayloadBytes,
	)
	if err != nil {
		tx.Rollback(ctx)
		emitPaymentFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), ingress.SeatIDs, ingress.NumSeats, payloadBytes)
		return true, nil
	}

	if err := tx.Commit(ctx); err != nil {
		emitPaymentFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), ingress.SeatIDs, ingress.NumSeats, payloadBytes)
		return true, nil
	}

	log.Printf("[PAYMENT COMPENSATION] Successfully verified payments table for workflow %s and sent COMPENSATED outbox message", workflowUUID)
	return true, nil
}

func ProcessPaymentIngressMessage(ctx context.Context, msg *consumer.Message) (bool, error) {
	ingressPayload, payloadBytes, ok := consumer.DecodePayload(
		msg, "payment", func(p *PaymentIngressPayload) uuid.UUID { return p.WorkflowID },
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
		return handlePaymentCompensation(ctx, ingressPayload, payloadBytes)
	}

	// 2. Normal Payment (Worker performs single attempt; Orchestrator manages retries)
	paymentMethod := ingressPayload.PaymentMethod
	if paymentMethod == "" && ingressPayload.CustomerDetails != nil {
		paymentMethod = ingressPayload.CustomerDetails.PaymentMethod
	}
	if paymentMethod == "" {
		paymentMethod = "DEFAULT"
	}

	amount := ingressPayload.Amount
	if amount <= 0 {
		numSeats := ingressPayload.NumSeats
		if numSeats <= 0 && len(ingressPayload.SeatIDs) > 0 {
			numSeats = len(ingressPayload.SeatIDs)
		}
		if numSeats > 0 {
			amount = numSeats * 500
		}
	}

	tx, err := shared.DBPool.Begin(ctx)
	if err != nil {
		emitPaymentFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to begin transaction: %v", err), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes)
		return true, nil
	}
	defer tx.Rollback(ctx)

	var existingState string
	err = tx.QueryRow(ctx, `SELECT workflow_state FROM workflows WHERE id = $1 FOR UPDATE`, workflowUUID).Scan(&existingState)
	if err == nil {
		switch existingState {
		case string(shared.PAYMENT_WORKFLOW_COMPLETED):
			var paymentID uuid.UUID
			var currentAmount int
			var pMethod string
			_ = tx.QueryRow(ctx, `SELECT id, amount, payment_method FROM payments WHERE workflow_id = $1`, workflowUUID).Scan(&paymentID, &currentAmount, &pMethod)
			return writeOutboxResponse(ctx, tx, workflowUUID, map[string]any{
				"workflow_id":    workflowUUID.String(),
				"status":         shared.STATUS_COMPLETED,
				"amount":         currentAmount,
				"payment_method": pMethod,
				"payment_id":     paymentID.String(),
				"seat_ids":       ingressPayload.SeatIDs,
				"num_seats":      ingressPayload.NumSeats,
				"updated_at":     time.Now().UTC().Format(time.RFC3339),
			}, "COMPLETED")

		case string(shared.PAYMENT_WORKFLOW_FAILED):
			return writeOutboxResponse(ctx, tx, workflowUUID, map[string]any{
				"workflow_id": workflowUUID.String(),
				"status":      shared.STATUS_FAILED,
				"reason":      "Payment previously failed",
				"updated_at":  time.Now().UTC().Format(time.RFC3339),
			}, "FAILED")

		case string(shared.PAYMENT_WORKFLOW_COMPENSATING):
			tx.Rollback(ctx)
			log.Printf("Workflow %s is in compensating state; skipping forward execution", workflowUUID)
			return true, nil
		}
	}

	// Deterministic payment_id derived from workflow UUID to ensure gateway idempotency
	paymentID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("payment:"+workflowUUID.String()))

	outboxPayload := map[string]any{
		"workflow_id":    workflowUUID.String(),
		"status":         shared.STATUS_COMPLETED,
		"amount":         amount,
		"payment_method": paymentMethod,
		"payment_id":     paymentID.String(),
		"seat_ids":       ingressPayload.SeatIDs,
		"num_seats":      ingressPayload.NumSeats,
		"updated_at":     time.Now().UTC().Format(time.RFC3339),
	}
	outboxPayloadBytes, err := json.Marshal(outboxPayload)
	if err != nil {
		tx.Rollback(ctx)
		emitPaymentFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to marshal outbox payload: %v", err), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes)
		return true, nil
	}

	outboxID := uuid.New()
	const completePaymentQuery = `
		WITH upserted_workflow AS (
			INSERT INTO workflows (id, workflow_state, payload)
			VALUES ($1, $2, $3)
			ON CONFLICT (id) DO UPDATE
			  SET workflow_state = EXCLUDED.workflow_state,
			      payload = EXCLUDED.payload
		),
		upserted_payment AS (
			INSERT INTO payments (id, workflow_id, amount, payment_method, status, created_at)
			VALUES ($4, $1, $5, $6, 'COMPLETED', NOW())
			ON CONFLICT (workflow_id) DO UPDATE
			  SET amount = EXCLUDED.amount,
			      payment_method = EXCLUDED.payment_method,
			      status = CASE WHEN payments.status = 'REFUNDED' THEN 'REFUNDED' ELSE EXCLUDED.status END
		)
		INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
		VALUES ($7, $1, 'PAYMENT', $8, NOW())`
	_, err = tx.Exec(ctx, completePaymentQuery,
		workflowUUID, string(shared.PAYMENT_WORKFLOW_COMPLETED), payloadBytes,
		paymentID, amount, paymentMethod,
		outboxID, outboxPayloadBytes,
	)
	if err != nil {
		tx.Rollback(ctx)
		emitPaymentFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to record completed payment and outbox: %v", err), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes)
		return true, nil
	}

	if err := tx.Commit(ctx); err != nil {
		emitPaymentFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to commit transaction: %v", err), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes)
		return true, nil
	}

	log.Printf("Workflow %s payment processed: state=COMPLETED amount=%d; outbox message %s created",
		workflowUUID, amount, outboxID)
	return true, nil
}
