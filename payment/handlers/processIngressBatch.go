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
)

type CustomerDetails struct {
	Email         string `json:"email,omitempty"`
	FullName      string `json:"full_name,omitempty"`
	Contact       any    `json:"contact,omitempty"`
	PaymentMethod string `json:"payment_method,omitempty"`
}

type PaymentIngressPayload struct {
	WorkflowID       uuid.UUID        `json:"workflow_id"`
	Action           string           `json:"action,omitempty"`
	Amount           int              `json:"amount"`
	NumSeats         int              `json:"num_seats"`
	SeatIDs          []string         `json:"seat_ids"`
	PaymentMethod    string           `json:"payment_method"`
	CustomerDetails  *CustomerDetails `json:"customer_details,omitempty"`
	TimeoutParameter int              `json:"timeout_parameter,omitempty"`
	TimeoutSeconds   int              `json:"timeout_seconds,omitempty"`
}

func emitPaymentFailure(ctx context.Context, workflowUUID uuid.UUID, action string, reason string, seatIDs []string, numSeats int, payloadBytes []byte, timeoutParam int) {
	tx, err := shared.DBPool.Begin(ctx)
	if err != nil {
		log.Printf("Failed to begin transaction for recording payment failure: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	failOutbox := map[string]any{
		"workflow_id":       workflowUUID.String(),
		"status":            shared.STATUS_FAILED,
		"reason":            reason,
		"seat_ids":          seatIDs,
		"num_seats":         numSeats,
		"updated_at":        time.Now().UTC().Format(time.RFC3339),
		"timeout_parameter": timeoutParam,
		"timeout_seconds":   timeoutParam,
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

func ProcessPaymentIngressMessage(ctx context.Context, msg *consumer.Message) (bool, error) {
	if msg == nil {
		return true, nil
	}

	if msg.Payload == nil {
		log.Printf("[POISON PILL DETECTED] Message payload is nil in payment message %s; acknowledging to prevent FIFO HoL blocking", msg.ID)
		return true, nil
	}

	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		log.Printf("[POISON PILL DETECTED] Failed to marshal message payload for payment message %s: %v; acknowledging to prevent FIFO HoL blocking", msg.ID, err)
		return true, nil
	}

	var ingressPayload PaymentIngressPayload
	if err := json.Unmarshal(payloadBytes, &ingressPayload); err != nil {
		log.Printf("[POISON PILL DETECTED] Unmarshal failed for payment message %s: %v; acknowledging to prevent FIFO HoL blocking", msg.ID, err)
		return true, nil
	}

	if ingressPayload.WorkflowID == uuid.Nil {
		log.Printf("[POISON PILL DETECTED] Missing or invalid workflow ID in payment message %s; acknowledging to prevent FIFO HoL blocking", msg.ID)
		return true, nil
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
		timeoutParam = int(shared.IngressVisibilityTimeout / 2)
		if timeoutParam <= 0 {
			timeoutParam = 15
		}
	}

	// 1. Handle Compensation
	if ingressPayload.Action == shared.ACTION_COMPENSATE {
		tx, err := shared.DBPool.Begin(ctx)
		if err != nil {
			emitPaymentFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes, timeoutParam)
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
					emitPaymentFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes, timeoutParam)
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
			"workflow_id":       workflowUUID.String(),
			"status":            shared.STATUS_COMPENSATED,
			"action":            shared.ACTION_COMPENSATE,
			"seat_ids":          ingressPayload.SeatIDs,
			"num_seats":         ingressPayload.NumSeats,
			"updated_at":        time.Now().UTC().Format(time.RFC3339),
			"timeout_parameter": timeoutParam,
			"timeout_seconds":   timeoutParam,
		}
		outboxPayloadBytes, err := json.Marshal(outboxPayload)
		if err != nil {
			tx.Rollback(ctx)
			emitPaymentFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes, timeoutParam)
			return true, nil
		}

		compensateQuery := `
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
			emitPaymentFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes, timeoutParam)
			return true, nil
		}

		if err := tx.Commit(ctx); err != nil {
			emitPaymentFailure(ctx, workflowUUID, shared.ACTION_COMPENSATE, err.Error(), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes, timeoutParam)
			return true, nil
		}

		log.Printf("[PAYMENT COMPENSATION] Successfully verified payments table for workflow %s and sent COMPENSATED outbox message", workflowUUID)
		return true, nil
	}

	// 2. Normal Payment (No local retry loop: Orchestrator manages retries)
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
		emitPaymentFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to begin transaction: %v", err), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes, timeoutParam)
		return true, nil
	}
	defer tx.Rollback(ctx)

	var existingState string
	err = tx.QueryRow(ctx, `SELECT workflow_state FROM workflows WHERE id = $1 FOR UPDATE`, workflowUUID).Scan(&existingState)
	if err == nil {
		if existingState == string(shared.PAYMENT_WORKFLOW_COMPLETED) {
			var paymentID uuid.UUID
			var currentAmount int
			var pMethod string
			_ = tx.QueryRow(ctx, `SELECT id, amount, payment_method FROM payments WHERE workflow_id = $1`, workflowUUID).Scan(&paymentID, &currentAmount, &pMethod)
			outboxPayload := map[string]any{
				"workflow_id":       workflowUUID.String(),
				"status":            shared.STATUS_COMPLETED,
				"amount":            currentAmount,
				"payment_method":    pMethod,
				"payment_id":        paymentID.String(),
				"seat_ids":          ingressPayload.SeatIDs,
				"num_seats":         ingressPayload.NumSeats,
				"updated_at":        time.Now().UTC().Format(time.RFC3339),
				"timeout_parameter": timeoutParam,
				"timeout_seconds":   timeoutParam,
			}
			outboxPayloadBytes, _ := json.Marshal(outboxPayload)
			_, _ = tx.Exec(ctx,
				`INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
				 VALUES ($1, $2, 'PAYMENT', $3, NOW())`,
				uuid.New(), workflowUUID, outboxPayloadBytes,
			)
			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("failed to commit re-emitted payment egress outbox: %w", err)
			}
			log.Printf("Workflow %s payment is already COMPLETED; re-emitted egress outbox message to satisfy orchestrator retry", workflowUUID)
			return true, nil
		}
		if existingState == string(shared.PAYMENT_WORKFLOW_FAILED) {
			outboxPayload := map[string]any{
				"workflow_id":       workflowUUID.String(),
				"status":            shared.STATUS_FAILED,
				"reason":            "Payment previously failed",
				"updated_at":        time.Now().UTC().Format(time.RFC3339),
				"timeout_parameter": timeoutParam,
				"timeout_seconds":   timeoutParam,
			}
			outboxPayloadBytes, _ := json.Marshal(outboxPayload)
			_, _ = tx.Exec(ctx,
				`INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
				 VALUES ($1, $2, 'PAYMENT', $3, NOW())`,
				uuid.New(), workflowUUID, outboxPayloadBytes,
			)
			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("failed to commit re-emitted failed payment outbox: %w", err)
			}
			log.Printf("Workflow %s is already FAILED; re-emitted failure outbox message to satisfy orchestrator retry", workflowUUID)
			return true, nil
		}
		if existingState == string(shared.PAYMENT_WORKFLOW_COMPENSATING) {
			tx.Rollback(ctx)
			log.Printf("Workflow %s is in compensating state; skipping forward execution", workflowUUID)
			return true, nil
		}
	}

	// Deterministic payment_id derived from workflow UUID to ensure gateway idempotency
	paymentID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("payment:"+workflowUUID.String()))

	outboxPayload := map[string]any{
		"workflow_id":       workflowUUID.String(),
		"status":            shared.STATUS_COMPLETED,
		"amount":            amount,
		"payment_method":    paymentMethod,
		"payment_id":        paymentID.String(),
		"seat_ids":          ingressPayload.SeatIDs,
		"num_seats":         ingressPayload.NumSeats,
		"updated_at":        time.Now().UTC().Format(time.RFC3339),
		"timeout_parameter": timeoutParam,
		"timeout_seconds":   timeoutParam,
	}
	outboxPayloadBytes, err := json.Marshal(outboxPayload)
	if err != nil {
		tx.Rollback(ctx)
		emitPaymentFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to marshal outbox payload: %v", err), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes, timeoutParam)
		return true, nil
	}

	outboxID := uuid.New()
	completePaymentQuery := `
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
		emitPaymentFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to record completed payment and outbox: %v", err), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes, timeoutParam)
		return true, nil
	}

	if err := tx.Commit(ctx); err != nil {
		emitPaymentFailure(ctx, workflowUUID, "", fmt.Sprintf("failed to commit transaction: %v", err), ingressPayload.SeatIDs, ingressPayload.NumSeats, payloadBytes, timeoutParam)
		return true, nil
	}

	log.Printf("Workflow %s payment processed: state=COMPLETED amount=%d; outbox message %s created",
		workflowUUID, amount, outboxID)
	return true, nil
}

func ProcessIngressBatch(ctx context.Context, messages []*consumer.Message) {
	var toDelete []*consumer.Message
	var toRelease []*consumer.Message

	for _, msg := range messages {
		if msg == nil {
			continue
		}

		deleted := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Recovered from panic processing payment message %s: %v", msg.ID, r)
				}
			}()

			shouldDelete, err := ProcessPaymentIngressMessage(ctx, msg)
			if err != nil {
				log.Printf("Error processing payment message %s: %v", msg.ID, err)
				return
			}

			if shouldDelete {
				toDelete = append(toDelete, msg)
				deleted = true
			}
		}()

		if !deleted {
			toRelease = append(toRelease, msg)
		}
	}

	if len(toDelete) > 0 {
		consumer.DeleteAllBatches(ctx, shared.IngressSQSClient, shared.IngressQueueURL, toDelete)
	}

	if len(toRelease) > 0 {
		consumer.ChangeVisibilityAllBatches(ctx, shared.IngressSQSClient, shared.IngressQueueURL, toRelease, 0)
	}
}
