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
	UpdatedAt        string           `json:"updated_at,omitempty"`
	CreatedAt        string           `json:"created_at,omitempty"`
	TimeoutParameter int              `json:"timeout_parameter,omitempty"`
	TimeoutSeconds   int              `json:"timeout_seconds,omitempty"`
}

func ProcessPaymentIngressMessage(ctx context.Context, msg *consumer.Message) (bool, error) {
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

	var ingressPayload PaymentIngressPayload
	if err := json.Unmarshal(payloadBytes, &ingressPayload); err != nil {
		return false, fmt.Errorf("failed to unmarshal payment ingress payload (id=%s): %w", msg.ID, err)
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

	// 1. Handle Compensation: Look into payments table to see whether money was taken
	if ingressPayload.Action == shared.ACTION_COMPENSATE {
		tx, err := shared.DBPool.Begin(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to begin transaction: %w", err)
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
					return false, fmt.Errorf("failed to refund payment for workflow %s: %w", workflowUUID, err)
				}
				log.Printf("[PAYMENT COMPENSATION] Refunded amount %d for workflow %s", amount, workflowUUID)
			} else {
				log.Printf("[PAYMENT COMPENSATION] Payment status is '%s' for workflow %s; no money taken to refund", currentStatus, workflowUUID)
			}
		} else {
			log.Printf("[PAYMENT COMPENSATION] No payment record found for workflow %s; no money taken", workflowUUID)
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO workflows (id, workflow_state, payload)
			 VALUES ($1, $2, $3)
			 ON CONFLICT (id) DO UPDATE
			   SET workflow_state = EXCLUDED.workflow_state,
			       payload = EXCLUDED.payload`,
			workflowUUID, string(shared.PAYMENT_WORKFLOW_FAILED), payloadBytes,
		)
		if err != nil {
			return false, fmt.Errorf("failed to update workflow state during compensation: %w", err)
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
			return false, fmt.Errorf("failed to marshal compensation outbox payload: %w", err)
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
			 VALUES ($1, $2, 'PAYMENT', $3, NOW())`,
			uuid.New(), workflowUUID, outboxPayloadBytes,
		)
		if err != nil {
			return false, fmt.Errorf("failed to insert compensation outbox entry: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("failed to commit compensation transaction: %w", err)
		}

		log.Printf("[PAYMENT COMPENSATION] Successfully verified payments table for workflow %s and sent COMPENSATED outbox message", workflowUUID)
		return true, nil
	}

	// 2. Normal Payment with 2 retries on failure
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
		err = tx.QueryRow(ctx, `SELECT workflow_state FROM workflows WHERE id = $1 FOR UPDATE`, workflowUUID).Scan(&existingState)
		if err == nil && existingState == string(shared.PAYMENT_WORKFLOW_COMPLETED) {
			tx.Rollback(ctx)
			log.Printf("Workflow %s payment is already COMPLETED; skipping duplicate processing", workflowUUID)
			return true, nil
		}

		paymentID := uuid.New()
		_, err = tx.Exec(ctx,
			`INSERT INTO payments (id, workflow_id, amount, payment_method, status, created_at)
			 VALUES ($1, $2, $3, $4, 'COMPLETED', NOW())
			 ON CONFLICT (workflow_id) DO UPDATE
			   SET amount = EXCLUDED.amount,
			       payment_method = EXCLUDED.payment_method,
			       status = EXCLUDED.status`,
			paymentID, workflowUUID, amount, paymentMethod,
		)
		if err != nil {
			tx.Rollback(ctx)
			lastErr = fmt.Errorf("failed to insert payment record: %w", err)
			continue
		}

		_, err = tx.Exec(ctx,
			`INSERT INTO workflows (id, workflow_state, payload)
			 VALUES ($1, $2, $3)
			 ON CONFLICT (id) DO UPDATE
			   SET workflow_state = EXCLUDED.workflow_state,
			       payload = EXCLUDED.payload`,
			workflowUUID, string(shared.PAYMENT_WORKFLOW_COMPLETED), payloadBytes,
		)
		if err != nil {
			tx.Rollback(ctx)
			lastErr = fmt.Errorf("failed to upsert workflow: %w", err)
			continue
		}

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
			lastErr = fmt.Errorf("failed to marshal outbox payload: %w", err)
			continue
		}

		outboxID := uuid.New()
		_, err = tx.Exec(ctx,
			`INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
			 VALUES ($1, $2, 'PAYMENT', $3, NOW())`,
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

		log.Printf("Workflow %s payment processed: state=COMPLETED amount=%d; outbox message %s created",
			workflowUUID, amount, outboxID)
		return true, nil
	}

	// 3. Retries exhausted: Record FAILED in DB, emit FAILED outbox message, acknowledge from SQS
	tx, err := shared.DBPool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction for recording payment failure: %w", err)
	}
	defer tx.Rollback(ctx)

	_, _ = tx.Exec(ctx,
		`INSERT INTO workflows (id, workflow_state, payload)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO UPDATE
		   SET workflow_state = EXCLUDED.workflow_state,
		       payload = EXCLUDED.payload`,
		workflowUUID, string(shared.PAYMENT_WORKFLOW_FAILED), payloadBytes,
	)

	failOutbox := map[string]any{
		"workflow_id":       workflowUUID.String(),
		"status":            shared.STATUS_FAILED,
		"reason":            lastErr.Error(),
		"seat_ids":          ingressPayload.SeatIDs,
		"num_seats":         ingressPayload.NumSeats,
		"updated_at":        time.Now().UTC().Format(time.RFC3339),
		"timeout_parameter": timeoutParam,
		"timeout_seconds":   timeoutParam,
	}
	failBytes, _ := json.Marshal(failOutbox)

	_, _ = tx.Exec(ctx,
		`INSERT INTO outbox (id, workflow_id, worker_type, payload, updated_at)
		 VALUES ($1, $2, 'PAYMENT', $3, NOW())`,
		uuid.New(), workflowUUID, failBytes,
	)

	_ = tx.Commit(ctx)
	log.Printf("Workflow %s payment failed after %d retries: %v; sent FAILED outbox message", workflowUUID, maxRetries, lastErr)
	return true, nil
}

func ProcessIngressBatch(ctx context.Context, messages []*consumer.Message) {
	var toDelete []*consumer.Message

	for _, msg := range messages {
		if msg == nil {
			continue
		}

		shouldDelete, err := ProcessPaymentIngressMessage(ctx, msg)
		if err != nil {
			log.Printf("Error processing payment message %s: %v", msg.ID, err)
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
