package egresshandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	"github.com/D-pixel-crime/Endeavour/orchestrator/db/models"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type PaymentEgressPayload struct {
	WorkflowID    uuid.UUID `json:"workflow_id"`
	Status        string    `json:"status"`
	Amount        int       `json:"amount"`
	PaymentMethod string    `json:"payment_method,omitempty"`
	PaymentID     string    `json:"payment_id,omitempty"`
	SeatIDs       []string  `json:"seat_ids,omitempty"`
	NumSeats      int       `json:"num_seats,omitempty"`
	Reason        string    `json:"reason,omitempty"`
}

func ProcessPaymentEgressMessage(ctx context.Context, msg *consumer.Message) (bool, error) {
	if msg == nil {
		return true, nil
	}

	if msg.Payload == nil {
		log.Printf("[POISON PILL DETECTED] Message payload is nil for payment egress message %s; acknowledging to prevent FIFO HoL blocking", msg.ID)
		return true, nil
	}

	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		log.Printf("[POISON PILL DETECTED] Failed to marshal payload for payment egress message %s: %v; acknowledging to prevent FIFO HoL blocking", msg.ID, err)
		return true, nil
	}

	var egressPayload PaymentEgressPayload
	if err := json.Unmarshal(payloadBytes, &egressPayload); err != nil {
		log.Printf("[POISON PILL DETECTED] Unmarshal failed for payment egress message %s: %v; acknowledging to prevent FIFO HoL blocking", msg.ID, err)
		return true, nil
	}

	if egressPayload.WorkflowID == uuid.Nil {
		log.Printf("[POISON PILL DETECTED] Missing or invalid workflow ID in payment egress message %s; acknowledging to prevent FIFO HoL blocking", msg.ID)
		return true, nil
	}

	workflowUUID := egressPayload.WorkflowID

	if shared_vars.Orchestrator_DB_Pool == nil {
		return false, fmt.Errorf("database pool is not initialized")
	}

	tx, err := shared_vars.Orchestrator_DB_Pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var wf models.Workflow
	queryWorkflow := `SELECT id, workflow_type, workflow_state, payload, created_at, expires_at, state_timeout, retry_count FROM workflows WHERE id = $1 FOR UPDATE`
	err = tx.QueryRow(ctx, queryWorkflow, workflowUUID).Scan(
		&wf.ID,
		&wf.WorkflowType,
		&wf.WorkflowState,
		&wf.Payload,
		&wf.CreatedAt,
		&wf.ExpiresAt,
		&wf.StateTimeout,
		&wf.RetryCount,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			log.Printf("[POISON PILL DETECTED] Workflow %s not found in workflows table; acknowledging message %s to prevent FIFO HoL blocking", workflowUUID, msg.ID)
			return true, nil
		}
		return false, fmt.Errorf("failed to query workflow %s: %w", workflowUUID, err)
	}

	var currentPayload map[string]any
	if len(wf.Payload) > 0 {
		if err := json.Unmarshal(wf.Payload, &currentPayload); err != nil {
			log.Printf("Warning: failed to unmarshal existing workflow payload for %s: %v", workflowUUID, err)
			currentPayload = make(map[string]any)
		}
	}
	if currentPayload == nil {
		currentPayload = make(map[string]any)
	}

	// 1. Payment Failed -> Check retries before going to backward compensation
	if egressPayload.Status == shared_vars.STATUS_FAILED {
		// A. Handling Failure during Backward Compensation (COMPENSATING_PAYMENT)
		if wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT {
			visTimeout := int(shared_vars.PaymentEgressVisibilityTimeout)
			if visTimeout <= 0 {
				visTimeout = 30
			}
			timeoutSec := shared_vars.PaymentTaskTimeoutSeconds
			if timeoutSec <= 0 {
				timeoutSec = visTimeout / 2
			}
			stateTimeoutSec := visTimeout + timeoutSec

			const maxRetries = 2

			// If retries not exhausted (< 2), re-emit payment compensation with the same timeout
			if wf.RetryCount < maxRetries {
				newRetryCount := wf.RetryCount + 1
				currentPayload["last_payment_compensation_failure"] = map[string]any{
					"status":    egressPayload.Status,
					"reason":    egressPayload.Reason,
					"failed_at": time.Now().UTC().Format(time.RFC3339),
					"attempt":   newRetryCount,
				}
				updatedPayloadBytes, err := json.Marshal(currentPayload)
				if err != nil {
					return false, fmt.Errorf("failed to marshal retry payload for %s: %w", workflowUUID, err)
				}

				compensatePaymentPayload := map[string]any{
					"workflow_id":       workflowUUID.String(),
					"action":            shared_vars.ACTION_COMPENSATE,
					"reason":            fmt.Sprintf("Payment compensation retry (attempt %d/%d): %s", newRetryCount, maxRetries, egressPayload.Reason),
					"updated_at":        time.Now().UTC().Format(time.RFC3339),
					"timeout_seconds":   timeoutSec,
					"timeout_parameter": timeoutSec,
					"retry_count":       newRetryCount,
				}
				compensateBytes, _ := json.Marshal(compensatePaymentPayload)

				outboxID := uuid.New()
				retryQuery := fmt.Sprintf(`
					WITH updated_workflow AS (
						UPDATE workflows 
						SET payload = $1, 
						    state_timeout = NOW() + INTERVAL '%d seconds', 
						    retry_count = retry_count + 1 
						WHERE id = $2
						RETURNING id
					)
					INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
					SELECT $3, id, $4, $5, $6, NOW()
					FROM updated_workflow`, stateTimeoutSec)

				_, err = tx.Exec(ctx, retryQuery,
					updatedPayloadBytes,
					workflowUUID,
					outboxID,
					shared_vars.PAYMENT_WORKER,
					compensateBytes,
					timeoutSec,
				)
				if err != nil {
					return false, fmt.Errorf("failed to retry payment compensation for workflow %s: %w", workflowUUID, err)
				}

				if err := tx.Commit(ctx); err != nil {
					return false, fmt.Errorf("failed to commit retry transaction for workflow %s: %w", workflowUUID, err)
				}

				log.Printf("[RETRY] Payment compensation failed for workflow %s (attempt %d/%d): %s; re-emitted payment compensation task to outbox with timeout %ds",
					workflowUUID, newRetryCount, maxRetries, egressPayload.Reason, timeoutSec)
				return true, nil
			}

			// Payment compensation retries exhausted (>= 2):
			// Escalate directly to COMPENSATING_BOOKING to release reserved seats, and record a critical operator alert!
			seatIDs := egressPayload.SeatIDs
			if len(seatIDs) == 0 {
				if rawSeats, ok := currentPayload["seat_ids"].([]any); ok {
					for _, s := range rawSeats {
						if str, ok := s.(string); ok {
							seatIDs = append(seatIDs, str)
						}
					}
				}
			}

			visBookingTimeout := int(shared_vars.BookingEgressVisibilityTimeout)
			if visBookingTimeout <= 0 {
				visBookingTimeout = 30
			}
			bookingTimeout := shared_vars.BookingTaskTimeoutSeconds
			if bookingTimeout <= 0 {
				bookingTimeout = visBookingTimeout / 2
			}
			bookingStateTimeoutSec := visBookingTimeout + bookingTimeout

			currentPayload["payment_compensation_failure"] = map[string]any{
				"status":            egressPayload.Status,
				"reason":            egressPayload.Reason,
				"failed_at":         time.Now().UTC().Format(time.RFC3339),
				"exhausted_retries": wf.RetryCount,
			}
			currentPayload["critical_operator_alert"] = fmt.Sprintf("Payment compensation failed after %d retries (%s); escalated to Booking seat release. Customer refund requires manual operator review.", wf.RetryCount, egressPayload.Reason)

			updatedPayloadBytes, err := json.Marshal(currentPayload)
			if err != nil {
				return false, fmt.Errorf("failed to marshal escalated payload for %s: %w", workflowUUID, err)
			}

			compensateBookingPayload := map[string]any{
				"workflow_id":       workflowUUID.String(),
				"action":            shared_vars.ACTION_COMPENSATE,
				"seat_ids":          seatIDs,
				"reason":            fmt.Sprintf("Payment compensation failed after %d retries; escalating to release seats", wf.RetryCount),
				"updated_at":        time.Now().UTC().Format(time.RFC3339),
				"timeout_seconds":   bookingTimeout,
				"timeout_parameter": bookingTimeout,
			}
			compensateBookingBytes, _ := json.Marshal(compensateBookingPayload)

			escalateQuery := fmt.Sprintf(`
				WITH updated_workflow AS (
					UPDATE workflows 
					SET workflow_state = $1, 
					    payload = $2, 
					    state_timeout = NOW() + INTERVAL '%d seconds', 
					    retry_count = 0 
					WHERE id = $3
					RETURNING id
				)
				INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
				SELECT $4, id, $5, $6, $7, NOW()
				FROM updated_workflow`, bookingStateTimeoutSec)

			_, err = tx.Exec(ctx, escalateQuery,
				shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING,
				updatedPayloadBytes,
				workflowUUID,
				uuid.New(),
				shared_vars.BOOKING_WORKER,
				compensateBookingBytes,
				bookingTimeout,
			)
			if err != nil {
				return false, fmt.Errorf("failed to escalate to booking compensation for %s: %w", workflowUUID, err)
			}

			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("failed to commit escalation transaction for %s: %w", workflowUUID, err)
			}

			log.Printf("[SAGA ESCALATION] Payment compensation failed for %s after exhausting %d retries; escalated to COMPENSATING_BOOKING to release seats", workflowUUID, wf.RetryCount)
			return true, nil
		}

		// B. Handling Failure during Forward Step (PAYMENT)
		// Idempotency Guard: Only initiate retry or compensation if the workflow is currently expecting payment.
		// If it has already transitioned to COMPENSATING_PAYMENT, COMPENSATING_BOOKING, or FAILED,
		// skip re-dispatching to prevent duplicate compensation storms or state regression.
		if wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT {
			log.Printf("Workflow %s payment failed message received, but workflow state is %s (expected PAYMENT or COMPENSATING_PAYMENT); skipping duplicate failure transition", workflowUUID, wf.WorkflowState)
			return true, nil
		}

		visTimeout := int(shared_vars.PaymentEgressVisibilityTimeout)
		if visTimeout <= 0 {
			visTimeout = 30
		}
		timeoutSec := shared_vars.PaymentTaskTimeoutSeconds
		if timeoutSec <= 0 {
			timeoutSec = visTimeout / 2
		}
		stateTimeoutSec := visTimeout + timeoutSec

		const maxRetries = 2

		// If retries not exhausted (< 2), re-emit work for PAYMENT worker with the same timeout as earlier
		if wf.RetryCount < maxRetries {
			newRetryCount := wf.RetryCount + 1
			currentPayload["last_payment_failure"] = map[string]any{
				"status":    egressPayload.Status,
				"reason":    egressPayload.Reason,
				"failed_at": time.Now().UTC().Format(time.RFC3339),
				"attempt":   newRetryCount,
			}
			updatedPayloadBytes, err := json.Marshal(currentPayload)
			if err != nil {
				return false, fmt.Errorf("failed to marshal retry payload for %s: %w", workflowUUID, err)
			}

			seatIDs := egressPayload.SeatIDs
			if len(seatIDs) == 0 {
				if rawSeats, ok := currentPayload["seat_ids"].([]any); ok {
					for _, s := range rawSeats {
						if str, ok := s.(string); ok {
							seatIDs = append(seatIDs, str)
						}
					}
				}
			}

			numSeats := egressPayload.NumSeats
			if numSeats <= 0 {
				if ns, ok := currentPayload["num_seats"].(float64); ok && ns > 0 {
					numSeats = int(ns)
				} else if len(seatIDs) > 0 {
					numSeats = len(seatIDs)
				}
			}

			amount := egressPayload.Amount
			if amount <= 0 {
				if am, ok := currentPayload["amount"].(float64); ok && am > 0 {
					amount = int(am)
				} else if numSeats > 0 {
					amount = numSeats * 500
				}
			}

			paymentRetryPayload := map[string]any{
				"workflow_id":       workflowUUID.String(),
				"seat_ids":          seatIDs,
				"num_seats":         numSeats,
				"amount":            amount,
				"updated_at":        time.Now().UTC().Format(time.RFC3339),
				"timeout_seconds":   timeoutSec,
				"timeout_parameter": timeoutSec,
				"retry_count":       newRetryCount,
			}

			if pm, ok := currentPayload["payment_method"].(string); ok && pm != "" {
				paymentRetryPayload["payment_method"] = pm
			} else if cd, ok := currentPayload["customer_details"].(map[string]any); ok {
				if pm, ok := cd["payment_method"].(string); ok && pm != "" {
					paymentRetryPayload["payment_method"] = pm
				}
			}

			paymentRetryBytes, err := json.Marshal(paymentRetryPayload)
			if err != nil {
				return false, fmt.Errorf("failed to marshal payment retry outbox payload for %s: %w", workflowUUID, err)
			}

			outboxID := uuid.New()
			retryQuery := fmt.Sprintf(`
				WITH updated_workflow AS (
					UPDATE workflows 
					SET payload = $1, 
					    state_timeout = NOW() + INTERVAL '%d seconds', 
					    retry_count = retry_count + 1 
					WHERE id = $2
					RETURNING id
				)
				INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
				SELECT $3, id, $4, $5, $6, NOW()
				FROM updated_workflow`, stateTimeoutSec)

			_, err = tx.Exec(ctx, retryQuery,
				updatedPayloadBytes,
				workflowUUID,
				outboxID,
				shared_vars.PAYMENT_WORKER,
				paymentRetryBytes,
				timeoutSec,
			)
			if err != nil {
				return false, fmt.Errorf("failed to retry payment for workflow %s: %w", workflowUUID, err)
			}

			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("failed to commit retry transaction for workflow %s: %w", workflowUUID, err)
			}

			log.Printf("[RETRY] Workflow %s payment failed (attempt %d/%d): %s; re-emitted payment task to outbox with timeout %ds",
				workflowUUID, newRetryCount, maxRetries, egressPayload.Reason, timeoutSec)
			return true, nil
		}

		// Retries exhausted (>= 2): Start backward compensation from PAYMENT backwards
		currentPayload["payment_failure"] = map[string]any{
			"status":            egressPayload.Status,
			"reason":            egressPayload.Reason,
			"failed_at":         time.Now().UTC().Format(time.RFC3339),
			"exhausted_retries": wf.RetryCount,
		}
		updatedPayloadBytes, err := json.Marshal(currentPayload)
		if err != nil {
			return false, fmt.Errorf("failed to marshal payload for %s: %w", workflowUUID, err)
		}

		compensatePaymentPayload := map[string]any{
			"workflow_id":       workflowUUID.String(),
			"action":            shared_vars.ACTION_COMPENSATE,
			"reason":            egressPayload.Reason,
			"updated_at":        time.Now().UTC().Format(time.RFC3339),
			"timeout_seconds":   timeoutSec,
			"timeout_parameter": timeoutSec,
		}
		compensateBytes, _ := json.Marshal(compensatePaymentPayload)

		query := fmt.Sprintf(`
			WITH updated_workflow AS (
				UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NOW() + INTERVAL '%d seconds', retry_count = 0 WHERE id = $3
				RETURNING id
			)
			INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
			SELECT $4, id, $5, $6, $7, NOW()
			FROM updated_workflow`, stateTimeoutSec)
		_, err = tx.Exec(ctx, query,
			shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT,
			updatedPayloadBytes,
			workflowUUID,
			uuid.New(),
			shared_vars.PAYMENT_WORKER,
			compensateBytes,
			timeoutSec,
		)
		if err != nil {
			return false, fmt.Errorf("failed to update workflow %s and insert payment compensation outbox: %w", workflowUUID, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
		}

		log.Printf("[SAGA ROLLBACK] Payment failed for workflow %s after exhausting %d retries; started backward compensation at PAYMENT", workflowUUID, wf.RetryCount)
		return true, nil
	}

	// 2. Payment Compensated -> Move backward to previous service: BOOKING
	if egressPayload.Status == shared_vars.STATUS_COMPENSATED {
		// Idempotency Guard: Only advance to COMPENSATING_BOOKING if currently in COMPENSATING_PAYMENT (or PAYMENT).
		// If already in COMPENSATING_BOOKING or terminal FAILED, skip to prevent state regression and duplicate outbox events.
		if wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT && wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT {
			log.Printf("Workflow %s payment compensated message received, but workflow state is %s (expected COMPENSATING_PAYMENT); skipping duplicate transition", workflowUUID, wf.WorkflowState)
			return true, nil
		}
		currentPayload["payment_compensation"] = map[string]any{
			"status":         egressPayload.Status,
			"compensated_at": time.Now().UTC().Format(time.RFC3339),
		}
		updatedPayloadBytes, err := json.Marshal(currentPayload)
		if err != nil {
			return false, fmt.Errorf("failed to marshal payload for %s: %w", workflowUUID, err)
		}

		visTimeout := int(shared_vars.BookingEgressVisibilityTimeout)
		if visTimeout <= 0 {
			visTimeout = 30
		}
		timeoutSec := shared_vars.BookingTaskTimeoutSeconds
		if timeoutSec <= 0 {
			timeoutSec = visTimeout / 2
		}
		stateTimeoutSec := visTimeout + timeoutSec

		seatIDs := egressPayload.SeatIDs
		if len(seatIDs) == 0 {
			if rawSeats, ok := currentPayload["seat_ids"].([]any); ok {
				for _, s := range rawSeats {
					if str, ok := s.(string); ok {
						seatIDs = append(seatIDs, str)
					}
				}
			}
		}

		compensateBookingPayload := map[string]any{
			"workflow_id":       workflowUUID.String(),
			"action":            shared_vars.ACTION_COMPENSATE,
			"seat_ids":          seatIDs,
			"updated_at":        time.Now().UTC().Format(time.RFC3339),
			"timeout_seconds":   timeoutSec,
			"timeout_parameter": timeoutSec,
		}
		compensateBookingBytes, _ := json.Marshal(compensateBookingPayload)

		query := fmt.Sprintf(`
			WITH updated_workflow AS (
				UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NOW() + INTERVAL '%d seconds', retry_count = 0 WHERE id = $3
				RETURNING id
			)
			INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
			SELECT $4, id, $5, $6, $7, NOW()
			FROM updated_workflow`, stateTimeoutSec)
		_, err = tx.Exec(ctx, query,
			shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING,
			updatedPayloadBytes,
			workflowUUID,
			uuid.New(),
			shared_vars.BOOKING_WORKER,
			compensateBookingBytes,
			timeoutSec,
		)
		if err != nil {
			return false, fmt.Errorf("failed to update workflow %s and insert booking compensation outbox: %w", workflowUUID, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
		}

		log.Printf("[SAGA ROLLBACK] Payment compensated for workflow %s; continuing backward compensation to BOOKING", workflowUUID)
		return true, nil
	}

	// 3. Normal Forward Payment Completed
	if egressPayload.Status != shared_vars.STATUS_COMPLETED {
		log.Printf("Workflow %s payment status is '%s'; unhandled status", workflowUUID, egressPayload.Status)
		return true, nil
	}

	if wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT {
		log.Printf("Workflow %s is in state %s (expected PAYMENT); skipping ticket creation", workflowUUID, wf.WorkflowState)

		// Ghost charge prevention: If payment was captured in Payment DB but workflow is no longer in PAYMENT state
		// (e.g. timed out to FAILED or escalated to COMPENSATING_BOOKING), refund the charge immediately by emitting
		// a compensation outbox event.
		if egressPayload.Amount > 0 || egressPayload.PaymentID != "" {
			compensatePaymentPayload := map[string]any{
				"workflow_id":       workflowUUID.String(),
				"action":            shared_vars.ACTION_COMPENSATE,
				"reason":            fmt.Sprintf("Ghost charge refund: completed after workflow state was %s", wf.WorkflowState),
				"updated_at":        time.Now().UTC().Format(time.RFC3339),
				"timeout_seconds":   15,
				"timeout_parameter": 15,
			}
			compensatePaymentBytes, _ := json.Marshal(compensatePaymentPayload)
			outboxInsert := `INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at) VALUES ($1, $2, $3, $4, $5, NOW())`
			_, err = tx.Exec(ctx, outboxInsert, uuid.New(), workflowUUID, shared_vars.PAYMENT_WORKER, compensatePaymentBytes, 15)
			if err != nil {
				return false, fmt.Errorf("failed to insert ghost charge refund compensation outbox for %s: %w", workflowUUID, err)
			}
			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("failed to commit ghost charge refund compensation for %s: %w", workflowUUID, err)
			}
			log.Printf("[GHOST CHARGE RESOLVED] Dispatched compensation to refund payment for workflow %s (current state: %s)", workflowUUID, wf.WorkflowState)
			return true, nil
		}

		return true, nil
	}

	var customerName, customerEmail string
	if fn, ok := currentPayload["full_name"].(string); ok {
		customerName = fn
	}
	if em, ok := currentPayload["email"].(string); ok {
		customerEmail = em
	}
	if customerName == "" || customerEmail == "" {
		if cd, ok := currentPayload["customer_details"].(map[string]any); ok {
			if customerName == "" {
				if fn, ok := cd["full_name"].(string); ok {
					customerName = fn
				}
			}
			if customerEmail == "" {
				if em, ok := cd["email"].(string); ok {
					customerEmail = em
				}
			}
		}
	}

	seatIDs := egressPayload.SeatIDs
	if len(seatIDs) == 0 {
		if rawSeats, ok := currentPayload["seat_ids"].([]any); ok {
			for _, s := range rawSeats {
				if str, ok := s.(string); ok {
					seatIDs = append(seatIDs, str)
				}
			}
		}
	}
	if len(seatIDs) == 0 {
		numSeats := egressPayload.NumSeats
		if numSeats <= 0 {
			if ns, ok := currentPayload["num_seats"].(float64); ok && ns > 0 {
				numSeats = int(ns)
			}
		}
		if numSeats > 0 {
			for i := 1; i <= numSeats; i++ {
				seatIDs = append(seatIDs, fmt.Sprintf("SEAT-%d", i))
			}
		} else {
			seatIDs = append(seatIDs, "GENERAL")
		}
	}

	amount := egressPayload.Amount
	if amount <= 0 {
		if a, ok := currentPayload["amount"].(float64); ok && a > 0 {
			amount = int(a)
		} else {
			amount = len(seatIDs) * 500
		}
	}

	now := time.Now().UTC()
	t := models.Ticket{
		ID:            uuid.New(),
		WorkflowID:    workflowUUID,
		SeatIDs:       seatIDs,
		Amount:        amount,
		PaymentID:     egressPayload.PaymentID,
		CustomerName:  customerName,
		CustomerEmail: customerEmail,
		Status:        "CONFIRMED",
		CreatedAt:     now,
	}

	currentPayload["payment"] = map[string]any{
		"status":         egressPayload.Status,
		"amount":         amount,
		"payment_id":     egressPayload.PaymentID,
		"payment_method": egressPayload.PaymentMethod,
	}
	currentPayload["ticket"] = t

	updatedWorkflowPayload, err := json.Marshal(currentPayload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal updated workflow payload for %s: %w", workflowUUID, err)
	}

	completeWorkflowAndInsertTicket := `
		WITH upserted_ticket AS (
			INSERT INTO tickets (id, workflow_id, seat_ids, amount, payment_id, customer_name, customer_email, status, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (workflow_id) DO UPDATE
			SET seat_ids = EXCLUDED.seat_ids,
			    amount = EXCLUDED.amount,
			    payment_id = COALESCE(EXCLUDED.payment_id, tickets.payment_id),
			    customer_name = COALESCE(EXCLUDED.customer_name, tickets.customer_name),
			    customer_email = COALESCE(EXCLUDED.customer_email, tickets.customer_email),
			    status = EXCLUDED.status
			RETURNING id
		)
		UPDATE workflows 
		SET workflow_state = $10, 
		    payload = $11, 
		    state_timeout = NULL, 
		    retry_count = 0 
		WHERE id = $2`

	_, err = tx.Exec(ctx, completeWorkflowAndInsertTicket,
		t.ID,
		t.WorkflowID,
		t.SeatIDs,
		t.Amount,
		t.PaymentID,
		t.CustomerName,
		t.CustomerEmail,
		t.Status,
		t.CreatedAt,
		shared_vars.ORCHESTRATOR_WORKFLOW_COMPLETED,
		updatedWorkflowPayload,
	)
	if err != nil {
		return false, fmt.Errorf("failed to insert ticket and complete workflow %s: %w", workflowUUID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
	}

	log.Printf("Workflow %s successfully transitioned to COMPLETED; ticket %s created", workflowUUID, t.ID)
	return true, nil
}

func ProcessPaymentEgressBatch(ctx context.Context, messages []*consumer.Message) {
	var toDelete []*consumer.Message

	for _, msg := range messages {
		if msg == nil {
			continue
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Recovered from panic processing payment egress message %s: %v", msg.ID, r)
				}
			}()

			shouldDelete, err := ProcessPaymentEgressMessage(ctx, msg)
			if err != nil {
				log.Printf("Error processing payment egress message %s: %v", msg.ID, err)
				return
			}

			if shouldDelete {
				toDelete = append(toDelete, msg)
			}
		}()
	}

	if len(toDelete) > 0 {
		consumer.DeleteAllBatches(ctx, shared_vars.PaymentEgressSQSClient, shared_vars.PaymentEgressQueueURL, toDelete)
	}
}
