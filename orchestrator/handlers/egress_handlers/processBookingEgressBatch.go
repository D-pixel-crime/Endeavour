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

type BookingEgressPayload struct {
	WorkflowID uuid.UUID `json:"workflow_id"`
	Status     string    `json:"status"`
	SeatIDs    []string  `json:"seat_ids"`
	NumSeats   int       `json:"num_seats"`
	Reason     string    `json:"reason,omitempty"`
}

func ProcessBookingEgressMessage(ctx context.Context, msg *consumer.Message) (bool, error) {
	if msg == nil {
		return true, nil
	}

	if msg.Payload == nil {
		log.Printf("[POISON PILL DETECTED] Message payload is nil for booking egress message %s; acknowledging to prevent FIFO HoL blocking", msg.ID)
		return true, nil
	}

	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		log.Printf("[POISON PILL DETECTED] Failed to marshal payload for booking egress message %s: %v; acknowledging to prevent FIFO HoL blocking", msg.ID, err)
		return true, nil
	}

	var egressPayload BookingEgressPayload
	if err := json.Unmarshal(payloadBytes, &egressPayload); err != nil {
		log.Printf("[POISON PILL DETECTED] Unmarshal failed for booking egress message %s: %v; acknowledging to prevent FIFO HoL blocking", msg.ID, err)
		return true, nil
	}

	if egressPayload.WorkflowID == uuid.Nil {
		log.Printf("[POISON PILL DETECTED] Missing or invalid workflow ID in booking egress message %s; acknowledging to prevent FIFO HoL blocking", msg.ID)
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

	// 1. Booking Failed -> Check retries before marking whole workflow as FAILED
	if egressPayload.Status == shared_vars.STATUS_FAILED {
		// A. Handling Failure during Backward Compensation (COMPENSATING_BOOKING)
		if wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING {
			visTimeout := int(shared_vars.BookingEgressVisibilityTimeout)
			if visTimeout <= 0 {
				visTimeout = 30
			}
			bookingTimeout := shared_vars.BookingTaskTimeoutSeconds
			if bookingTimeout <= 0 {
				bookingTimeout = visTimeout / 2
			}
			stateTimeoutSec := visTimeout + bookingTimeout

			const maxRetries = 2

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

			// If retries not exhausted (< 2), re-emit booking compensation with the same timeout
			if wf.RetryCount < maxRetries {
				newRetryCount := wf.RetryCount + 1
				currentPayload["last_booking_compensation_failure"] = map[string]any{
					"status":    egressPayload.Status,
					"reason":    egressPayload.Reason,
					"failed_at": time.Now().UTC().Format(time.RFC3339),
					"attempt":   newRetryCount,
				}
				updatedPayloadBytes, err := json.Marshal(currentPayload)
				if err != nil {
					return false, fmt.Errorf("failed to marshal retry payload for %s: %w", workflowUUID, err)
				}

				compensateBookingPayload := map[string]any{
					"workflow_id":       workflowUUID.String(),
					"action":            shared_vars.ACTION_COMPENSATE,
					"seat_ids":          seatIDs,
					"reason":            fmt.Sprintf("Booking compensation retry (attempt %d/%d): %s", newRetryCount, maxRetries, egressPayload.Reason),
					"updated_at":        time.Now().UTC().Format(time.RFC3339),
					"timeout_seconds":   bookingTimeout,
					"timeout_parameter": bookingTimeout,
					"retry_count":       newRetryCount,
				}
				compensateBookingBytes, _ := json.Marshal(compensateBookingPayload)

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
					shared_vars.BOOKING_WORKER,
					compensateBookingBytes,
					bookingTimeout,
				)
				if err != nil {
					return false, fmt.Errorf("failed to retry booking compensation for %s: %w", workflowUUID, err)
				}

				if err := tx.Commit(ctx); err != nil {
					return false, fmt.Errorf("failed to commit retry transaction for workflow %s: %w", workflowUUID, err)
				}

				log.Printf("[RETRY] Booking compensation failed for %s (attempt %d/%d): %s; re-emitted booking compensation task to outbox with timeout %ds",
					workflowUUID, newRetryCount, maxRetries, egressPayload.Reason, bookingTimeout)
				return true, nil
			}

			// Booking compensation retries exhausted (>= 2):
			// We have reached the start of the workflow. Mark workflow FAILED and record critical operator alert for leaked inventory!
			currentPayload["booking_compensation_failure"] = map[string]any{
				"status":            egressPayload.Status,
				"reason":            egressPayload.Reason,
				"failed_at":         time.Now().UTC().Format(time.RFC3339),
				"exhausted_retries": wf.RetryCount,
			}
			currentPayload["critical_operator_alert"] = fmt.Sprintf("Booking compensation failed after %d retries (%s); reserved seats (%v) may be leaked and require manual release.", wf.RetryCount, egressPayload.Reason, seatIDs)

			updatedPayloadBytes, err := json.Marshal(currentPayload)
			if err != nil {
				return false, fmt.Errorf("failed to marshal payload for %s: %w", workflowUUID, err)
			}

			updateQuery := `UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NULL, retry_count = 0 WHERE id = $3`
			_, err = tx.Exec(ctx, updateQuery, shared_vars.ORCHESTRATOR_WORKFLOW_FAILED, updatedPayloadBytes, workflowUUID)
			if err != nil {
				return false, fmt.Errorf("failed to update workflow %s to FAILED: %w", workflowUUID, err)
			}

			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
			}

			log.Printf("[CRITICAL ALERT] Booking compensation failed for %s after exhausting %d retries; marked as FAILED with operator alert", workflowUUID, wf.RetryCount)
			return true, nil
		}

		// B. Handling Failure during Forward Step (BOOKING)
		// Idempotency Guard: Only handle failure if workflow is in BOOKING state.
		// If already in FAILED or another state, skip duplicate transition.
		if wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_FAILED {
			log.Printf("Workflow %s booking failed message received, but workflow is already FAILED; skipping duplicate transition", workflowUUID)
			return true, nil
		}
		if wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_BOOKING {
			log.Printf("Workflow %s booking failed message received, but workflow state is %s (expected BOOKING or COMPENSATING_BOOKING); skipping", workflowUUID, wf.WorkflowState)
			return true, nil
		}

		visTimeout := int(shared_vars.BookingEgressVisibilityTimeout)
		if visTimeout <= 0 {
			visTimeout = 30
		}
		bookingTimeout := shared_vars.BookingTaskTimeoutSeconds
		if bookingTimeout <= 0 {
			bookingTimeout = visTimeout / 2
		}
		stateTimeoutSec := visTimeout + bookingTimeout

		const maxRetries = 2

		// If retries not exhausted (< 2), re-emit work for BOOKING worker with the same timeout as earlier
		if wf.RetryCount < maxRetries {
			newRetryCount := wf.RetryCount + 1
			currentPayload["last_booking_failure"] = map[string]any{
				"status":    egressPayload.Status,
				"reason":    egressPayload.Reason,
				"failed_at": time.Now().UTC().Format(time.RFC3339),
				"attempt":   newRetryCount,
			}
			updatedPayloadBytes, err := json.Marshal(currentPayload)
			if err != nil {
				return false, fmt.Errorf("failed to marshal retry payload for %s: %w", workflowUUID, err)
			}

			numSeats := 0
			if ns, ok := currentPayload["num_seats"].(float64); ok && ns > 0 {
				numSeats = int(ns)
			} else if egressPayload.NumSeats > 0 {
				numSeats = egressPayload.NumSeats
			} else if rawSeats, ok := currentPayload["seat_ids"].([]any); ok && len(rawSeats) > 0 {
				numSeats = len(rawSeats)
			}

			bookingRetryPayload := map[string]any{
				"workflow_id":       workflowUUID.String(),
				"num_seats":         numSeats,
				"updated_at":        time.Now().UTC().Format(time.RFC3339),
				"timeout_seconds":   bookingTimeout,
				"timeout_parameter": bookingTimeout,
				"retry_count":       newRetryCount,
			}
			bookingRetryBytes, err := json.Marshal(bookingRetryPayload)
			if err != nil {
				return false, fmt.Errorf("failed to marshal booking retry outbox payload for %s: %w", workflowUUID, err)
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
				shared_vars.BOOKING_WORKER,
				bookingRetryBytes,
				bookingTimeout,
			)
			if err != nil {
				return false, fmt.Errorf("failed to retry booking for workflow %s: %w", workflowUUID, err)
			}

			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("failed to commit retry transaction for workflow %s: %w", workflowUUID, err)
			}

			log.Printf("[RETRY] Workflow %s booking failed (attempt %d/%d): %s; re-emitted booking task to outbox with timeout %ds",
				workflowUUID, newRetryCount, maxRetries, egressPayload.Reason, bookingTimeout)
			return true, nil
		}

		// Retries exhausted (>= 2): First step failed, no prior steps to compensate
		currentPayload["booking_failure"] = map[string]any{
			"status":            egressPayload.Status,
			"reason":            egressPayload.Reason,
			"failed_at":         time.Now().UTC().Format(time.RFC3339),
			"exhausted_retries": wf.RetryCount,
		}
		updatedPayloadBytes, err := json.Marshal(currentPayload)
		if err != nil {
			return false, fmt.Errorf("failed to marshal payload for %s: %w", workflowUUID, err)
		}

		updateQuery := `UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NULL WHERE id = $3`
		_, err = tx.Exec(ctx, updateQuery, shared_vars.ORCHESTRATOR_WORKFLOW_FAILED, updatedPayloadBytes, workflowUUID)
		if err != nil {
			return false, fmt.Errorf("failed to update workflow %s to FAILED: %w", workflowUUID, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
		}

		log.Printf("Workflow %s booking failed after exhausting %d retries; marked as FAILED with no prior steps to compensate", workflowUUID, wf.RetryCount)
		return true, nil
	}

	// 2. Booking Compensated -> Start reached, backward compensation complete
	if egressPayload.Status == shared_vars.STATUS_COMPENSATED {
		// Idempotency Guard: If already terminal FAILED, skip re-executing.
		if wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_FAILED {
			log.Printf("Workflow %s booking compensated message received, but workflow is already FAILED; skipping duplicate transition", workflowUUID)
			return true, nil
		}
		currentPayload["booking_compensation"] = map[string]any{
			"status":         egressPayload.Status,
			"compensated_at": time.Now().UTC().Format(time.RFC3339),
		}
		updatedPayloadBytes, err := json.Marshal(currentPayload)
		if err != nil {
			return false, fmt.Errorf("failed to marshal payload for %s: %w", workflowUUID, err)
		}

		updateQuery := `UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NULL, retry_count = 0 WHERE id = $3`
		_, err = tx.Exec(ctx, updateQuery, shared_vars.ORCHESTRATOR_WORKFLOW_FAILED, updatedPayloadBytes, workflowUUID)
		if err != nil {
			return false, fmt.Errorf("failed to update workflow %s to FAILED: %w", workflowUUID, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
		}

		log.Printf("[SAGA ROLLBACK] Booking compensated for workflow %s; reached start of workflow. Full compensation complete!", workflowUUID)
		return true, nil
	}

	// 3. Normal Forward Booking Completed
	if egressPayload.Status != shared_vars.STATUS_COMPLETED {
		log.Printf("Workflow %s booking status is '%s'; unhandled status", workflowUUID, egressPayload.Status)
		return true, nil
	}

	if wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_BOOKING {
		log.Printf("Workflow %s is in state %s (expected BOOKING); skipping advance to payment", workflowUUID, wf.WorkflowState)

		// Ghost allocation prevention: If seats were allocated in Booking DB but workflow is no longer in BOOKING state,
		// release those seats by emitting a compensation outbox event.
		if len(egressPayload.SeatIDs) > 0 {
			compensateBookingPayload := map[string]any{
				"workflow_id":       workflowUUID.String(),
				"action":            shared_vars.ACTION_COMPENSATE,
				"seat_ids":          egressPayload.SeatIDs,
				"reason":            fmt.Sprintf("Ghost allocation release: completed after workflow state was %s", wf.WorkflowState),
				"updated_at":        time.Now().UTC().Format(time.RFC3339),
				"timeout_seconds":   15,
				"timeout_parameter": 15,
			}
			compensateBookingBytes, _ := json.Marshal(compensateBookingPayload)
			outboxInsert := `INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at) VALUES ($1, $2, $3, $4, $5, NOW())`
			_, err = tx.Exec(ctx, outboxInsert, uuid.New(), workflowUUID, shared_vars.BOOKING_WORKER, compensateBookingBytes, 15)
			if err != nil {
				return false, fmt.Errorf("failed to insert ghost seat compensation outbox for %s: %w", workflowUUID, err)
			}
			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("failed to commit ghost seat compensation for %s: %w", workflowUUID, err)
			}
			log.Printf("[GHOST ALLOCATION RESOLVED] Dispatched compensation to release %d leaked seats for workflow %s (current state: %s)", len(egressPayload.SeatIDs), workflowUUID, wf.WorkflowState)
			return true, nil
		}

		return true, nil
	}

	currentPayload["booking"] = map[string]any{
		"status":    egressPayload.Status,
		"seat_ids":  egressPayload.SeatIDs,
		"num_seats": egressPayload.NumSeats,
	}
	currentPayload["seat_ids"] = egressPayload.SeatIDs
	currentPayload["num_seats"] = egressPayload.NumSeats

	updatedWorkflowPayload, err := json.Marshal(currentPayload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal updated workflow payload for %s: %w", workflowUUID, err)
	}

	visTimeout := int(shared_vars.PaymentEgressVisibilityTimeout)
	if visTimeout <= 0 {
		visTimeout = 30
	}
	paymentTimeout := shared_vars.PaymentTaskTimeoutSeconds
	if paymentTimeout <= 0 {
		paymentTimeout = visTimeout / 2
	}
	// Cascading rule: DB Timeout < Worker Processing (V/2) < SQS Visibility (V) < Orchestrator State Timeout (V + TaskTimeout)
	stateTimeoutSec := visTimeout + paymentTimeout

	numSeats := egressPayload.NumSeats
	if numSeats <= 0 && len(egressPayload.SeatIDs) > 0 {
		numSeats = len(egressPayload.SeatIDs)
	}
	amount := numSeats * 500

	paymentPayload := map[string]any{
		"workflow_id": workflowUUID.String(),
		"seat_ids":    egressPayload.SeatIDs,
		"num_seats":   numSeats,
		"amount":      amount,
		"updated_at":  time.Now().UTC().Format(time.RFC3339),
	}

	if pm, ok := currentPayload["payment_method"].(string); ok && pm != "" {
		paymentPayload["payment_method"] = pm
	} else if cd, ok := currentPayload["customer_details"].(map[string]any); ok {
		if pm, ok := cd["payment_method"].(string); ok && pm != "" {
			paymentPayload["payment_method"] = pm
		}
	}

	paymentPayloadBytes, err := json.Marshal(paymentPayload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal payment outbox payload for %s: %w", workflowUUID, err)
	}

	outboxID := uuid.New()
	updateWorkflowAndOutbox := fmt.Sprintf(`
		WITH updated_workflow AS (
			UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NOW() + INTERVAL '%d seconds', retry_count = 0 WHERE id = $3
			RETURNING id
		)
		INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
		SELECT $4, id, $5, $6, $7, NOW()
		FROM updated_workflow`, stateTimeoutSec)
	_, err = tx.Exec(ctx, updateWorkflowAndOutbox,
		shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT,
		updatedWorkflowPayload,
		workflowUUID,
		outboxID,
		shared_vars.PAYMENT_WORKER,
		paymentPayloadBytes,
		paymentTimeout,
	)
	if err != nil {
		return false, fmt.Errorf("failed to update workflow %s and insert payment outbox: %w", workflowUUID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
	}

	log.Printf("Workflow %s successfully transitioned to PAYMENT; outbox message %s created", workflowUUID, outboxID)
	return true, nil
}

func ProcessBookingEgressBatch(ctx context.Context, messages []*consumer.Message) {
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
					log.Printf("Recovered from panic processing booking egress message %s: %v", msg.ID, r)
				}
			}()

			shouldDelete, err := ProcessBookingEgressMessage(ctx, msg)
			if err != nil {
				log.Printf("Error processing booking egress message %s: %v", msg.ID, err)
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
		consumer.DeleteAllBatches(ctx, shared_vars.BookingEgressSQSClient, shared_vars.BookingEgressQueueURL, toDelete)
	}

	if len(toRelease) > 0 {
		consumer.ChangeVisibilityAllBatches(ctx, shared_vars.BookingEgressSQSClient, shared_vars.BookingEgressQueueURL, toRelease, 0)
	}
}
