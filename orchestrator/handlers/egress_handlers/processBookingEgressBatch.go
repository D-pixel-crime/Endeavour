package egresshandlers

import (
	"context"
	"fmt"
	"log"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	"github.com/D-pixel-crime/Endeavour/orchestrator/db"
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
	egressPayload, _, ok := consumer.DecodePayload(
		msg, "booking egress", func(p *BookingEgressPayload) uuid.UUID { return p.WorkflowID },
	)
	if !ok {
		return true, nil
	}

	if shared_vars.Orchestrator_DB_Pool == nil {
		return false, fmt.Errorf("database pool is not initialized")
	}

	workflowUUID := egressPayload.WorkflowID

	tx, err := shared_vars.Orchestrator_DB_Pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	wf, err := db.GetWorkflowByID(ctx, tx, workflowUUID, true)
	if err != nil {
		if err == pgx.ErrNoRows {
			log.Printf("[POISON PILL DETECTED] Workflow %s not found in workflows table; acknowledging message %s to prevent FIFO HoL blocking", workflowUUID, msg.ID)
			return true, nil
		}
		return false, fmt.Errorf("failed to query workflow %s: %w", workflowUUID, err)
	}

	currentPayload := db.UnmarshalWorkflowPayload(wf.Payload)

	// 1. Booking Failed -> Check retries before marking whole workflow as FAILED
	if egressPayload.Status == shared_vars.STATUS_FAILED {
		const maxRetries = 1
		bookingTimeout, stateTimeoutSec := db.ComputeStageTimeouts(shared_vars.BookingIngressVisibilityTimeout, shared_vars.BookingEgressVisibilityTimeout)

		// A. Handling Failure during Backward Compensation (COMPENSATING_BOOKING)
		if wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING {
			seatIDs := egressPayload.SeatIDs
			if len(seatIDs) == 0 {
				seatIDs = db.ExtractSeatIDs(currentPayload)
			}

			if wf.RetryCount < maxRetries {
				newRetryCount := wf.RetryCount + 1
				currentPayload["last_booking_compensation_failure"] = map[string]any{
					"status":    egressPayload.Status,
					"reason":    egressPayload.Reason,
					"failed_at": db.NewOutboxTimestamp(),
					"attempt":   newRetryCount,
				}

				_, err := db.TransitionWorkflowWithOutbox(ctx, tx, db.TransitionParams{
					WorkflowID:      workflowUUID,
					NextState:       wf.WorkflowState,
					WorkflowPayload: currentPayload,
					StateTimeoutSec: stateTimeoutSec,
					RetryCount:      newRetryCount,
					WorkerType:      shared_vars.BOOKING_WORKER,
					OutboxPayload: map[string]any{
						"workflow_id":     workflowUUID.String(),
						"action":          shared_vars.ACTION_COMPENSATE,
						"seat_ids":        seatIDs,
						"reason":          fmt.Sprintf("Booking compensation retry (attempt %d/%d): %s", newRetryCount, maxRetries, egressPayload.Reason),
						"updated_at":      db.NewOutboxTimestamp(),
						"timeout_seconds": bookingTimeout,
						"retry_count":     newRetryCount,
					},
					TaskTimeoutSec: bookingTimeout,
				})
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

			// Booking compensation retries exhausted (>= 2): mark terminal FAILED with critical operator alert.
			currentPayload["booking_compensation_failure"] = map[string]any{
				"status":            egressPayload.Status,
				"reason":            egressPayload.Reason,
				"failed_at":         db.NewOutboxTimestamp(),
				"exhausted_retries": wf.RetryCount,
			}
			currentPayload["critical_operator_alert"] = fmt.Sprintf("Booking compensation failed after %d retries (%s); reserved seats (%v) may be leaked and require manual release.", wf.RetryCount, egressPayload.Reason, seatIDs)

			if err := db.MarkWorkflowFailed(ctx, tx, workflowUUID, currentPayload); err != nil {
				return false, fmt.Errorf("failed to update workflow %s to FAILED: %w", workflowUUID, err)
			}
			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
			}

			log.Printf("[CRITICAL ALERT] Booking compensation failed for %s after exhausting %d retries; marked as FAILED with operator alert", workflowUUID, wf.RetryCount)
			return true, nil
		}

		// B. Handling Failure during Forward Step (BOOKING)
		if wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_FAILED {
			log.Printf("Workflow %s booking failed message received, but workflow is already FAILED; skipping duplicate transition", workflowUUID)
			return true, nil
		}
		if wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_BOOKING {
			log.Printf("Workflow %s booking failed message received, but workflow state is %s (expected BOOKING or COMPENSATING_BOOKING); skipping", workflowUUID, wf.WorkflowState)
			return true, nil
		}

		if wf.RetryCount < maxRetries {
			newRetryCount := wf.RetryCount + 1
			currentPayload["last_booking_failure"] = map[string]any{
				"status":    egressPayload.Status,
				"reason":    egressPayload.Reason,
				"failed_at": db.NewOutboxTimestamp(),
				"attempt":   newRetryCount,
			}

			numSeats := 0
			if ns, ok := currentPayload["num_seats"].(float64); ok && ns > 0 {
				numSeats = int(ns)
			} else if egressPayload.NumSeats > 0 {
				numSeats = egressPayload.NumSeats
			} else if seatIDs := db.ExtractSeatIDs(currentPayload); len(seatIDs) > 0 {
				numSeats = len(seatIDs)
			}

			_, err := db.TransitionWorkflowWithOutbox(ctx, tx, db.TransitionParams{
				WorkflowID:      workflowUUID,
				NextState:       wf.WorkflowState,
				WorkflowPayload: currentPayload,
				StateTimeoutSec: stateTimeoutSec,
				RetryCount:      newRetryCount,
				WorkerType:      shared_vars.BOOKING_WORKER,
				OutboxPayload: map[string]any{
					"workflow_id":     workflowUUID.String(),
					"num_seats":       numSeats,
					"updated_at":      db.NewOutboxTimestamp(),
					"timeout_seconds": bookingTimeout,
					"retry_count":     newRetryCount,
				},
				TaskTimeoutSec: bookingTimeout,
			})
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
			"failed_at":         db.NewOutboxTimestamp(),
			"exhausted_retries": wf.RetryCount,
		}
		if err := db.MarkWorkflowFailed(ctx, tx, workflowUUID, currentPayload); err != nil {
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
		if wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_FAILED {
			log.Printf("Workflow %s booking compensated message received, but workflow is already FAILED; skipping duplicate transition", workflowUUID)
			return true, nil
		}
		currentPayload["booking_compensation"] = map[string]any{
			"status":         egressPayload.Status,
			"compensated_at": db.NewOutboxTimestamp(),
		}
		if err := db.MarkWorkflowFailed(ctx, tx, workflowUUID, currentPayload); err != nil {
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

		// Ghost allocation prevention: Release seats if booking completed after workflow timed out/failed.
		if len(egressPayload.SeatIDs) > 0 {
			_, err := db.InsertOutbox(ctx, tx, workflowUUID, shared_vars.BOOKING_WORKER, map[string]any{
				"workflow_id":     workflowUUID.String(),
				"action":          shared_vars.ACTION_COMPENSATE,
				"seat_ids":        egressPayload.SeatIDs,
				"reason":          fmt.Sprintf("Ghost allocation release: completed after workflow state was %s", wf.WorkflowState),
				"updated_at":      db.NewOutboxTimestamp(),
				"timeout_seconds": 15,
			}, 15)
			if err != nil {
				return false, fmt.Errorf("failed to insert ghost seat compensation outbox for %s: %w", workflowUUID, err)
			}
			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("failed to commit ghost seat compensation for %s: %w", workflowUUID, err)
			}
			log.Printf("[GHOST ALLOCATION RESOLVED] Dispatched compensation to release %d leaked seats for workflow %s (current state: %s)", len(egressPayload.SeatIDs), workflowUUID, wf.WorkflowState)
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

	paymentTimeout, stateTimeoutSec := db.ComputeStageTimeouts(shared_vars.PaymentIngressVisibilityTimeout, shared_vars.PaymentEgressVisibilityTimeout)

	numSeats := egressPayload.NumSeats
	if numSeats <= 0 && len(egressPayload.SeatIDs) > 0 {
		numSeats = len(egressPayload.SeatIDs)
	}
	paymentPayload := map[string]any{
		"workflow_id": workflowUUID.String(),
		"seat_ids":    egressPayload.SeatIDs,
		"num_seats":   numSeats,
		"amount":      numSeats * 500,
		"updated_at":  db.NewOutboxTimestamp(),
	}
	if pm := db.ExtractPaymentMethod(currentPayload); pm != "" {
		paymentPayload["payment_method"] = pm
	}

	outboxID, err := db.TransitionWorkflowWithOutbox(ctx, tx, db.TransitionParams{
		WorkflowID:      workflowUUID,
		NextState:       shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT,
		WorkflowPayload: currentPayload,
		StateTimeoutSec: stateTimeoutSec,
		RetryCount:      0,
		WorkerType:      shared_vars.PAYMENT_WORKER,
		OutboxPayload:   paymentPayload,
		TaskTimeoutSec:  paymentTimeout,
	})
	if err != nil {
		return false, fmt.Errorf("failed to update workflow %s and insert payment outbox: %w", workflowUUID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
	}

	log.Printf("Workflow %s successfully transitioned to PAYMENT; outbox message %s created", workflowUUID, outboxID)
	return true, nil
}
