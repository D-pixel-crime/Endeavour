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

func resolveEgressSeatIDs(egressPayload PaymentEgressPayload, currentPayload map[string]any) []string {
	if len(egressPayload.SeatIDs) > 0 {
		return egressPayload.SeatIDs
	}
	return db.ExtractSeatIDs(currentPayload)
}

func ProcessPaymentEgressMessage(ctx context.Context, msg *consumer.Message) (bool, error) {
	egressPayload, _, ok := consumer.DecodePayload[PaymentEgressPayload](
		msg, "payment egress", func(p *PaymentEgressPayload) uuid.UUID { return p.WorkflowID },
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

	// 1. Payment Failed -> Check retries before going to backward compensation
	if egressPayload.Status == shared_vars.STATUS_FAILED {
		const maxRetries = 2
		timeoutSec, stateTimeoutSec := db.ComputeStageTimeouts(shared_vars.PaymentEgressVisibilityTimeout, shared_vars.PaymentTaskTimeoutSeconds)

		// A. Handling Failure during Backward Compensation (COMPENSATING_PAYMENT)
		if wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT {
			if wf.RetryCount < maxRetries {
				newRetryCount := wf.RetryCount + 1
				currentPayload["last_payment_compensation_failure"] = map[string]any{
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
					WorkerType:      shared_vars.PAYMENT_WORKER,
					OutboxPayload: map[string]any{
						"workflow_id":     workflowUUID.String(),
						"action":          shared_vars.ACTION_COMPENSATE,
						"reason":          fmt.Sprintf("Payment compensation retry (attempt %d/%d): %s", newRetryCount, maxRetries, egressPayload.Reason),
						"updated_at":      db.NewOutboxTimestamp(),
						"timeout_seconds": timeoutSec,
						"retry_count":     newRetryCount,
					},
					TaskTimeoutSec: timeoutSec,
				})
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
			seatIDs := resolveEgressSeatIDs(egressPayload, currentPayload)
			bookingTimeout, bookingStateTimeoutSec := db.ComputeStageTimeouts(shared_vars.BookingEgressVisibilityTimeout, shared_vars.BookingTaskTimeoutSeconds)

			currentPayload["payment_compensation_failure"] = map[string]any{
				"status":            egressPayload.Status,
				"reason":            egressPayload.Reason,
				"failed_at":         db.NewOutboxTimestamp(),
				"exhausted_retries": wf.RetryCount,
			}
			currentPayload["critical_operator_alert"] = fmt.Sprintf("Payment compensation failed after %d retries (%s); escalated to Booking seat release. Customer refund requires manual operator review.", wf.RetryCount, egressPayload.Reason)

			_, err := db.TransitionWorkflowWithOutbox(ctx, tx, db.TransitionParams{
				WorkflowID:      workflowUUID,
				NextState:       shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING,
				WorkflowPayload: currentPayload,
				StateTimeoutSec: bookingStateTimeoutSec,
				RetryCount:      0,
				WorkerType:      shared_vars.BOOKING_WORKER,
				OutboxPayload: map[string]any{
					"workflow_id":     workflowUUID.String(),
					"action":          shared_vars.ACTION_COMPENSATE,
					"seat_ids":        seatIDs,
					"reason":          fmt.Sprintf("Payment compensation failed after %d retries; escalating to release seats", wf.RetryCount),
					"updated_at":      db.NewOutboxTimestamp(),
					"timeout_seconds": bookingTimeout,
				},
				TaskTimeoutSec: bookingTimeout,
			})
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
		if wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT {
			log.Printf("Workflow %s payment failed message received, but workflow state is %s (expected PAYMENT or COMPENSATING_PAYMENT); skipping duplicate failure transition", workflowUUID, wf.WorkflowState)
			return true, nil
		}

		if wf.RetryCount < maxRetries {
			newRetryCount := wf.RetryCount + 1
			currentPayload["last_payment_failure"] = map[string]any{
				"status":    egressPayload.Status,
				"reason":    egressPayload.Reason,
				"failed_at": db.NewOutboxTimestamp(),
				"attempt":   newRetryCount,
			}

			seatIDs := resolveEgressSeatIDs(egressPayload, currentPayload)

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
				"workflow_id":     workflowUUID.String(),
				"seat_ids":        seatIDs,
				"num_seats":       numSeats,
				"amount":          amount,
				"updated_at":      db.NewOutboxTimestamp(),
				"timeout_seconds": timeoutSec,
				"retry_count":     newRetryCount,
			}
			if pm := db.ExtractPaymentMethod(currentPayload); pm != "" {
				paymentRetryPayload["payment_method"] = pm
			}

			_, err := db.TransitionWorkflowWithOutbox(ctx, tx, db.TransitionParams{
				WorkflowID:      workflowUUID,
				NextState:       wf.WorkflowState,
				WorkflowPayload: currentPayload,
				StateTimeoutSec: stateTimeoutSec,
				RetryCount:      newRetryCount,
				WorkerType:      shared_vars.PAYMENT_WORKER,
				OutboxPayload:   paymentRetryPayload,
				TaskTimeoutSec:  timeoutSec,
			})
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
			"failed_at":         db.NewOutboxTimestamp(),
			"exhausted_retries": wf.RetryCount,
		}

		_, err := db.TransitionWorkflowWithOutbox(ctx, tx, db.TransitionParams{
			WorkflowID:      workflowUUID,
			NextState:       shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT,
			WorkflowPayload: currentPayload,
			StateTimeoutSec: stateTimeoutSec,
			RetryCount:      0,
			WorkerType:      shared_vars.PAYMENT_WORKER,
			OutboxPayload: map[string]any{
				"workflow_id":     workflowUUID.String(),
				"action":          shared_vars.ACTION_COMPENSATE,
				"reason":          egressPayload.Reason,
				"updated_at":      db.NewOutboxTimestamp(),
				"timeout_seconds": timeoutSec,
			},
			TaskTimeoutSec: timeoutSec,
		})
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
		if wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT && wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT {
			log.Printf("Workflow %s payment compensated message received, but workflow state is %s (expected COMPENSATING_PAYMENT); skipping duplicate transition", workflowUUID, wf.WorkflowState)
			return true, nil
		}
		currentPayload["payment_compensation"] = map[string]any{
			"status":         egressPayload.Status,
			"compensated_at": db.NewOutboxTimestamp(),
		}

		bookingTimeout, stateTimeoutSec := db.ComputeStageTimeouts(shared_vars.BookingEgressVisibilityTimeout, shared_vars.BookingTaskTimeoutSeconds)
		seatIDs := resolveEgressSeatIDs(egressPayload, currentPayload)

		_, err := db.TransitionWorkflowWithOutbox(ctx, tx, db.TransitionParams{
			WorkflowID:      workflowUUID,
			NextState:       shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING,
			WorkflowPayload: currentPayload,
			StateTimeoutSec: stateTimeoutSec,
			RetryCount:      0,
			WorkerType:      shared_vars.BOOKING_WORKER,
			OutboxPayload: map[string]any{
				"workflow_id":     workflowUUID.String(),
				"action":          shared_vars.ACTION_COMPENSATE,
				"seat_ids":        seatIDs,
				"updated_at":      db.NewOutboxTimestamp(),
				"timeout_seconds": bookingTimeout,
			},
			TaskTimeoutSec: bookingTimeout,
		})
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

	paymentInfo := db.PaymentInfo{
		PaymentID:     egressPayload.PaymentID,
		Amount:        egressPayload.Amount,
		PaymentMethod: egressPayload.PaymentMethod,
		Status:        egressPayload.Status,
		SeatIDs:       egressPayload.SeatIDs,
		NumSeats:      egressPayload.NumSeats,
	}

	if wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_COMPLETED {
		existingTicket, err := db.GetTicketByWorkflowID(ctx, tx, workflowUUID)
		if err == nil {
			log.Printf("Workflow %s is already COMPLETED with ticket %s; skipping duplicate payment egress message", workflowUUID, existingTicket.ID)
			return true, nil
		}
		if err != pgx.ErrNoRows {
			return false, fmt.Errorf("failed to verify existing ticket for completed workflow %s: %w", workflowUUID, err)
		}

		t, err := db.GenerateAndStoreTicket(ctx, tx, wf, currentPayload, paymentInfo)
		if err != nil {
			return false, fmt.Errorf("failed to backfill missing ticket for completed workflow %s: %w", workflowUUID, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
		}
		log.Printf("Workflow %s was already COMPLETED without a ticket; created ticket %s", workflowUUID, t.ID)
		return true, nil
	}

	if wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT {
		log.Printf("Workflow %s is in state %s (expected PAYMENT); skipping ticket creation", workflowUUID, wf.WorkflowState)

		// Ghost charge prevention: Refund charge if payment completed after workflow timed out/escalated.
		if egressPayload.Amount > 0 || egressPayload.PaymentID != "" {
			_, err := db.InsertOutbox(ctx, tx, workflowUUID, shared_vars.PAYMENT_WORKER, map[string]any{
				"workflow_id":     workflowUUID.String(),
				"action":          shared_vars.ACTION_COMPENSATE,
				"reason":          fmt.Sprintf("Ghost charge refund: completed after workflow state was %s", wf.WorkflowState),
				"updated_at":      db.NewOutboxTimestamp(),
				"timeout_seconds": 15,
			}, 15)
			if err != nil {
				return false, fmt.Errorf("failed to insert ghost charge refund compensation outbox for %s: %w", workflowUUID, err)
			}
			if err := tx.Commit(ctx); err != nil {
				return false, fmt.Errorf("failed to commit ghost charge refund compensation for %s: %w", workflowUUID, err)
			}
			log.Printf("[GHOST CHARGE RESOLVED] Dispatched compensation to refund payment for workflow %s (current state: %s)", workflowUUID, wf.WorkflowState)
		}
		return true, nil
	}

	t, err := db.GenerateAndStoreTicket(ctx, tx, wf, currentPayload, paymentInfo)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
	}

	log.Printf("Workflow %s successfully transitioned to COMPLETED; ticket %s created", workflowUUID, t.ID)
	return true, nil
}
