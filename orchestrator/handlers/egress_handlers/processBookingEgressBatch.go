package egresshandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/consumer"
	"github.com/D-pixel-crime/Endeavour/orchestrator/db/models"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type BookingEgressPayload struct {
	WorkflowID string   `json:"workflow_id"`
	Status     string   `json:"status"`
	SeatIDs    []string `json:"seat_ids"`
	NumSeats   int      `json:"num_seats"`
	Reason     string   `json:"reason,omitempty"`
}

func deleteFromBookingEgress(ctx context.Context, msg *consumer.Message) {
	if shared_vars.BookingEgressSQSClient == nil || shared_vars.BookingEgressQueueURL == "" {
		return
	}
	if err := consumer.DeleteMessage(ctx, shared_vars.BookingEgressSQSClient, shared_vars.BookingEgressQueueURL, msg.ReceiptHandle); err != nil {
		log.Printf("Failed to delete SQS message %s: %v", msg.ID, err)
	}
}

// ProcessBookingEgressMessage handles a single message from the booking egress queue.
// It returns (shouldDelete, error). If shouldDelete is true, the message can be removed from SQS.
func ProcessBookingEgressMessage(ctx context.Context, msg *consumer.Message) (bool, error) {
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

	var egressPayload BookingEgressPayload
	if err := json.Unmarshal(payloadBytes, &egressPayload); err != nil {
		return false, fmt.Errorf("failed to unmarshal booking egress payload (id=%s): %w", msg.ID, err)
	}

	if egressPayload.WorkflowID == "" && msg.ID != "" {
		egressPayload.WorkflowID = msg.ID
	}

	workflowUUID, err := uuid.Parse(egressPayload.WorkflowID)
	if err != nil {
		return false, fmt.Errorf("invalid workflow ID '%s' in message (id=%s): %w", egressPayload.WorkflowID, msg.ID, err)
	}

	// Currently only focus on successful transactions; skip failure cases for later
	if egressPayload.Status != "COMPLETED" {
		log.Printf("Workflow %s booking status is '%s' (not COMPLETED). Skipping failure handling for now.", workflowUUID, egressPayload.Status)
		return false, nil
	}

	if shared_vars.Orchestrator_DB_Pool == nil {
		return false, fmt.Errorf("database pool is not initialized")
	}

	tx, err := shared_vars.Orchestrator_DB_Pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Fetch existing workflow with row-level lock
	var wf models.Workflow
	queryWorkflow := `SELECT id, workflow_type, workflow_state, payload, created_at, expires_at, state_timeout FROM workflows WHERE id = $1 FOR UPDATE`
	err = tx.QueryRow(ctx, queryWorkflow, workflowUUID).Scan(
		&wf.ID,
		&wf.WorkflowType,
		&wf.WorkflowState,
		&wf.Payload,
		&wf.CreatedAt,
		&wf.ExpiresAt,
		&wf.StateTimeout,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, fmt.Errorf("workflow %s not found in workflows table", workflowUUID)
		}
		return false, fmt.Errorf("failed to query workflow %s: %w", workflowUUID, err)
	}

	// Idempotency check: if already transitioned to PAYMENT or COMPLETED, do not duplicate
	if wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT || wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_COMPLETED {
		log.Printf("Workflow %s is already in state %s; skipping duplicate processing", workflowUUID, wf.WorkflowState)
		return true, nil
	}

	// Unmarshal existing payload to preserve customer_details and merge booking results
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

	// Merge booking info into workflow payload
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

	// Update workflows table: transition state to PAYMENT and set next state timeout
	updateWorkflowQuery := `UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NOW() + INTERVAL '30 seconds' WHERE id = $3`
	_, err = tx.Exec(ctx, updateWorkflowQuery, shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT, updatedWorkflowPayload, workflowUUID)
	if err != nil {
		return false, fmt.Errorf("failed to update workflow %s: %w", workflowUUID, err)
	}

	// Calculate payment amount (cost per seat is 500)
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
	}

	// Pass customer details to payment worker
	if customerDetails, ok := currentPayload["customer_details"]; ok {
		paymentPayload["customer_details"] = customerDetails
	} else if email, ok := currentPayload["email"]; ok {
		paymentPayload["customer_details"] = map[string]any{
			"email":     email,
			"full_name": currentPayload["full_name"],
			"contact":   currentPayload["contact"],
		}
	}

	paymentPayloadBytes, err := json.Marshal(paymentPayload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal payment outbox payload for %s: %w", workflowUUID, err)
	}

	// Create outbox message for PAYMENT worker
	outboxID := uuid.New()
	outboxInsert := `INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, created_at) VALUES ($1, $2, $3, $4, 15, NOW())`
	_, err = tx.Exec(ctx, outboxInsert, outboxID, workflowUUID, shared_vars.PAYMENT_WORKER, paymentPayloadBytes)
	if err != nil {
		return false, fmt.Errorf("failed to insert outbox message for %s: %w", workflowUUID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
	}

	log.Printf("Workflow %s successfully transitioned to PAYMENT; outbox message %s created", workflowUUID, outboxID)
	return true, nil
}

// ProcessBookingEgressBatch processes a batch of messages from the booking egress SQS queue.
func ProcessBookingEgressBatch(ctx context.Context, messages []*consumer.Message) {
	for _, msg := range messages {
		if msg == nil {
			continue
		}

		shouldDelete, err := ProcessBookingEgressMessage(ctx, msg)
		if err != nil {
			log.Printf("Error processing booking egress message %s: %v", msg.ID, err)
			continue
		}

		if shouldDelete {
			deleteFromBookingEgress(ctx, msg)
		}
	}
}
