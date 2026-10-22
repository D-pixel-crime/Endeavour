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
	WorkflowID       string   `json:"workflow_id"`
	Status           string   `json:"status"`
	SeatIDs          []string `json:"seat_ids"`
	NumSeats         int      `json:"num_seats"`
	Reason           string   `json:"reason,omitempty"`
	UpdatedAt        string   `json:"updated_at,omitempty"`
	CreatedAt        string   `json:"created_at,omitempty"`
	TimeoutParameter int      `json:"timeout_parameter,omitempty"`
	TimeoutSeconds   int      `json:"timeout_seconds,omitempty"`
}

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

	if egressPayload.WorkflowID == "" {
		return false, fmt.Errorf("missing workflow ID in message payload (id=%s)", msg.ID)
	}

	workflowUUID, err := uuid.Parse(egressPayload.WorkflowID)
	if err != nil {
		return false, fmt.Errorf("invalid workflow ID '%s' in message (id=%s): %w", egressPayload.WorkflowID, msg.ID, err)
	}

	if egressPayload.Status != "COMPLETED" {
		log.Printf("Workflow %s booking status is '%s' (not COMPLETED). Skipping failure handling for now.", workflowUUID, egressPayload.Status)
		return true, nil
	}

	if shared_vars.Orchestrator_DB_Pool == nil {
		return false, fmt.Errorf("database pool is not initialized")
	}

	tx, err := shared_vars.Orchestrator_DB_Pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

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

	if wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_BOOKING && wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING {
		log.Printf("Workflow %s is already in state %s; skipping duplicate processing", workflowUUID, wf.WorkflowState)
		return true, nil
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

	paymentTimeout := shared_vars.PaymentTaskTimeoutSeconds
	if paymentTimeout <= 0 {
		paymentTimeout = 15
	}
	stateTimeoutSec := paymentTimeout + 5

	updateWorkflowQuery := fmt.Sprintf(`UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NOW() + INTERVAL '%d seconds' WHERE id = $3`, stateTimeoutSec)
	_, err = tx.Exec(ctx, updateWorkflowQuery, shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT, updatedWorkflowPayload, workflowUUID)
	if err != nil {
		return false, fmt.Errorf("failed to update workflow %s: %w", workflowUUID, err)
	}

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

	if customerDetails, ok := currentPayload["customer_details"]; ok {
		paymentPayload["customer_details"] = customerDetails
	} else if email, ok := currentPayload["email"]; ok {
		paymentPayload["customer_details"] = map[string]any{
			"email":     email,
			"full_name": currentPayload["full_name"],
			"contact":   currentPayload["contact"],
		}
	}

	if pm, ok := currentPayload["payment_method"]; ok {
		paymentPayload["payment_method"] = pm
	}

	paymentPayloadBytes, err := json.Marshal(paymentPayload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal payment outbox payload for %s: %w", workflowUUID, err)
	}

	outboxID := uuid.New()
	outboxInsert := `INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at) VALUES ($1, $2, $3, $4, $5, NOW())`
	_, err = tx.Exec(ctx, outboxInsert, outboxID, workflowUUID, shared_vars.PAYMENT_WORKER, paymentPayloadBytes, paymentTimeout)
	if err != nil {
		return false, fmt.Errorf("failed to insert outbox message for %s: %w", workflowUUID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
	}

	log.Printf("Workflow %s successfully transitioned to PAYMENT; outbox message %s created", workflowUUID, outboxID)
	return true, nil
}

func ProcessBookingEgressBatch(ctx context.Context, messages []*consumer.Message) {
	var toDelete []*consumer.Message

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
			toDelete = append(toDelete, msg)
		}
	}

	if len(toDelete) > 0 {
		consumer.DeleteAllBatches(ctx, shared_vars.BookingEgressSQSClient, shared_vars.BookingEgressQueueURL, toDelete)
	}
}
