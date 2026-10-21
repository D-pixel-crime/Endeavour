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
	WorkflowID       string   `json:"workflow_id"`
	Status           string   `json:"status"`
	Amount           int      `json:"amount"`
	PaymentMethod    string   `json:"payment_method,omitempty"`
	PaymentID        string   `json:"payment_id,omitempty"`
	SeatIDs          []string `json:"seat_ids,omitempty"`
	NumSeats         int      `json:"num_seats,omitempty"`
	Reason           string   `json:"reason,omitempty"`
	CreatedAt        string   `json:"created_at,omitempty"`
	TimeoutParameter int      `json:"timeout_parameter,omitempty"`
	TimeoutSeconds   int      `json:"timeout_seconds,omitempty"`
}

func ProcessPaymentEgressMessage(ctx context.Context, msg *consumer.Message) (bool, error) {
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

	var egressPayload PaymentEgressPayload
	if err := json.Unmarshal(payloadBytes, &egressPayload); err != nil {
		return false, fmt.Errorf("failed to unmarshal payment egress payload (id=%s): %w", msg.ID, err)
	}

	if egressPayload.WorkflowID == "" {
		return false, fmt.Errorf("missing workflow ID in message payload (id=%s)", msg.ID)
	}

	workflowUUID, err := uuid.Parse(egressPayload.WorkflowID)
	if err != nil {
		return false, fmt.Errorf("invalid workflow ID '%s' in message (id=%s): %w", egressPayload.WorkflowID, msg.ID, err)
	}

	if egressPayload.Status != "COMPLETED" {
		log.Printf("Workflow %s payment status is '%s' (not COMPLETED). Skipping failure handling for now.", workflowUUID, egressPayload.Status)
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

	if wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT && wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT {
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

	var customerName string
	var customerEmail string
	if cd, ok := currentPayload["customer_details"].(map[string]any); ok {
		if fn, ok := cd["full_name"].(string); ok {
			customerName = fn
		}
		if em, ok := cd["email"].(string); ok {
			customerEmail = em
		}
	}
	if customerName == "" {
		if fn, ok := currentPayload["full_name"].(string); ok {
			customerName = fn
		}
	}
	if customerEmail == "" {
		if em, ok := currentPayload["email"].(string); ok {
			customerEmail = em
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

	ticketInsertQuery := `
		INSERT INTO tickets (id, workflow_id, seat_ids, amount, payment_id, customer_name, customer_email, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (workflow_id) DO UPDATE
		SET seat_ids = EXCLUDED.seat_ids,
		    amount = EXCLUDED.amount,
		    payment_id = COALESCE(EXCLUDED.payment_id, tickets.payment_id),
		    customer_name = COALESCE(EXCLUDED.customer_name, tickets.customer_name),
		    customer_email = COALESCE(EXCLUDED.customer_email, tickets.customer_email),
		    status = EXCLUDED.status
		RETURNING id, created_at
	`

	err = tx.QueryRow(ctx, ticketInsertQuery, t.ID, t.WorkflowID, t.SeatIDs, t.Amount, t.PaymentID, t.CustomerName, t.CustomerEmail, t.Status, t.CreatedAt).Scan(&t.ID, &t.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("failed to insert ticket for workflow %s: %w", workflowUUID, err)
	}

	tickets := []models.Ticket{t}

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

	updateWorkflowQuery := `UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NULL WHERE id = $3`
	_, err = tx.Exec(ctx, updateWorkflowQuery, shared_vars.ORCHESTRATOR_WORKFLOW_COMPLETED, updatedWorkflowPayload, workflowUUID)
	if err != nil {
		return false, fmt.Errorf("failed to update workflow %s: %w", workflowUUID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction for workflow %s: %w", workflowUUID, err)
	}

	log.Printf("Workflow %s successfully transitioned to COMPLETED; %d ticket(s) created", workflowUUID, len(tickets))
	return true, nil
}

func ProcessPaymentEgressBatch(ctx context.Context, messages []*consumer.Message) {

	var toDelete []*consumer.Message

	for _, msg := range messages {
		if msg == nil {
			continue
		}

		shouldDelete, err := ProcessPaymentEgressMessage(ctx, msg)
		if err != nil {
			log.Printf("Error processing payment egress message %s: %v", msg.ID, err)
			continue
		}

		if shouldDelete {
			toDelete = append(toDelete, msg)
		}
	}

	if len(toDelete) > 0 {
		consumer.DeleteAllBatches(ctx, shared_vars.PaymentEgressSQSClient, shared_vars.PaymentEgressQueueURL, toDelete)
	}
}
