package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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
	WorkflowID      string           `json:"workflow_id"`
	Amount          int              `json:"amount"`
	NumSeats        int              `json:"num_seats"`
	SeatIDs         []string         `json:"seat_ids"`
	PaymentMethod   string           `json:"payment_method"`
	CustomerDetails *CustomerDetails `json:"customer_details,omitempty"`
}

func deleteBatchFromIngress(ctx context.Context, messages []*consumer.Message) {
	if len(messages) == 0 || shared.IngressSQSClient == nil || shared.IngressQueueURL == "" {
		return
	}

	for i := 0; i < len(messages); i += 10 {
		end := min(i+10, len(messages))
		chunk := messages[i:end]

		result, err := consumer.DeleteMessageBatch(ctx, shared.IngressSQSClient, shared.IngressQueueURL, chunk)
		if err != nil {
			log.Printf("Failed to batch delete payment ingress SQS messages: %v", err)
			continue
		}

		if len(result.Failed) > 0 {
			for _, f := range result.Failed {
				log.Printf("Failed to delete payment ingress SQS message (id=%s, code=%s): %s", f.ID, f.Code, f.Message)
			}
		}
	}
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

	if ingressPayload.WorkflowID == "" && msg.ID != "" {
		ingressPayload.WorkflowID = msg.ID
	}

	workflowUUID, err := uuid.Parse(ingressPayload.WorkflowID)
	if err != nil {
		return false, fmt.Errorf("invalid workflow ID '%s' in message (id=%s): %w", ingressPayload.WorkflowID, msg.ID, err)
	}

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

	if shared.DBPool == nil {
		return false, fmt.Errorf("database pool is not initialized")
	}

	tx, err := shared.DBPool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var existingState string
	err = tx.QueryRow(ctx, `SELECT workflow_state FROM workflows WHERE id = $1 FOR UPDATE`, workflowUUID).Scan(&existingState)
	if err == nil {
		if existingState == string(shared.PAYMENT_WORKFLOW_COMPLETED) {
			log.Printf("Workflow %s is already in state %s; skipping duplicate payment processing", workflowUUID, existingState)
			return true, nil
		}
	}

	log.Printf("[PAYMENT PROCESSING] Initiating payment for workflow %s: amount=%d, method=%s", workflowUUID, amount, paymentMethod)
	if ingressPayload.CustomerDetails != nil {
		log.Printf("[PAYMENT PROCESSING] Customer details: email=%s, full_name=%s",
			ingressPayload.CustomerDetails.Email, ingressPayload.CustomerDetails.FullName)
	}
	log.Printf("[PAYMENT SUCCESS] Payment of %d successfully completed via %s for workflow %s",
		amount, paymentMethod, workflowUUID)

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
		return false, fmt.Errorf("failed to insert payment record: %w", err)
	}

	workflowState := shared.PAYMENT_WORKFLOW_COMPLETED
	_, err = tx.Exec(ctx,
		`INSERT INTO workflows (id, workflow_state, payload)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO UPDATE
		   SET workflow_state = EXCLUDED.workflow_state,
		       payload = EXCLUDED.payload`,
		workflowUUID, string(workflowState), payloadBytes,
	)
	if err != nil {
		return false, fmt.Errorf("failed to upsert workflow: %w", err)
	}

	outboxID := uuid.New()
	outboxPayload := map[string]any{
		"workflow_id":    workflowUUID.String(),
		"status":         "COMPLETED",
		"amount":         amount,
		"payment_method": paymentMethod,
		"payment_id":     paymentID.String(),
		"seat_ids":       ingressPayload.SeatIDs,
		"num_seats":      ingressPayload.NumSeats,
	}
	outboxPayloadBytes, err := json.Marshal(outboxPayload)
	if err != nil {
		return false, fmt.Errorf("failed to marshal outbox payload: %w", err)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (id, workflow_id, worker_type, payload, created_at)
		 VALUES ($1, $2, 'PAYMENT', $3, NOW())`,
		outboxID, workflowUUID, outboxPayloadBytes,
	)
	if err != nil {
		return false, fmt.Errorf("failed to insert outbox entry: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("Workflow %s payment processed: state=%s amount=%d; outbox message %s created",
		workflowUUID, workflowState, amount, outboxID)
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
		deleteBatchFromIngress(ctx, toDelete)
	}
}
