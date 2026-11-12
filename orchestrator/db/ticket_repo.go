package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/D-pixel-crime/Endeavour/orchestrator/db/models"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// PaymentInfo holds the completed payment details needed to build and persist a ticket.
type PaymentInfo struct {
	PaymentID     string
	Amount        int
	PaymentMethod string
	Status        string
	SeatIDs       []string
	NumSeats      int
}

// GetTicketByWorkflowID fetches an existing ticket for the given workflow ID.
func GetTicketByWorkflowID(ctx context.Context, db RowQueryer, workflowID uuid.UUID) (*models.Ticket, error) {
	query := `SELECT id, workflow_id, seat_ids, amount, COALESCE(payment_id, ''), COALESCE(customer_name, ''), COALESCE(customer_email, ''), status, created_at 
	          FROM tickets WHERE workflow_id = $1`

	var t models.Ticket
	err := db.QueryRow(ctx, query, workflowID).Scan(
		&t.ID, &t.WorkflowID, &t.SeatIDs, &t.Amount, &t.PaymentID, &t.CustomerName, &t.CustomerEmail, &t.Status, &t.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// ExtractPaymentInfoFromPayload reads previously recorded payment info from the workflow payload.
func ExtractPaymentInfoFromPayload(currentPayload map[string]any) (PaymentInfo, bool) {
	var info PaymentInfo
	rawPayment, ok := currentPayload["payment"].(map[string]any)
	if !ok {
		return info, false
	}

	if status, ok := rawPayment["status"].(string); ok {
		info.Status = status
	}
	if pid, ok := rawPayment["payment_id"].(string); ok {
		info.PaymentID = pid
	}
	if pm, ok := rawPayment["payment_method"].(string); ok {
		info.PaymentMethod = pm
	}
	if amt, ok := rawPayment["amount"].(float64); ok && amt > 0 {
		info.Amount = int(amt)
	}
	return info, info.Status == shared_vars.STATUS_COMPLETED
}

// GenerateAndStoreTicket creates a confirmed ticket from the workflow payload and payment info,
// upserts it into the tickets table, and marks the workflow as COMPLETED atomically within tx.
func GenerateAndStoreTicket(ctx context.Context, tx pgx.Tx, wf *models.Workflow, currentPayload map[string]any, paymentInfo PaymentInfo) (models.Ticket, error) {
	if currentPayload == nil {
		currentPayload = make(map[string]any)
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

	seatIDs := append([]string(nil), paymentInfo.SeatIDs...)
	if len(seatIDs) == 0 {
		seatIDs = ExtractSeatIDs(currentPayload)
	}
	if len(seatIDs) == 0 {
		numSeats := paymentInfo.NumSeats
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

	amount := paymentInfo.Amount
	if amount <= 0 {
		if a, ok := currentPayload["amount"].(float64); ok && a > 0 {
			amount = int(a)
		} else {
			amount = len(seatIDs) * 500
		}
	}

	paymentMethod := paymentInfo.PaymentMethod
	if paymentMethod == "" {
		paymentMethod = ExtractPaymentMethod(currentPayload)
	}

	status := paymentInfo.Status
	if status == "" {
		status = shared_vars.STATUS_COMPLETED
	}

	now := time.Now().UTC()
	t := models.Ticket{
		ID:            uuid.New(),
		WorkflowID:    wf.ID,
		SeatIDs:       seatIDs,
		Amount:        amount,
		PaymentID:     paymentInfo.PaymentID,
		CustomerName:  customerName,
		CustomerEmail: customerEmail,
		Status:        "CONFIRMED",
		CreatedAt:     now,
	}

	upsertTicketQuery := `
		INSERT INTO tickets (id, workflow_id, seat_ids, amount, payment_id, customer_name, customer_email, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (workflow_id) DO UPDATE
		SET seat_ids = EXCLUDED.seat_ids,
		    amount = EXCLUDED.amount,
		    payment_id = COALESCE(NULLIF(EXCLUDED.payment_id, ''), tickets.payment_id),
		    customer_name = COALESCE(NULLIF(EXCLUDED.customer_name, ''), tickets.customer_name),
		    customer_email = COALESCE(NULLIF(EXCLUDED.customer_email, ''), tickets.customer_email),
		    status = EXCLUDED.status
		RETURNING id, COALESCE(payment_id, ''), COALESCE(customer_name, ''), COALESCE(customer_email, ''), status, created_at`

	err := tx.QueryRow(ctx, upsertTicketQuery,
		t.ID,
		t.WorkflowID,
		t.SeatIDs,
		t.Amount,
		t.PaymentID,
		t.CustomerName,
		t.CustomerEmail,
		t.Status,
		t.CreatedAt,
	).Scan(&t.ID, &t.PaymentID, &t.CustomerName, &t.CustomerEmail, &t.Status, &t.CreatedAt)
	if err != nil {
		return models.Ticket{}, fmt.Errorf("failed to upsert ticket for workflow %s: %w", wf.ID, err)
	}

	currentPayload["payment"] = map[string]any{
		"status":         status,
		"amount":         amount,
		"payment_id":     t.PaymentID,
		"payment_method": paymentMethod,
	}
	currentPayload["ticket"] = t

	updatedWorkflowPayload, err := json.Marshal(currentPayload)
	if err != nil {
		return models.Ticket{}, fmt.Errorf("failed to marshal updated workflow payload for %s: %w", wf.ID, err)
	}

	updateWorkflowQuery := `
		UPDATE workflows
		SET workflow_state = $1,
		    payload = $2,
		    state_timeout = NULL,
		    retry_count = 0
		WHERE id = $3`
	if _, err := tx.Exec(ctx, updateWorkflowQuery,
		shared_vars.ORCHESTRATOR_WORKFLOW_COMPLETED,
		updatedWorkflowPayload,
		wf.ID,
	); err != nil {
		return models.Ticket{}, fmt.Errorf("failed to complete workflow %s: %w", wf.ID, err)
	}

	return t, nil
}

