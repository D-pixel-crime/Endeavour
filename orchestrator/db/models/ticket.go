package models

import (
	"time"

	"github.com/google/uuid"
)

type Ticket struct {
	ID            uuid.UUID `db:"id" json:"id"`
	WorkflowID    uuid.UUID `db:"workflow_id" json:"workflow_id"`
	SeatIDs       []string  `db:"seat_ids" json:"seat_ids"`
	Amount        int       `db:"amount" json:"amount"`
	PaymentID     string    `db:"payment_id" json:"payment_id,omitempty"`
	CustomerName  string    `db:"customer_name" json:"customer_name,omitempty"`
	CustomerEmail string    `db:"customer_email" json:"customer_email,omitempty"`
	Status        string    `db:"status" json:"status"`
	CreatedAt     time.Time `db:"created_at" json:"created_at"`
}
