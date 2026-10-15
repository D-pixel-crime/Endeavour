package models

import (
	"time"

	"github.com/google/uuid"
)

type Payment struct {
	ID            uuid.UUID `db:"id" json:"id"`
	WorkflowID    uuid.UUID `db:"workflow_id" json:"workflow_id"`
	Amount        int       `db:"amount" json:"amount"`
	PaymentMethod string    `db:"payment_method" json:"payment_method"`
	Status        string    `db:"status" json:"status"`
	CreatedAt     time.Time `db:"created_at" json:"created_at"`
}
