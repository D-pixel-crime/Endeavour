package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type WorkerType string

const (
	BOOKING_WORKER WorkerType = "BOOKING"
	PAYMENT_WORKER WorkerType = "PAYMENT"
)

type OutboxMessage struct {
	ID             uuid.UUID       `db:"id"`
	Processed      bool            `db:"processed"`
	WorkflowID     uuid.UUID       `db:"workflow_id"`
	WorkerType     WorkerType      `db:"worker_type"`
	Payload        json.RawMessage `db:"payload"`
	TimeoutSeconds int             `db:"timeout_seconds"` // Used by the worker to set context timeout upon starting
	CreatedAt      time.Time       `db:"created_at"`      // Crucial for the Orchestrator daemon to poll in order
}
