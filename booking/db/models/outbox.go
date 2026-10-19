package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type OutboxMessage struct {
	ID             uuid.UUID       `db:"id"`
	WorkflowID     uuid.UUID       `db:"workflow_id"`
	WorkerType     string          `db:"worker_type"`
	Payload        json.RawMessage `db:"payload"`
	TimeoutSeconds int             `db:"timeout_seconds"`
	CreatedAt      time.Time       `db:"created_at"`
}
