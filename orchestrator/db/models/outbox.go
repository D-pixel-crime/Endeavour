package models

import (
	"encoding/json"
	"time"

	"github.com/D-pixel-crime/Endeavor/orchestrator/shared"
	"github.com/google/uuid"
)

type OutboxMessage struct {
	ID             uuid.UUID         `db:"id"`
	Processed      bool              `db:"processed"`
	WorkflowID     uuid.UUID         `db:"workflow_id"`
	WorkerType     shared.WorkerType `db:"worker_type"`
	Payload        json.RawMessage   `db:"payload"`
	TimeoutSeconds int               `db:"timeout_seconds"` // Used by the worker to set context timeout upon starting
	CreatedAt      time.Time         `db:"created_at"`      // Crucial for the Orchestrator daemon to poll in order
}
