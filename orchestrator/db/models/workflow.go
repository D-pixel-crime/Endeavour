package models

import (
	"encoding/json"
	"time"

	"github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/google/uuid"
)

type Workflow struct {
	ID            uuid.UUID            `db:"id" json:"id"`
	WorkflowType  shared.WorkflowType  `db:"workflow_type" json:"workflow_type"`
	WorkflowState shared.WorkflowState `db:"workflow_state" json:"workflow_state"`
	Payload       json.RawMessage      `db:"payload" json:"payload,omitempty"`
	CreatedAt     time.Time            `db:"created_at" json:"created_at"`
	ExpiresAt     *time.Time           `db:"expires_at" json:"expires_at,omitempty"`
	StateTimeout  *time.Time           `db:"state_timeout" json:"state_timeout,omitempty"`
}
