package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type WorkflowState string

const (
	WORKFLOW_INITIALIZED  WorkflowState = "INITIALIZED"
	WORKFLOW_PENDING      WorkflowState = "PENDING"
	WORKFLOW_BOOKING      WorkflowState = "BOOKING"
	WORKFLOW_PAYMENT      WorkflowState = "PAYMENT"
	WORKFLOW_COMPLETED    WorkflowState = "COMPLETED"
	WORKFLOW_FAILED       WorkflowState = "FAILED"
	WORKFLOW_COMPENSATING WorkflowState = "COMPENSATING"
)

type Workflow struct {
	ID           uuid.UUID       `db:"id" json:"id"`
	WorkflowType string          `db:"workflow_type" json:"workflow_type"`
	Status       WorkflowState   `db:"status" json:"status"`
	Payload      json.RawMessage `db:"payload" json:"payload,omitempty"`
	CreatedAt    time.Time       `db:"created_at" json:"created_at"`
	ExpiresAt    *time.Time      `db:"expires_at" json:"expires_at,omitempty"`
}
