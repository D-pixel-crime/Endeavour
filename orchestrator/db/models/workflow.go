package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type WorkflowState string
type WorkflowType string

const (
	WORKFLOW_INITIALIZED  WorkflowState = "INITIALIZED"
	WORKFLOW_PENDING      WorkflowState = "PENDING"
	WORKFLOW_BOOKING      WorkflowState = "BOOKING"
	WORKFLOW_PAYMENT      WorkflowState = "PAYMENT"
	WORKFLOW_COMPLETED    WorkflowState = "COMPLETED"
	WORKFLOW_FAILED       WorkflowState = "FAILED"
	WORKFLOW_COMPENSATING WorkflowState = "COMPENSATING"
)

const (
	WORKFLOW_TYPE_TICKET_BOOKING WorkflowType = "TICKET_BOOKING"
)

type Workflow struct {
	ID            uuid.UUID       `db:"id" json:"id"`
	WorkflowType  WorkflowType    `db:"workflow_type" json:"workflow_type"`
	WorkflowState WorkflowState   `db:"workflow_state" json:"workflow_state"`
	Payload       json.RawMessage `db:"payload" json:"payload,omitempty"`
	CreatedAt     time.Time       `db:"created_at" json:"created_at"`
	ExpiresAt     *time.Time      `db:"expires_at" json:"expires_at,omitempty"`
	StateTimeout  *time.Time      `db:"state_timeout" json:"state_timeout,omitempty"`
}
