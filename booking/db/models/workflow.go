package models

import (
	"github.com/D-pixel-crime/Endeavour/booking/shared"
	"github.com/google/uuid"
)

type Workflow struct {
	ID            uuid.UUID                   `db:"id" json:"id"`
	WorkflowState shared.BookingWorkflowState `db:"workflow_state" json:"workflow_state"`
}
