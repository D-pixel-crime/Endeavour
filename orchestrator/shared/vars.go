package shared

import (
	"github.com/jackc/pgx/v5/pgxpool"
)

var Orchestrator_DB_Pool *pgxpool.Pool

type WorkerType string
type WorkflowState string
type WorkflowType string

const (
	BOOKING_WORKER WorkerType = "BOOKING"
	PAYMENT_WORKER WorkerType = "PAYMENT"
)

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
