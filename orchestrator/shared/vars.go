package shared

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	Orchestrator_DB_Pool           *pgxpool.Pool
	BookingEgressSQSClient         *sqs.Client
	BookingEgressQueueURL          string
	BookingEgressVisibilityTimeout int32
	PaymentEgressSQSClient         *sqs.Client
	PaymentEgressQueueURL          string
	PaymentEgressVisibilityTimeout int32

	BookingTaskTimeoutSeconds int
	PaymentTaskTimeoutSeconds int
)

type WorkerType string
type Orchestrator_WorkflowState string
type WorkflowType string

const (
	BOOKING_WORKER WorkerType = "BOOKING"
	PAYMENT_WORKER WorkerType = "PAYMENT"
)

const (
	ORCHESTRATOR_WORKFLOW_BOOKING              Orchestrator_WorkflowState = "BOOKING"
	ORCHESTRATOR_WORKFLOW_PAYMENT              Orchestrator_WorkflowState = "PAYMENT"
	ORCHESTRATOR_WORKFLOW_COMPLETED            Orchestrator_WorkflowState = "COMPLETED"
	ORCHESTRATOR_WORKFLOW_FAILED               Orchestrator_WorkflowState = "FAILED"
	ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT Orchestrator_WorkflowState = "COMPENSATING_PAYMENT"
	ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING Orchestrator_WorkflowState = "COMPENSATING_BOOKING"
)

const (
	WORKFLOW_TYPE_TICKET_BOOKING WorkflowType = "TICKET_BOOKING"
)
