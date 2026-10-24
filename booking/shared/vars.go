package shared

import (
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	DBPool                   *pgxpool.Pool
	IngressSQSClient         *sqs.Client
	InitOnce                 sync.Once
	IngressQueueURL          string
	IngressVisibilityTimeout int32
)

type BookingWorkflowState string

const (
	BOOKING_WORKFLOW_PENDING      BookingWorkflowState = "PENDING"
	BOOKING_WORKFLOW_COMPLETED    BookingWorkflowState = "COMPLETED"
	BOOKING_WORKFLOW_FAILED       BookingWorkflowState = "FAILED"
	BOOKING_WORKFLOW_COMPENSATING BookingWorkflowState = "COMPENSATING"
)

const (
	ACTION_COMPENSATE  = "COMPENSATE"
	STATUS_COMPLETED   = "COMPLETED"
	STATUS_FAILED      = "FAILED"
	STATUS_COMPENSATED = "COMPENSATED"
)
