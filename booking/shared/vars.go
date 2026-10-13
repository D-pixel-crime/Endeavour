package shared

import (
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	DBPool           *pgxpool.Pool
	IngressSQSClient *sqs.Client
	InitOnce         sync.Once
	IngressQueueURL  string
)

type BookingWorkflowState string

const (
	BOOKING_WORKFLOW_PENDING   BookingWorkflowState = "PENDING"
	BOOKING_WORKFLOW_COMPLETED BookingWorkflowState = "COMPLETED"
	BOOKING_WORKFLOW_FAILED    BookingWorkflowState = "FAILED"
)
