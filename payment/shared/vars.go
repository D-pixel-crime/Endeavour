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

type PaymentWorkflowState string

const (
	PAYMENT_WORKFLOW_PENDING      PaymentWorkflowState = "PENDING"
	PAYMENT_WORKFLOW_COMPLETED    PaymentWorkflowState = "COMPLETED"
	PAYMENT_WORKFLOW_FAILED       PaymentWorkflowState = "FAILED"
	PAYMENT_WORKFLOW_COMPENSATING PaymentWorkflowState = "COMPENSATING"
)
