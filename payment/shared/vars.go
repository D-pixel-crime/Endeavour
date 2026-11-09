package shared

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	DBPool                   *pgxpool.Pool
	IngressSQSClient         *sqs.Client
	IngressQueueURL          string
	IngressVisibilityTimeout int32
)

type PaymentWorkflowState string

const (
	PAYMENT_WORKFLOW_COMPLETED    PaymentWorkflowState = "COMPLETED"
	PAYMENT_WORKFLOW_FAILED       PaymentWorkflowState = "FAILED"
	PAYMENT_WORKFLOW_COMPENSATING PaymentWorkflowState = "COMPENSATING"
)

const (
	ACTION_COMPENSATE  = "COMPENSATE"
	STATUS_COMPLETED   = "COMPLETED"
	STATUS_FAILED      = "FAILED"
	STATUS_COMPENSATED = "COMPENSATED"
)
