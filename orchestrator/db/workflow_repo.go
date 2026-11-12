package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/D-pixel-crime/Endeavour/orchestrator/db/models"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RowQueryer abstracts pgxpool.Pool and pgx.Tx for single-row lookups.
type RowQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// GetWorkflowByID fetches a workflow row, optionally locking it FOR UPDATE.
func GetWorkflowByID(ctx context.Context, q RowQueryer, workflowID uuid.UUID, forUpdate bool) (*models.Workflow, error) {
	query := `SELECT id, workflow_type, workflow_state, payload, created_at, expires_at, state_timeout, retry_count FROM workflows WHERE id = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}

	var wf models.Workflow
	err := q.QueryRow(ctx, query, workflowID).Scan(
		&wf.ID,
		&wf.WorkflowType,
		&wf.WorkflowState,
		&wf.Payload,
		&wf.CreatedAt,
		&wf.ExpiresAt,
		&wf.StateTimeout,
		&wf.RetryCount,
	)
	if err != nil {
		return nil, err
	}
	return &wf, nil
}

// UnmarshalWorkflowPayload decodes raw JSON workflow payload into a non-nil map.
func UnmarshalWorkflowPayload(raw json.RawMessage) map[string]any {
	payload := make(map[string]any)
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &payload); err != nil || payload == nil {
			return make(map[string]any)
		}
	}
	return payload
}

// ExtractSeatIDs extracts seat IDs from top-level payload or nested booking metadata.
func ExtractSeatIDs(payload map[string]any) []string {
	var seatIDs []string
	if raw, ok := payload["seat_ids"].([]any); ok {
		for _, s := range raw {
			if str, ok := s.(string); ok {
				seatIDs = append(seatIDs, str)
			}
		}
	}
	if len(seatIDs) == 0 {
		if bookingMap, ok := payload["booking"].(map[string]any); ok {
			if raw, ok := bookingMap["seat_ids"].([]any); ok {
				for _, s := range raw {
					if str, ok := s.(string); ok {
						seatIDs = append(seatIDs, str)
					}
				}
			}
		}
	}
	return seatIDs
}

// ExtractPaymentMethod extracts payment_method from top-level payload or nested customer_details.
func ExtractPaymentMethod(payload map[string]any) string {
	if pm, ok := payload["payment_method"].(string); ok && pm != "" {
		return pm
	}
	if cd, ok := payload["customer_details"].(map[string]any); ok {
		if pm, ok := cd["payment_method"].(string); ok && pm != "" {
			return pm
		}
	}
	return ""
}

// ComputeStageTimeouts resolves task timeout and state timeout (visibility + task timeout) per cascading rules.
func ComputeStageTimeouts(visTimeoutSec int32, configuredTaskTimeout int) (taskTimeoutSec int, stateTimeoutSec int) {
	vis := int(visTimeoutSec)
	if vis <= 0 {
		vis = 30
	}
	task := configuredTaskTimeout
	if task <= 0 {
		task = vis / 2
	}
	return task, vis + task
}

// TransitionParams describes an atomic workflow state/retry update and outbox task insertion.
type TransitionParams struct {
	WorkflowID      uuid.UUID
	NextState       shared_vars.Orchestrator_WorkflowState
	WorkflowPayload map[string]any // nil keeps existing workflows.payload unchanged
	StateTimeoutSec int
	RetryCount      int
	WorkerType      shared_vars.WorkerType
	OutboxPayload   map[string]any
	TaskTimeoutSec  int
}

// TransitionWorkflowWithOutbox atomically updates workflows and inserts a new outbox event within tx.
func TransitionWorkflowWithOutbox(ctx context.Context, tx pgx.Tx, params TransitionParams) (uuid.UUID, error) {
	var wfPayloadArg any
	if params.WorkflowPayload != nil {
		b, err := json.Marshal(params.WorkflowPayload)
		if err != nil {
			return uuid.Nil, fmt.Errorf("failed to marshal workflow payload for %s: %w", params.WorkflowID, err)
		}
		wfPayloadArg = b
	}

	outboxBytes, err := json.Marshal(params.OutboxPayload)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to marshal outbox payload for %s: %w", params.WorkflowID, err)
	}

	outboxID := uuid.New()
	const query = `
		WITH updated_workflow AS (
			UPDATE workflows
			SET workflow_state = $1,
			    payload = COALESCE($2::jsonb, payload),
			    state_timeout = NOW() + make_interval(secs => $3),
			    retry_count = $4
			WHERE id = $5
			RETURNING id
		)
		INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
		SELECT $6, id, $7, $8, $9, NOW()
		FROM updated_workflow`

	if _, err := tx.Exec(ctx, query,
		params.NextState,
		wfPayloadArg,
		params.StateTimeoutSec,
		params.RetryCount,
		params.WorkflowID,
		outboxID,
		params.WorkerType,
		outboxBytes,
		params.TaskTimeoutSec,
	); err != nil {
		return uuid.Nil, err
	}
	return outboxID, nil
}

// InsertOutbox inserts an outbox task without modifying the workflow's state or timeout.
func InsertOutbox(ctx context.Context, tx pgx.Tx, workflowID uuid.UUID, workerType shared_vars.WorkerType, payload map[string]any, timeoutSec int) (uuid.UUID, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to marshal outbox payload for %s: %w", workflowID, err)
	}
	outboxID := uuid.New()
	const query = `INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at) VALUES ($1, $2, $3, $4, $5, NOW())`
	if _, err := tx.Exec(ctx, query, outboxID, workflowID, workerType, payloadBytes, timeoutSec); err != nil {
		return uuid.Nil, err
	}
	return outboxID, nil
}

// MarkWorkflowFailed atomically moves a workflow to terminal FAILED state and clears its timeout/retry counter.
func MarkWorkflowFailed(ctx context.Context, tx pgx.Tx, workflowID uuid.UUID, payload map[string]any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal failed payload for %s: %w", workflowID, err)
	}
	const query = `UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NULL, retry_count = 0 WHERE id = $3`
	_, err = tx.Exec(ctx, query, shared_vars.ORCHESTRATOR_WORKFLOW_FAILED, payloadBytes, workflowID)
	return err
}

// NewOutboxTimestamp returns the standard UTC RFC3339 timestamp string for outbox messages.
func NewOutboxTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}

