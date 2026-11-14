package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/D-pixel-crime/Endeavour/orchestrator/db"
	"github.com/D-pixel-crime/Endeavour/orchestrator/db/models"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SweeperStats struct {
	TotalSweptWorkflows    uint64    `json:"total_swept_workflows"`
	FailedBookingTimeouts  uint64    `json:"failed_booking_timeouts"`
	CompensatedPayments    uint64    `json:"compensated_payments"`
	EscalatedCompensations uint64    `json:"escalated_compensations"`
	TotalSweepCycles       uint64    `json:"total_sweep_cycles"`
	TotalRetries           uint64    `json:"total_retries"`
	LastSweepTime          time.Time `json:"last_sweep_time"`
	LastError              string    `json:"last_error,omitempty"`
}

func (s *Sweeper) calculateStateTimeout() (taskTimeout int, stateTimeout int) {
	// Base timeout: taskTimeoutSec (worker ingress visibility is source of truth)
	// With queue-level redrive (up to R_in retries into DLQ before timeout compensation):
	// stateTimeout = (R_in * ingressVis) + (R_out * egressVis) + buffer
	taskTimeout = s.taskTimeoutSec
	stateTimeout = (s.ingressMaxRetries * s.taskTimeoutSec) + (s.egressMaxRetries * s.taskTimeoutSec) + s.stageTimeoutBufferSec
	return taskTimeout, stateTimeout
}

type Sweeper struct {
	dbPool                *pgxpool.Pool
	interval              time.Duration
	batchSize             int
	taskTimeoutSec        int
	ingressMaxRetries     int
	egressMaxRetries      int
	stageTimeoutBufferSec int
	stats                 SweeperStats
	statsMu               sync.RWMutex
}

func NewSweeper(dbPool *pgxpool.Pool, interval time.Duration, batchSize int, taskTimeoutSec int, ingressRetries ...int) *Sweeper {
	inRetries := 5
	egRetries := 3
	bufSec := 10
	if len(ingressRetries) > 0 && ingressRetries[0] > 0 {
		inRetries = ingressRetries[0]
	}
	if len(ingressRetries) > 1 && ingressRetries[1] > 0 {
		egRetries = ingressRetries[1]
	}
	if len(ingressRetries) > 2 && ingressRetries[2] >= 0 {
		bufSec = ingressRetries[2]
	}

	return &Sweeper{
		dbPool:                dbPool,
		interval:              interval,
		batchSize:             batchSize,
		taskTimeoutSec:        taskTimeoutSec,
		ingressMaxRetries:     inRetries,
		egressMaxRetries:      egRetries,
		stageTimeoutBufferSec: bufSec,
	}
}

func (s *Sweeper) GetStats() SweeperStats {
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	return s.stats
}

func (s *Sweeper) recordStat(fn func(*SweeperStats)) {
	s.statsMu.Lock()
	fn(&s.stats)
	s.statsMu.Unlock()
}

func (s *Sweeper) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	log.Printf("[SWEEPER] Started timeout sweeper daemon (interval: %v, batch size: %d, task timeout: %ds)",
		s.interval, s.batchSize, s.taskTimeoutSec)

	for {
		select {
		case <-ctx.Done():
			log.Println("[SWEEPER] Shutting down timeout sweeper cleanly...")
			return
		case <-ticker.C:
			count, err := s.Sweep(ctx)
			s.recordStat(func(st *SweeperStats) {
				st.TotalSweepCycles++
				st.LastSweepTime = time.Now().UTC()
				if err != nil {
					st.LastError = err.Error()
					log.Printf("[SWEEPER ERROR] Sweep cycle failed: %v", err)
				} else {
					st.LastError = ""
					if count > 0 {
						log.Printf("[SWEEPER] Successfully swept and processed %d timed-out workflow(s)", count)
					}
				}
			})
		}
	}
}

func (s *Sweeper) Sweep(ctx context.Context) (int, error) {
	sweepCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	tx, err := s.dbPool.Begin(sweepCtx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(sweepCtx)

	const query = `
		SELECT id, workflow_type, workflow_state, payload, created_at, expires_at, state_timeout, retry_count
		FROM workflows
		WHERE state_timeout IS NOT NULL AND state_timeout < NOW()
		ORDER BY state_timeout ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED;
	`

	rows, err := tx.Query(sweepCtx, query, s.batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to query expired workflows: %w", err)
	}
	defer rows.Close()

	var workflows []models.Workflow
	for rows.Next() {
		var wf models.Workflow
		if err := rows.Scan(
			&wf.ID,
			&wf.WorkflowType,
			&wf.WorkflowState,
			&wf.Payload,
			&wf.CreatedAt,
			&wf.ExpiresAt,
			&wf.StateTimeout,
			&wf.RetryCount,
		); err != nil {
			log.Printf("[SWEEPER] Error scanning workflow: %v", err)
			continue
		}
		workflows = append(workflows, wf)
	}
	rows.Close()

	if len(workflows) == 0 {
		return 0, nil
	}

	for _, wf := range workflows {
		currentPayload := db.UnmarshalWorkflowPayload(wf.Payload)

		switch wf.WorkflowState {
		case shared_vars.ORCHESTRATOR_WORKFLOW_BOOKING:
			// Booking step timed out after queue retries exhausted.
			// Run speculative compensation in case seats were booked in the worker DB but egress was delayed or dropped.
			taskTimeout, stateTimeout := s.calculateStateTimeout()
			seatIDs := db.ExtractSeatIDs(currentPayload)

			currentPayload["timeout"] = map[string]any{
				"timed_out_state": string(shared_vars.ORCHESTRATOR_WORKFLOW_BOOKING),
				"timed_out_at":    db.NewOutboxTimestamp(),
				"reason":          "Booking step exceeded state_timeout after queue retries exhausted; initiating speculative compensation",
			}

			_, err := db.TransitionWorkflowWithOutbox(sweepCtx, tx, db.TransitionParams{
				WorkflowID:      wf.ID,
				NextState:       shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING,
				WorkflowPayload: currentPayload,
				StateTimeoutSec: stateTimeout,
				RetryCount:      0,
				WorkerType:      shared_vars.BOOKING_WORKER,
				OutboxPayload: map[string]any{
					"workflow_id":     wf.ID.String(),
					"action":          shared_vars.ACTION_COMPENSATE,
					"seat_ids":        seatIDs,
					"reason":          "Booking step timed out after queue retries exhausted; releasing any booked seats",
					"updated_at":      db.NewOutboxTimestamp(),
					"timeout_seconds": taskTimeout,
				},
				TaskTimeoutSec: taskTimeout,
			})
			if err != nil {
				log.Printf("[SWEEPER] Error updating workflow %s and inserting booking compensation outbox: %v", wf.ID, err)
				continue
			}

			s.recordStat(func(st *SweeperStats) {
				st.FailedBookingTimeouts++
				st.TotalSweptWorkflows++
			})
			log.Printf("[SWEEPER] Workflow %s timed out in BOOKING after queue retries; initiated speculative compensation at BOOKING", wf.ID)

		case shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT:
			// Payment step timed out after queue retries exhausted.
			// Initiate backward compensation at PAYMENT (cascading to BOOKING).
			taskTimeout, stateTimeout := s.calculateStateTimeout()

			currentPayload["timeout"] = map[string]any{
				"timed_out_state": string(shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT),
				"timed_out_at":    db.NewOutboxTimestamp(),
				"reason":          "Payment step exceeded state_timeout after queue retries exhausted; initiating backward compensation",
			}

			_, err := db.TransitionWorkflowWithOutbox(sweepCtx, tx, db.TransitionParams{
				WorkflowID:      wf.ID,
				NextState:       shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT,
				WorkflowPayload: currentPayload,
				StateTimeoutSec: stateTimeout,
				RetryCount:      0,
				WorkerType:      shared_vars.PAYMENT_WORKER,
				OutboxPayload: map[string]any{
					"workflow_id":     wf.ID.String(),
					"action":          shared_vars.ACTION_COMPENSATE,
					"reason":          "Payment step exceeded state_timeout after queue retries exhausted; initiating backward compensation",
					"updated_at":      db.NewOutboxTimestamp(),
					"timeout_seconds": taskTimeout,
				},
				TaskTimeoutSec: taskTimeout,
			})
			if err != nil {
				log.Printf("[SWEEPER] Error updating workflow %s and inserting payment compensation outbox: %v", wf.ID, err)
				continue
			}

			s.recordStat(func(st *SweeperStats) {
				st.CompensatedPayments++
				st.TotalSweptWorkflows++
			})
			log.Printf("[SWEEPER] Workflow %s timed out in PAYMENT after queue retries; initiated backward compensation at PAYMENT", wf.ID)

		case shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT:
			// Escalate directly to BOOKING compensation so reserved seats are released.
			seatIDs := db.ExtractSeatIDs(currentPayload)
			taskTimeout, stateTimeout := s.calculateStateTimeout()

			currentPayload["timeout_escalation"] = map[string]any{
				"escalated_from": string(shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT),
				"escalated_at":   db.NewOutboxTimestamp(),
				"reason":         "Payment compensation timed out after queue retries exhausted; escalating directly to release booked seats",
			}

			_, err := db.TransitionWorkflowWithOutbox(sweepCtx, tx, db.TransitionParams{
				WorkflowID:      wf.ID,
				NextState:       shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING,
				WorkflowPayload: currentPayload,
				StateTimeoutSec: stateTimeout,
				RetryCount:      0,
				WorkerType:      shared_vars.BOOKING_WORKER,
				OutboxPayload: map[string]any{
					"workflow_id":     wf.ID.String(),
					"action":          shared_vars.ACTION_COMPENSATE,
					"seat_ids":        seatIDs,
					"reason":          "Payment compensation timed out; sweeping to release seats",
					"updated_at":      db.NewOutboxTimestamp(),
					"timeout_seconds": taskTimeout,
				},
				TaskTimeoutSec: taskTimeout,
			})
			if err != nil {
				log.Printf("[SWEEPER] Error updating workflow %s and inserting booking compensation outbox: %v", wf.ID, err)
				continue
			}

			s.recordStat(func(st *SweeperStats) {
				st.EscalatedCompensations++
				st.TotalSweptWorkflows++
			})
			log.Printf("[SWEEPER] Workflow %s timed out in COMPENSATING_PAYMENT after queue retries; escalated to release seats in BOOKING", wf.ID)

		case shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING:
			// Booking compensation timed out after queue retries exhausted.
			// Mark terminal FAILED with critical operator alert for manual seat audit.
			currentPayload["critical_alert"] = "Booking compensation timed out after queue retries exhausted. Manual operator intervention required to release seats."
			if err := db.MarkWorkflowFailed(sweepCtx, tx, wf.ID, currentPayload); err != nil {
				log.Printf("[SWEEPER] Error marking %s FAILED after booking compensation timeout: %v", wf.ID, err)
				continue
			}

			s.recordStat(func(st *SweeperStats) {
				st.TotalSweptWorkflows++
			})
			log.Printf("[CRITICAL OPERATOR ALERT] Workflow %s timed out in COMPENSATING_BOOKING after queue retries; manual seat audit required", wf.ID)

		case shared_vars.ORCHESTRATOR_WORKFLOW_COMPLETED, shared_vars.ORCHESTRATOR_WORKFLOW_FAILED:
			// Workflow already terminal, clear leftover state_timeout
			_, _ = tx.Exec(sweepCtx, `UPDATE workflows SET state_timeout = NULL, retry_count = 0 WHERE id = $1`, wf.ID)

		default:
			log.Printf("[SWEEPER] Workflow %s in unhandled state %s with expired timeout", wf.ID, wf.WorkflowState)
		}
	}

	if err := tx.Commit(sweepCtx); err != nil {
		return 0, fmt.Errorf("failed to commit sweep transaction: %w", err)
	}

	return len(workflows), nil
}
