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

const MaxRetries = 1

func (s *Sweeper) calculateRetryTimeout(retryCount int) (taskTimeout int, stateTimeout int) {
	// Base timeout: taskTimeoutSec (worker ingress visibility is source of truth)
	// stateTimeout = 2 * ingressVis + 2 * egressVis + 10s queue lag buffer
	taskTimeout = s.taskTimeoutSec
	stateTimeout = (2 * s.taskTimeoutSec) + (2 * s.taskTimeoutSec) + 10
	return taskTimeout, stateTimeout
}

type Sweeper struct {
	dbPool         *pgxpool.Pool
	interval       time.Duration
	batchSize      int
	taskTimeoutSec int
	stats          SweeperStats
	statsMu        sync.RWMutex
}

func NewSweeper(dbPool *pgxpool.Pool, interval time.Duration, batchSize int, taskTimeoutSec int) *Sweeper {
	return &Sweeper{
		dbPool:         dbPool,
		interval:       interval,
		batchSize:      batchSize,
		taskTimeoutSec: taskTimeoutSec,
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
			if wf.RetryCount < MaxRetries {
				taskTimeout, stateTimeout := s.calculateRetryTimeout(wf.RetryCount)
				nextRetry := wf.RetryCount + 1

				numSeats := 1
				if ns, ok := currentPayload["num_seats"].(float64); ok && ns > 0 {
					numSeats = int(ns)
				}
				_, err := db.TransitionWorkflowWithOutbox(sweepCtx, tx, db.TransitionParams{
					WorkflowID:      wf.ID,
					NextState:       wf.WorkflowState,
					StateTimeoutSec: stateTimeout,
					RetryCount:      nextRetry,
					WorkerType:      shared_vars.BOOKING_WORKER,
					OutboxPayload: map[string]any{
						"workflow_id":     wf.ID.String(),
						"num_seats":       numSeats,
						"timeout_seconds": taskTimeout,
					},
					TaskTimeoutSec: taskTimeout,
				})
				if err != nil {
					log.Printf("[SWEEPER] Error updating workflow %s and inserting outbox for BOOKING retry: %v", wf.ID, err)
					continue
				}

				s.recordStat(func(st *SweeperStats) {
					st.TotalRetries++
					st.TotalSweptWorkflows++
				})
				log.Printf("[SWEEPER] Workflow %s timed out in BOOKING; re-dispatched retry (%d/%d) with timeout %ds", wf.ID, nextRetry, MaxRetries, stateTimeout)
			} else {
				currentPayload["timeout"] = map[string]any{
					"timed_out_state": string(shared_vars.ORCHESTRATOR_WORKFLOW_BOOKING),
					"timed_out_at":    db.NewOutboxTimestamp(),
					"reason":          fmt.Sprintf("Booking step exceeded state_timeout after %d retries (~2 min cap exhausted)", MaxRetries),
				}
				if err := db.MarkWorkflowFailed(sweepCtx, tx, wf.ID, currentPayload); err != nil {
					log.Printf("[SWEEPER] Error updating timed-out booking %s to FAILED: %v", wf.ID, err)
					continue
				}

				s.recordStat(func(st *SweeperStats) {
					st.FailedBookingTimeouts++
					st.TotalSweptWorkflows++
				})
				log.Printf("[SWEEPER] Workflow %s exhausted all %d retries in BOOKING state; marked as FAILED", wf.ID, MaxRetries)
			}

		case shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT:
			if wf.RetryCount < MaxRetries {
				taskTimeout, stateTimeout := s.calculateRetryTimeout(wf.RetryCount)
				nextRetry := wf.RetryCount + 1

				seatIDs := db.ExtractSeatIDs(currentPayload)
				numSeats := len(seatIDs)
				if ns, ok := currentPayload["num_seats"].(float64); ok && ns > 0 {
					numSeats = int(ns)
				}
				amount := numSeats * 500
				if a, ok := currentPayload["amount"].(float64); ok && a > 0 {
					amount = int(a)
				}

				paymentPayload := map[string]any{
					"workflow_id":     wf.ID.String(),
					"seat_ids":        seatIDs,
					"num_seats":       numSeats,
					"amount":          amount,
					"updated_at":      db.NewOutboxTimestamp(),
					"timeout_seconds": taskTimeout,
				}
				if pm := db.ExtractPaymentMethod(currentPayload); pm != "" {
					paymentPayload["payment_method"] = pm
				}

				_, err := db.TransitionWorkflowWithOutbox(sweepCtx, tx, db.TransitionParams{
					WorkflowID:      wf.ID,
					NextState:       wf.WorkflowState,
					StateTimeoutSec: stateTimeout,
					RetryCount:      nextRetry,
					WorkerType:      shared_vars.PAYMENT_WORKER,
					OutboxPayload:   paymentPayload,
					TaskTimeoutSec:  taskTimeout,
				})
				if err != nil {
					log.Printf("[SWEEPER] Error updating workflow %s and inserting outbox for PAYMENT retry: %v", wf.ID, err)
					continue
				}

				s.recordStat(func(st *SweeperStats) {
					st.TotalRetries++
					st.TotalSweptWorkflows++
				})
				log.Printf("[SWEEPER] Workflow %s timed out in PAYMENT; re-dispatched retry (%d/%d) with timeout %ds", wf.ID, nextRetry, MaxRetries, stateTimeout)
			} else {
				timeoutSec := s.taskTimeoutSec
				stateTimeoutSec := timeoutSec + (s.taskTimeoutSec * 2)

				currentPayload["timeout"] = map[string]any{
					"timed_out_state": string(shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT),
					"timed_out_at":    db.NewOutboxTimestamp(),
					"reason":          fmt.Sprintf("Payment step exceeded state_timeout after %d retries; initiating backward compensation", MaxRetries),
				}

				_, err := db.TransitionWorkflowWithOutbox(sweepCtx, tx, db.TransitionParams{
					WorkflowID:      wf.ID,
					NextState:       shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT,
					WorkflowPayload: currentPayload,
					StateTimeoutSec: stateTimeoutSec,
					RetryCount:      0,
					WorkerType:      shared_vars.PAYMENT_WORKER,
					OutboxPayload: map[string]any{
						"workflow_id":     wf.ID.String(),
						"action":          shared_vars.ACTION_COMPENSATE,
						"reason":          fmt.Sprintf("Payment step exceeded state_timeout after %d retries; initiating backward compensation", MaxRetries),
						"updated_at":      db.NewOutboxTimestamp(),
						"timeout_seconds": timeoutSec,
					},
					TaskTimeoutSec: timeoutSec,
				})
				if err != nil {
					log.Printf("[SWEEPER] Error updating workflow %s and inserting payment compensation outbox: %v", wf.ID, err)
					continue
				}

				s.recordStat(func(st *SweeperStats) {
					st.CompensatedPayments++
					st.TotalSweptWorkflows++
				})
				log.Printf("[SWEEPER] Workflow %s exhausted all %d retries in PAYMENT; initiated backward compensation at PAYMENT", wf.ID, MaxRetries)
			}

		case shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT:
			if wf.RetryCount < MaxRetries {
				taskTimeout, stateTimeout := s.calculateRetryTimeout(wf.RetryCount)
				nextRetry := wf.RetryCount + 1

				_, err := db.TransitionWorkflowWithOutbox(sweepCtx, tx, db.TransitionParams{
					WorkflowID:      wf.ID,
					NextState:       wf.WorkflowState,
					StateTimeoutSec: stateTimeout,
					RetryCount:      nextRetry,
					WorkerType:      shared_vars.PAYMENT_WORKER,
					OutboxPayload: map[string]any{
						"workflow_id":     wf.ID.String(),
						"action":          shared_vars.ACTION_COMPENSATE,
						"reason":          fmt.Sprintf("Payment compensation retry (%d/%d)", nextRetry, MaxRetries),
						"updated_at":      db.NewOutboxTimestamp(),
						"timeout_seconds": taskTimeout,
					},
					TaskTimeoutSec: taskTimeout,
				})
				if err != nil {
					log.Printf("[SWEEPER] Error updating workflow %s and inserting payment compensation retry outbox: %v", wf.ID, err)
					continue
				}

				s.recordStat(func(st *SweeperStats) {
					st.TotalRetries++
					st.TotalSweptWorkflows++
				})
				log.Printf("[SWEEPER] Workflow %s timed out in COMPENSATING_PAYMENT; re-dispatched retry (%d/%d) with timeout %ds", wf.ID, nextRetry, MaxRetries, stateTimeout)
			} else {
				// Escalate directly to BOOKING compensation so reserved seats are released.
				seatIDs := db.ExtractSeatIDs(currentPayload)
				timeoutSec := s.taskTimeoutSec
				stateTimeoutSec := timeoutSec + (s.taskTimeoutSec * 2)

				currentPayload["timeout_escalation"] = map[string]any{
					"escalated_from": string(shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT),
					"escalated_at":   db.NewOutboxTimestamp(),
					"reason":         fmt.Sprintf("Payment compensation timed out after %d retries; escalating directly to release booked seats", MaxRetries),
				}

				_, err := db.TransitionWorkflowWithOutbox(sweepCtx, tx, db.TransitionParams{
					WorkflowID:      wf.ID,
					NextState:       shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING,
					WorkflowPayload: currentPayload,
					StateTimeoutSec: stateTimeoutSec,
					RetryCount:      0,
					WorkerType:      shared_vars.BOOKING_WORKER,
					OutboxPayload: map[string]any{
						"workflow_id":     wf.ID.String(),
						"action":          shared_vars.ACTION_COMPENSATE,
						"seat_ids":        seatIDs,
						"reason":          "Payment compensation timed out; sweeping to release seats",
						"updated_at":      db.NewOutboxTimestamp(),
						"timeout_seconds": timeoutSec,
					},
					TaskTimeoutSec: timeoutSec,
				})
				if err != nil {
					log.Printf("[SWEEPER] Error updating workflow %s and inserting booking compensation outbox: %v", wf.ID, err)
					continue
				}

				s.recordStat(func(st *SweeperStats) {
					st.EscalatedCompensations++
					st.TotalSweptWorkflows++
				})
				log.Printf("[SWEEPER] Workflow %s exhausted all %d retries in COMPENSATING_PAYMENT; escalated to release seats in BOOKING", wf.ID, MaxRetries)
			}

		case shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING:
			if wf.RetryCount < MaxRetries {
				taskTimeout, stateTimeout := s.calculateRetryTimeout(wf.RetryCount)
				nextRetry := wf.RetryCount + 1
				seatIDs := db.ExtractSeatIDs(currentPayload)

				_, err := db.TransitionWorkflowWithOutbox(sweepCtx, tx, db.TransitionParams{
					WorkflowID:      wf.ID,
					NextState:       wf.WorkflowState,
					StateTimeoutSec: stateTimeout,
					RetryCount:      nextRetry,
					WorkerType:      shared_vars.BOOKING_WORKER,
					OutboxPayload: map[string]any{
						"workflow_id":     wf.ID.String(),
						"action":          shared_vars.ACTION_COMPENSATE,
						"seat_ids":        seatIDs,
						"reason":          fmt.Sprintf("Retrying booking compensation (attempt %d/%d)", nextRetry, MaxRetries),
						"updated_at":      db.NewOutboxTimestamp(),
						"timeout_seconds": taskTimeout,
					},
					TaskTimeoutSec: taskTimeout,
				})
				if err != nil {
					log.Printf("[SWEEPER] Error updating workflow %s and inserting booking compensation retry outbox: %v", wf.ID, err)
					continue
				}

				s.recordStat(func(st *SweeperStats) {
					st.TotalRetries++
					st.TotalSweptWorkflows++
				})
				log.Printf("[SWEEPER] Re-emitted booking compensation for %s (attempt %d/%d, timeout %ds)", wf.ID, nextRetry, MaxRetries, stateTimeout)
			} else {
				currentPayload["critical_alert"] = fmt.Sprintf("Booking compensation retries (%d attempts) exhausted. Manual operator intervention required.", MaxRetries)
				if err := db.MarkWorkflowFailed(sweepCtx, tx, wf.ID, currentPayload); err != nil {
					log.Printf("[SWEEPER] Error marking %s FAILED after exhausted booking compensation: %v", wf.ID, err)
					continue
				}
				log.Printf("[CRITICAL OPERATOR ALERT] Workflow %s failed booking compensation after %d attempts; manual seat audit required", wf.ID, MaxRetries)
			}

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
