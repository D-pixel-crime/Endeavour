package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/D-pixel-crime/Endeavour/orchestrator/db/models"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/google/uuid"
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

const (
	MaxRetries = 3
)

func (s *Sweeper) calculateRetryTimeout(retryCount int) (taskTimeout int, stateTimeout int) {
	// Base timeout: taskTimeoutSec (e.g. 15s)
	// Dynamic backoff: base * (1 + 0.5 * retryCount)
	taskTimeout = int(float64(s.taskTimeoutSec) * (1.0 + float64(retryCount)*0.5))
	// State timeout is task timeout + visibility window (derived as taskTimeoutSec * 2)
	visibilitySec := s.taskTimeoutSec * 2
	stateTimeout = taskTimeout + visibilitySec
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
			s.statsMu.Lock()
			s.stats.TotalSweepCycles++
			s.stats.LastSweepTime = time.Now().UTC()
			if err != nil {
				s.stats.LastError = err.Error()
				log.Printf("[SWEEPER ERROR] Sweep cycle failed: %v", err)
			} else {
				s.stats.LastError = ""
				if count > 0 {
					log.Printf("[SWEEPER] Successfully swept and processed %d timed-out workflow(s)", count)
				}
			}
			s.statsMu.Unlock()
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

	query := `
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
		var currentPayload map[string]any
		if len(wf.Payload) > 0 {
			_ = json.Unmarshal(wf.Payload, &currentPayload)
		}
		if currentPayload == nil {
			currentPayload = make(map[string]any)
		}

		switch wf.WorkflowState {
		case shared_vars.ORCHESTRATOR_WORKFLOW_BOOKING:
			if wf.RetryCount < MaxRetries {
				taskTimeout, stateTimeout := s.calculateRetryTimeout(wf.RetryCount)
				nextRetry := wf.RetryCount + 1

				numSeats := 1
				if ns, ok := currentPayload["num_seats"].(float64); ok && ns > 0 {
					numSeats = int(ns)
				}
				bookingOutboxPayload := map[string]any{
					"workflow_id":       wf.ID.String(),
					"num_seats":         numSeats,
					"timeout_seconds":   taskTimeout,
					"timeout_parameter": taskTimeout,
				}
				bookingOutboxBytes, err := json.Marshal(bookingOutboxPayload)
				if err != nil {
					log.Printf("[SWEEPER] Error marshaling booking retry payload for %s: %v", wf.ID, err)
					continue
				}

				query := fmt.Sprintf(`
					WITH updated_workflow AS (
						UPDATE workflows SET retry_count = $1, state_timeout = NOW() + INTERVAL '%d seconds' WHERE id = $2
						RETURNING id
					)
					INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
					SELECT $3, id, $4, $5, $6, NOW()
					FROM updated_workflow`, stateTimeout)
				if _, err = tx.Exec(sweepCtx, query, nextRetry, wf.ID, uuid.New(), shared_vars.BOOKING_WORKER, bookingOutboxBytes, taskTimeout); err != nil {
					log.Printf("[SWEEPER] Error updating workflow %s and inserting outbox for BOOKING retry: %v", wf.ID, err)
					continue
				}

				s.statsMu.Lock()
				s.stats.TotalRetries++
				s.stats.TotalSweptWorkflows++
				s.statsMu.Unlock()
				log.Printf("[SWEEPER] Workflow %s timed out in BOOKING; re-dispatched retry (%d/%d) with timeout %ds", wf.ID, nextRetry, MaxRetries, stateTimeout)
			} else {
				// 1. Workflow timed out in BOOKING after exhausting all retries (~2 min cap).
				// Mark FAILED cleanly. If late booking message arrives later, ghost seat prevention will compensate it.
				currentPayload["timeout"] = map[string]any{
					"timed_out_state": string(shared_vars.ORCHESTRATOR_WORKFLOW_BOOKING),
					"timed_out_at":    time.Now().UTC().Format(time.RFC3339),
					"reason":          fmt.Sprintf("Booking step exceeded state_timeout after %d retries (~2 min cap exhausted)", MaxRetries),
				}
				newPayloadBytes, err := json.Marshal(currentPayload)
				if err != nil {
					log.Printf("[SWEEPER] Error marshaling payload for %s: %v", wf.ID, err)
					continue
				}

				_, err = tx.Exec(sweepCtx,
					`UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NULL, retry_count = 0 WHERE id = $3`,
					shared_vars.ORCHESTRATOR_WORKFLOW_FAILED, newPayloadBytes, wf.ID,
				)
				if err != nil {
					log.Printf("[SWEEPER] Error updating timed-out booking %s to FAILED: %v", wf.ID, err)
					continue
				}

				s.statsMu.Lock()
				s.stats.FailedBookingTimeouts++
				s.stats.TotalSweptWorkflows++
				s.statsMu.Unlock()
				log.Printf("[SWEEPER] Workflow %s exhausted all %d retries in BOOKING state; marked as FAILED", wf.ID, MaxRetries)
			}

		case shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT:
			if wf.RetryCount < MaxRetries {
				taskTimeout, stateTimeout := s.calculateRetryTimeout(wf.RetryCount)
				nextRetry := wf.RetryCount + 1

				seatIDs := extractSeatIDs(currentPayload)
				numSeats := len(seatIDs)
				if ns, ok := currentPayload["num_seats"].(float64); ok && ns > 0 {
					numSeats = int(ns)
				}
				amount := numSeats * 500
				if a, ok := currentPayload["amount"].(float64); ok && a > 0 {
					amount = int(a)
				}

				paymentPayload := map[string]any{
					"workflow_id":       wf.ID.String(),
					"seat_ids":          seatIDs,
					"num_seats":         numSeats,
					"amount":            amount,
					"updated_at":        time.Now().UTC().Format(time.RFC3339),
					"timeout_seconds":   taskTimeout,
					"timeout_parameter": taskTimeout,
				}
				if pm, ok := currentPayload["payment_method"].(string); ok && pm != "" {
					paymentPayload["payment_method"] = pm
				} else if cd, ok := currentPayload["customer_details"].(map[string]any); ok {
					if pm, ok := cd["payment_method"].(string); ok && pm != "" {
						paymentPayload["payment_method"] = pm
					}
				}

				paymentPayloadBytes, err := json.Marshal(paymentPayload)
				if err != nil {
					log.Printf("[SWEEPER] Error marshaling payment retry payload for %s: %v", wf.ID, err)
					continue
				}

				query := fmt.Sprintf(`
					WITH updated_workflow AS (
						UPDATE workflows SET retry_count = $1, state_timeout = NOW() + INTERVAL '%d seconds' WHERE id = $2
						RETURNING id
					)
					INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
					SELECT $3, id, $4, $5, $6, NOW()
					FROM updated_workflow`, stateTimeout)
				if _, err = tx.Exec(sweepCtx, query, nextRetry, wf.ID, uuid.New(), shared_vars.PAYMENT_WORKER, paymentPayloadBytes, taskTimeout); err != nil {
					log.Printf("[SWEEPER] Error updating workflow %s and inserting outbox for PAYMENT retry: %v", wf.ID, err)
					continue
				}

				s.statsMu.Lock()
				s.stats.TotalRetries++
				s.stats.TotalSweptWorkflows++
				s.statsMu.Unlock()
				log.Printf("[SWEEPER] Workflow %s timed out in PAYMENT; re-dispatched retry (%d/%d) with timeout %ds", wf.ID, nextRetry, MaxRetries, stateTimeout)
			} else {
				// 2. Workflow timed out during PAYMENT after all retries exhausted. Seats were booked in Booking DB!
				// Must trigger backward compensation starting from PAYMENT with retry_count reset to 0.
				timeoutSec := s.taskTimeoutSec
				stateTimeoutSec := timeoutSec + (s.taskTimeoutSec * 2)

				currentPayload["timeout"] = map[string]any{
					"timed_out_state": string(shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT),
					"timed_out_at":    time.Now().UTC().Format(time.RFC3339),
					"reason":          fmt.Sprintf("Payment step exceeded state_timeout after %d retries; initiating backward compensation", MaxRetries),
				}
				newPayloadBytes, err := json.Marshal(currentPayload)
				if err != nil {
					log.Printf("[SWEEPER] Error marshaling payload for %s: %v", wf.ID, err)
					continue
				}

				compensatePaymentPayload := map[string]any{
					"workflow_id":       wf.ID.String(),
					"action":            shared_vars.ACTION_COMPENSATE,
					"reason":            fmt.Sprintf("Payment step exceeded state_timeout after %d retries; initiating backward compensation", MaxRetries),
					"updated_at":        time.Now().UTC().Format(time.RFC3339),
					"timeout_seconds":   timeoutSec,
					"timeout_parameter": timeoutSec,
				}
				compensateBytes, _ := json.Marshal(compensatePaymentPayload)

				query := fmt.Sprintf(`
					WITH updated_workflow AS (
						UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NOW() + INTERVAL '%d seconds', retry_count = 0 WHERE id = $3
						RETURNING id
					)
					INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
					SELECT $4, id, $5, $6, $7, NOW()
					FROM updated_workflow`, stateTimeoutSec)
				_, err = tx.Exec(sweepCtx, query, shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT, newPayloadBytes, wf.ID, uuid.New(), shared_vars.PAYMENT_WORKER, compensateBytes, timeoutSec)
				if err != nil {
					log.Printf("[SWEEPER] Error updating workflow %s and inserting payment compensation outbox: %v", wf.ID, err)
					continue
				}

				s.statsMu.Lock()
				s.stats.CompensatedPayments++
				s.stats.TotalSweptWorkflows++
				s.statsMu.Unlock()
				log.Printf("[SWEEPER] Workflow %s exhausted all %d retries in PAYMENT; initiated backward compensation at PAYMENT", wf.ID, MaxRetries)
			}

		case shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT:
			if wf.RetryCount < MaxRetries {
				taskTimeout, stateTimeout := s.calculateRetryTimeout(wf.RetryCount)
				nextRetry := wf.RetryCount + 1

				compensatePaymentPayload := map[string]any{
					"workflow_id":       wf.ID.String(),
					"action":            shared_vars.ACTION_COMPENSATE,
					"reason":            fmt.Sprintf("Payment compensation retry (%d/%d)", nextRetry, MaxRetries),
					"updated_at":        time.Now().UTC().Format(time.RFC3339),
					"timeout_seconds":   taskTimeout,
					"timeout_parameter": taskTimeout,
				}
				compensateBytes, _ := json.Marshal(compensatePaymentPayload)

				query := fmt.Sprintf(`
					WITH updated_workflow AS (
						UPDATE workflows SET retry_count = $1, state_timeout = NOW() + INTERVAL '%d seconds' WHERE id = $2
						RETURNING id
					)
					INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
					SELECT $3, id, $4, $5, $6, NOW()
					FROM updated_workflow`, stateTimeout)
				if _, err = tx.Exec(sweepCtx, query, nextRetry, wf.ID, uuid.New(), shared_vars.PAYMENT_WORKER, compensateBytes, taskTimeout); err != nil {
					log.Printf("[SWEEPER] Error updating workflow %s and inserting payment compensation retry outbox: %v", wf.ID, err)
					continue
				}

				s.statsMu.Lock()
				s.stats.TotalRetries++
				s.stats.TotalSweptWorkflows++
				s.statsMu.Unlock()
				log.Printf("[SWEEPER] Workflow %s timed out in COMPENSATING_PAYMENT; re-dispatched retry (%d/%d) with timeout %ds", wf.ID, nextRetry, MaxRetries, stateTimeout)
			} else {
				// 3. Payment compensation timed out after all retries (e.g. Payment worker unreachable).
				// Escalate directly to BOOKING compensation so reserved seats are released. Reset retry_count to 0.
				seatIDs := extractSeatIDs(currentPayload)
				timeoutSec := s.taskTimeoutSec
				stateTimeoutSec := timeoutSec + (s.taskTimeoutSec * 2)

				currentPayload["timeout_escalation"] = map[string]any{
					"escalated_from": string(shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT),
					"escalated_at":   time.Now().UTC().Format(time.RFC3339),
					"reason":         fmt.Sprintf("Payment compensation timed out after %d retries; escalating directly to release booked seats", MaxRetries),
				}
				newPayloadBytes, _ := json.Marshal(currentPayload)

				compensateBookingPayload := map[string]any{
					"workflow_id":       wf.ID.String(),
					"action":            shared_vars.ACTION_COMPENSATE,
					"seat_ids":          seatIDs,
					"reason":            "Payment compensation timed out; sweeping to release seats",
					"updated_at":        time.Now().UTC().Format(time.RFC3339),
					"timeout_seconds":   timeoutSec,
					"timeout_parameter": timeoutSec,
				}
				compensateBookingBytes, _ := json.Marshal(compensateBookingPayload)

				query := fmt.Sprintf(`
					WITH updated_workflow AS (
						UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NOW() + INTERVAL '%d seconds', retry_count = 0 WHERE id = $3
						RETURNING id
					)
					INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
					SELECT $4, id, $5, $6, $7, NOW()
					FROM updated_workflow`, stateTimeoutSec)
				_, err = tx.Exec(sweepCtx, query, shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING, newPayloadBytes, wf.ID, uuid.New(), shared_vars.BOOKING_WORKER, compensateBookingBytes, timeoutSec)
				if err != nil {
					log.Printf("[SWEEPER] Error updating workflow %s and inserting booking compensation outbox: %v", wf.ID, err)
					continue
				}

				s.statsMu.Lock()
				s.stats.EscalatedCompensations++
				s.stats.TotalSweptWorkflows++
				s.statsMu.Unlock()
				log.Printf("[SWEEPER] Workflow %s exhausted all %d retries in COMPENSATING_PAYMENT; escalated to release seats in BOOKING", wf.ID, MaxRetries)
			}

		case shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING:
			if wf.RetryCount < MaxRetries {
				taskTimeout, stateTimeout := s.calculateRetryTimeout(wf.RetryCount)
				nextRetry := wf.RetryCount + 1
				seatIDs := extractSeatIDs(currentPayload)

				compensateBookingPayload := map[string]any{
					"workflow_id":       wf.ID.String(),
					"action":            shared_vars.ACTION_COMPENSATE,
					"seat_ids":          seatIDs,
					"reason":            fmt.Sprintf("Retrying booking compensation (attempt %d/%d)", nextRetry, MaxRetries),
					"updated_at":        time.Now().UTC().Format(time.RFC3339),
					"timeout_seconds":   taskTimeout,
					"timeout_parameter": taskTimeout,
				}
				compensateBookingBytes, _ := json.Marshal(compensateBookingPayload)

				query := fmt.Sprintf(`
					WITH updated_workflow AS (
						UPDATE workflows SET retry_count = $1, state_timeout = NOW() + INTERVAL '%d seconds' WHERE id = $2
						RETURNING id
					)
					INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
					SELECT $3, id, $4, $5, $6, NOW()
					FROM updated_workflow`, stateTimeout)
				if _, err = tx.Exec(sweepCtx, query, nextRetry, wf.ID, uuid.New(), shared_vars.BOOKING_WORKER, compensateBookingBytes, taskTimeout); err != nil {
					log.Printf("[SWEEPER] Error updating workflow %s and inserting booking compensation retry outbox: %v", wf.ID, err)
					continue
				}

				s.statsMu.Lock()
				s.stats.TotalRetries++
				s.stats.TotalSweptWorkflows++
				s.statsMu.Unlock()
				log.Printf("[SWEEPER] Re-emitted booking compensation for %s (attempt %d/%d, timeout %ds)", wf.ID, nextRetry, MaxRetries, stateTimeout)
			} else {
				currentPayload["critical_alert"] = fmt.Sprintf("Booking compensation retries (%d attempts) exhausted. Manual operator intervention required.", MaxRetries)
				newPayloadBytes, _ := json.Marshal(currentPayload)
				_, _ = tx.Exec(sweepCtx,
					`UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NULL, retry_count = 0 WHERE id = $3`,
					shared_vars.ORCHESTRATOR_WORKFLOW_FAILED, newPayloadBytes, wf.ID,
				)
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

func extractSeatIDs(payload map[string]any) []string {
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
