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
	LastSweepTime          time.Time `json:"last_sweep_time"`
	LastError              string    `json:"last_error,omitempty"`
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
		SELECT id, workflow_type, workflow_state, payload, created_at, expires_at, state_timeout
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
			// 1. Workflow timed out during BOOKING. No downstream steps succeeded yet.
			// Mark FAILED cleanly. If late booking message arrives later, ghost seat prevention will compensate it.
			currentPayload["timeout"] = map[string]any{
				"timed_out_state": string(shared_vars.ORCHESTRATOR_WORKFLOW_BOOKING),
				"timed_out_at":    time.Now().UTC().Format(time.RFC3339),
				"reason":          "Booking step exceeded state_timeout without receiving egress response",
			}
			newPayloadBytes, err := json.Marshal(currentPayload)
			if err != nil {
				log.Printf("[SWEEPER] Error marshaling payload for %s: %v", wf.ID, err)
				continue
			}

			_, err = tx.Exec(sweepCtx,
				`UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NULL WHERE id = $3`,
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
			log.Printf("[SWEEPER] Workflow %s timed out in BOOKING state; marked as FAILED", wf.ID)

		case shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT:
			// 2. Workflow timed out during PAYMENT. Seats were booked in Booking DB!
			// Must trigger backward compensation starting from PAYMENT.
			timeoutSec := s.taskTimeoutSec
			stateTimeoutSec := timeoutSec + 20

			currentPayload["timeout"] = map[string]any{
				"timed_out_state": string(shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT),
				"timed_out_at":    time.Now().UTC().Format(time.RFC3339),
				"reason":          "Payment step exceeded state_timeout; initiating backward compensation",
			}
			newPayloadBytes, err := json.Marshal(currentPayload)
			if err != nil {
				log.Printf("[SWEEPER] Error marshaling payload for %s: %v", wf.ID, err)
				continue
			}

			updateQuery := fmt.Sprintf(
				`UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NOW() + INTERVAL '%d seconds' WHERE id = $3`,
				stateTimeoutSec,
			)
			_, err = tx.Exec(sweepCtx, updateQuery, shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT, newPayloadBytes, wf.ID)
			if err != nil {
				log.Printf("[SWEEPER] Error updating workflow %s to COMPENSATING_PAYMENT: %v", wf.ID, err)
				continue
			}

			compensatePaymentPayload := map[string]any{
				"workflow_id":       wf.ID.String(),
				"action":            shared_vars.ACTION_COMPENSATE,
				"reason":            "Payment step exceeded state_timeout; initiating backward compensation",
				"updated_at":        time.Now().UTC().Format(time.RFC3339),
				"timeout_seconds":   timeoutSec,
				"timeout_parameter": timeoutSec,
			}
			compensateBytes, _ := json.Marshal(compensatePaymentPayload)

			outboxInsert := `INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at) VALUES ($1, $2, $3, $4, $5, NOW())`
			_, err = tx.Exec(sweepCtx, outboxInsert, uuid.New(), wf.ID, shared_vars.PAYMENT_WORKER, compensateBytes, timeoutSec)
			if err != nil {
				log.Printf("[SWEEPER] Error inserting payment compensation outbox for %s: %v", wf.ID, err)
				continue
			}

			s.statsMu.Lock()
			s.stats.CompensatedPayments++
			s.stats.TotalSweptWorkflows++
			s.statsMu.Unlock()
			log.Printf("[SWEEPER] Workflow %s timed out in PAYMENT; initiated backward compensation at PAYMENT", wf.ID)

		case shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT:
			// 3. Payment compensation timed out (e.g. Payment worker unreachable).
			// Escalate directly to BOOKING compensation so reserved seats are released.
			seatIDs := extractSeatIDs(currentPayload)
			timeoutSec := s.taskTimeoutSec
			stateTimeoutSec := timeoutSec + 20

			currentPayload["timeout_escalation"] = map[string]any{
				"escalated_from": string(shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_PAYMENT),
				"escalated_at":   time.Now().UTC().Format(time.RFC3339),
				"reason":         "Payment compensation timed out; escalating directly to release booked seats",
			}
			newPayloadBytes, _ := json.Marshal(currentPayload)

			updateQuery := fmt.Sprintf(
				`UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NOW() + INTERVAL '%d seconds' WHERE id = $3`,
				stateTimeoutSec,
			)
			_, err = tx.Exec(sweepCtx, updateQuery, shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING, newPayloadBytes, wf.ID)
			if err != nil {
				log.Printf("[SWEEPER] Error updating workflow %s to COMPENSATING_BOOKING: %v", wf.ID, err)
				continue
			}

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

			outboxInsert := `INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at) VALUES ($1, $2, $3, $4, $5, NOW())`
			_, err = tx.Exec(sweepCtx, outboxInsert, uuid.New(), wf.ID, shared_vars.BOOKING_WORKER, compensateBookingBytes, timeoutSec)
			if err != nil {
				log.Printf("[SWEEPER] Error inserting booking compensation outbox for %s: %v", wf.ID, err)
				continue
			}

			s.statsMu.Lock()
			s.stats.EscalatedCompensations++
			s.stats.TotalSweptWorkflows++
			s.statsMu.Unlock()
			log.Printf("[SWEEPER] Workflow %s timed out in COMPENSATING_PAYMENT; escalated to release seats in BOOKING", wf.ID)

		case shared_vars.ORCHESTRATOR_WORKFLOW_COMPENSATING_BOOKING:
			// 4. Booking compensation timed out. Re-emit outbox message up to 3 times, then mark FAILED with alert.
			retries := 0
			if r, ok := currentPayload["booking_compensation_retries"].(float64); ok {
				retries = int(r)
			}
			retries++
			currentPayload["booking_compensation_retries"] = retries

			if retries < 3 {
				seatIDs := extractSeatIDs(currentPayload)
				timeoutSec := s.taskTimeoutSec
				stateTimeoutSec := timeoutSec + 20

				newPayloadBytes, _ := json.Marshal(currentPayload)
				updateQuery := fmt.Sprintf(
					`UPDATE workflows SET payload = $1, state_timeout = NOW() + INTERVAL '%d seconds' WHERE id = $2`,
					stateTimeoutSec,
				)
				_, _ = tx.Exec(sweepCtx, updateQuery, newPayloadBytes, wf.ID)

				compensateBookingPayload := map[string]any{
					"workflow_id":       wf.ID.String(),
					"action":            shared_vars.ACTION_COMPENSATE,
					"seat_ids":          seatIDs,
					"reason":            fmt.Sprintf("Retrying booking compensation (attempt %d/3)", retries),
					"updated_at":        time.Now().UTC().Format(time.RFC3339),
					"timeout_seconds":   timeoutSec,
					"timeout_parameter": timeoutSec,
				}
				compensateBookingBytes, _ := json.Marshal(compensateBookingPayload)

				outboxInsert := `INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at) VALUES ($1, $2, $3, $4, $5, NOW())`
				_, _ = tx.Exec(sweepCtx, outboxInsert, uuid.New(), wf.ID, shared_vars.BOOKING_WORKER, compensateBookingBytes, timeoutSec)
				log.Printf("[SWEEPER] Re-emitted booking compensation for %s (attempt %d/3)", wf.ID, retries)
			} else {
				currentPayload["critical_alert"] = "Booking compensation retries exhausted. Manual operator intervention required."
				newPayloadBytes, _ := json.Marshal(currentPayload)
				_, _ = tx.Exec(sweepCtx,
					`UPDATE workflows SET workflow_state = $1, payload = $2, state_timeout = NULL WHERE id = $3`,
					shared_vars.ORCHESTRATOR_WORKFLOW_FAILED, newPayloadBytes, wf.ID,
				)
				log.Printf("[CRITICAL OPERATOR ALERT] Workflow %s failed booking compensation after 3 attempts; manual seat audit required", wf.ID)
			}

		case shared_vars.ORCHESTRATOR_WORKFLOW_COMPLETED, shared_vars.ORCHESTRATOR_WORKFLOW_FAILED:
			// Workflow already terminal, clear leftover state_timeout
			_, _ = tx.Exec(sweepCtx, `UPDATE workflows SET state_timeout = NULL WHERE id = $1`, wf.ID)

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
