package posthandlers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/D-pixel-crime/Endeavour/orchestrator/db"
	"github.com/D-pixel-crime/Endeavour/orchestrator/middleware"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type customerDetails struct {
	Email         string `json:"email"`
	FullName      string `json:"full_name"`
	Contact       int64  `json:"contact"`
	PaymentMethod string `json:"payment_method,omitempty"`
}

type reqBody struct {
	WorkflowID      uuid.UUID                `json:"workflow_id"`
	WorkflowType    shared_vars.WorkflowType `json:"workflow_type"`
	NumSeats        int                      `json:"num_seats"`
	CustomerDetails customerDetails          `json:"customer_details"`
}

func CreateWorkflow(c *gin.Context) {
	var req reqBody
	err := c.ShouldBindJSON(&req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	if req.WorkflowID == uuid.Nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "No Workflow ID",
		})
		return
	}

	if req.NumSeats <= 0 || req.NumSeats > 10 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid num_seats: must be between 1 and 10",
		})
		return
	}

	if req.CustomerDetails.Email == "" || req.CustomerDetails.FullName == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Customer email and full_name are required",
		})
		return
	}

	if req.CustomerDetails.PaymentMethod == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "No Payment Method",
		})
		return
	}

	if req.WorkflowType != shared_vars.WORKFLOW_TYPE_TICKET_BOOKING {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid Workflow Type",
		})
		return
	}

	wfPayload := map[string]any{
		"workflow_id":    req.WorkflowID.String(),
		"num_seats":      req.NumSeats,
		"payment_method": req.CustomerDetails.PaymentMethod,
		"email":          req.CustomerDetails.Email,
		"full_name":      req.CustomerDetails.FullName,
		"contact":        req.CustomerDetails.Contact,
	}
	wfPayloadBytes, err := json.Marshal(wfPayload)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Failed to marshal workflow payload",
		})
		return
	}

	ctx := c.Request.Context()

	// 1. Fast in-memory idempotency check: eliminate DB connection overhead for duplicate checks
	if cachedState, found := middleware.GlobalIdempotencyCache.Get(req.WorkflowID); found {
		c.JSON(http.StatusAlreadyReported, gin.H{
			"Success":     "Workflow Already Exists",
			"State":       cachedState,
			"workflow_id": req.WorkflowID,
		})
		return
	}

	// 2. In-flight singleflight lock: prevent parallel requests with identical workflow_id from racing on DB locks
	unlock, acquired := middleware.GlobalIdempotencyCache.TryLock(req.WorkflowID)
	if !acquired {
		if middleware.GlobalIdempotencyCache.WaitForInFlight(ctx, req.WorkflowID, 1*time.Second) {
			if cachedState, found := middleware.GlobalIdempotencyCache.Get(req.WorkflowID); found {
				c.JSON(http.StatusAlreadyReported, gin.H{
					"Success":     "Workflow Already Exists",
					"State":       cachedState,
					"workflow_id": req.WorkflowID,
				})
				return
			}
		}
		c.Header("Retry-After", "1")
		c.JSON(http.StatusTooManyRequests, gin.H{
			"error": "A workflow with this ID is currently being processed. Please retry shortly.",
		})
		return
	}
	defer unlock()

	tx, err := shared_vars.Orchestrator_DB_Pool.Begin(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to begin transaction",
		})
		return
	}
	defer tx.Rollback(ctx)

	var checkWorkflowExists string = `SELECT id, workflow_state FROM workflows WHERE id = $1 FOR UPDATE`
	var workflow_id uuid.UUID
	var workflow_state shared_vars.Orchestrator_WorkflowState
	if err = tx.QueryRow(ctx, checkWorkflowExists, req.WorkflowID).Scan(&workflow_id, &workflow_state); err != nil && err != pgx.ErrNoRows {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to assess existence of Workflow!",
		})
		return
	}
	if workflow_id != uuid.Nil {
		middleware.GlobalIdempotencyCache.Set(workflow_id, string(workflow_state))
		c.JSON(http.StatusAlreadyReported, gin.H{
			"Success":     "Workflow Already Exists",
			"State":       workflow_state,
			"workflow_id": workflow_id,
		})
		return
	}

	// Cascading rule: DB Timeout < Worker Processing (V/2) < SQS Visibility (V) < Orchestrator State Timeout (V + TaskTimeout)
	bookingTimeout, stateTimeoutSec := db.ComputeStageTimeouts(shared_vars.BookingEgressVisibilityTimeout, shared_vars.BookingTaskTimeoutSeconds)

	bookingOutboxPayload := map[string]any{
		"workflow_id":     req.WorkflowID.String(),
		"num_seats":       req.NumSeats,
		"timeout_seconds": bookingTimeout,
	}
	bookingOutboxBytes, err := json.Marshal(bookingOutboxPayload)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to marshal booking outbox payload",
		})
		return
	}

	const insertWorkflowAndOutbox = `
		WITH inserted_workflow AS (
			INSERT INTO workflows (id, workflow_type, workflow_state, payload, created_at, expires_at, state_timeout)
			VALUES ($1, $2, $3, $4, NOW(), NOW() + INTERVAL '10 minutes', NOW() + make_interval(secs => $5))
			RETURNING id
		)
		INSERT INTO outbox (id, workflow_id, worker_type, payload, timeout_seconds, updated_at)
		SELECT $6, id, $7, $8, $9, NOW()
		FROM inserted_workflow`
	_, err = tx.Exec(ctx, insertWorkflowAndOutbox,
		req.WorkflowID,
		req.WorkflowType,
		shared_vars.ORCHESTRATOR_WORKFLOW_BOOKING,
		wfPayloadBytes,
		stateTimeoutSec,
		uuid.New(),
		shared_vars.BOOKING_WORKER,
		bookingOutboxBytes,
		bookingTimeout,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to insert workflow and outbox entry",
		})
		return
	}

	err = tx.Commit(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to commit transaction",
		})
		return
	}

	// Cache successful workflow creation state
	middleware.GlobalIdempotencyCache.Set(req.WorkflowID, string(shared_vars.ORCHESTRATOR_WORKFLOW_BOOKING))

	c.JSON(http.StatusAccepted, gin.H{
		"message":     "Workflow created successfully",
		"workflow_id": req.WorkflowID,
	})
}
