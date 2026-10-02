package posthandlers

import (
	"encoding/json"
	"net/http"

	shared_vars "github.com/D-pixel-crime/Endeavor/orchestrator/shared"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type customerDetails struct {
	Email    string `json:"email"`
	FullName string `json:"full_name"`
	Contact  int64  `json:"contact"`
}

type reqBody struct {
	WorkflowID      uuid.UUID                `json:"workflow_id"`
	WorkflowType    shared_vars.WorkflowType `json:"workflow_type"`
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

	if req.WorkflowType != shared_vars.WORKFLOW_TYPE_TICKET_BOOKING {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid Workflow Type",
		})
		return
	}

	customerDetails, err := json.Marshal(req.CustomerDetails)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Failed to marshal customer_details",
		})
		return
	}

	if len(customerDetails) == 2 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "NO Customer Details",
		})
		return
	}

	ctx := c.Request.Context()

	tx, err := shared_vars.Orchestrator_DB_Pool.Begin(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to begin transaction",
		})
		return
	}
	defer tx.Rollback(ctx)

	var workflowInsert string = `INSERT INTO workflows (id, workflow_type, workflow_state, payload, created_at, expires_at, state_timeout) VALUES ($1, $2, $3, $4, NOW(), NOW() + INTERVAL '5 minutes', NOW() + INTERVAL '20 seconds') RETURNING id`
	_, err = tx.Exec(ctx, workflowInsert, req.WorkflowID, req.WorkflowType, shared_vars.WORKFLOW_INITIALIZED, customerDetails)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to insert workflow",
		})
		return
	}

	var outboxInsert string = `INSERT INTO outbox (id, processed, workflow_id, worker_type, payload, timeout_seconds, created_at) VALUES ($1, $2, $3, $4, $5, 15, NOW())`
	_, err = tx.Exec(ctx, outboxInsert, uuid.New(), false, req.WorkflowID, shared_vars.BOOKING_WORKER, customerDetails)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to insert outbox",
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

	c.JSON(http.StatusOK, gin.H{
		"message":     "Workflow created successfully",
		"workflow_id": req.WorkflowID,
	})
}
