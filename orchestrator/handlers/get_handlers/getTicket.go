package gethandlers

import (
	"net/http"

	"github.com/D-pixel-crime/Endeavour/orchestrator/db/models"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func GetTicketByWorkflowID(c *gin.Context) {
	workflowIDParam := c.Param("workflow_id")
	workflowUUID, err := uuid.Parse(workflowIDParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid workflow_id format"})
		return
	}

	if shared_vars.Orchestrator_DB_Pool == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database pool is not initialized"})
		return
	}

	query := `SELECT id, workflow_id, seat_ids, amount, COALESCE(payment_id, ''), COALESCE(customer_name, ''), COALESCE(customer_email, ''), status, created_at 
	          FROM tickets WHERE workflow_id = $1`

	var t models.Ticket
	err = shared_vars.Orchestrator_DB_Pool.QueryRow(c.Request.Context(), query, workflowUUID).Scan(
		&t.ID, &t.WorkflowID, &t.SeatIDs, &t.Amount, &t.PaymentID, &t.CustomerName, &t.CustomerEmail, &t.Status, &t.CreatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "ticket not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query ticket: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"workflow_id": workflowUUID.String(),
		"ticket":      t,
	})
}

func GetWorkflowByID(c *gin.Context) {
	workflowIDParam := c.Param("workflow_id")
	workflowUUID, err := uuid.Parse(workflowIDParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid workflow_id format"})
		return
	}

	if shared_vars.Orchestrator_DB_Pool == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database pool is not initialized"})
		return
	}

	var wf models.Workflow
	query := `SELECT id, workflow_type, workflow_state, payload, created_at, expires_at, state_timeout FROM workflows WHERE id = $1`
	err = shared_vars.Orchestrator_DB_Pool.QueryRow(c.Request.Context(), query, workflowUUID).Scan(
		&wf.ID,
		&wf.WorkflowType,
		&wf.WorkflowState,
		&wf.Payload,
		&wf.CreatedAt,
		&wf.ExpiresAt,
		&wf.StateTimeout,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "workflow not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query workflow: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, wf)
}
