package gethandlers

import (
	"net/http"

	"github.com/D-pixel-crime/Endeavour/orchestrator/db"
	"github.com/D-pixel-crime/Endeavour/orchestrator/db/models"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func resolveCompletedPayment(wf *models.Workflow) (db.PaymentInfo, bool) {
	currentPayload := db.UnmarshalWorkflowPayload(wf.Payload)
	paymentInfo, paymentDone := db.ExtractPaymentInfoFromPayload(currentPayload)

	if wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_COMPLETED {
		if paymentInfo.Status == "" {
			paymentInfo.Status = shared_vars.STATUS_COMPLETED
		}
		return paymentInfo, true
	}

	return paymentInfo, paymentDone
}

func parseWorkflowIDAndPool(c *gin.Context) (uuid.UUID, bool) {
	workflowUUID, err := uuid.Parse(c.Param("workflow_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid workflow_id format"})
		return uuid.Nil, false
	}
	if shared_vars.Orchestrator_DB_Pool == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database pool is not initialized"})
		return uuid.Nil, false
	}
	return workflowUUID, true
}

func GetTicketByWorkflowID(c *gin.Context) {
	workflowUUID, ok := parseWorkflowIDAndPool(c)
	if !ok {
		return
	}

	ctx := c.Request.Context()

	existingTicket, err := db.GetTicketByWorkflowID(ctx, shared_vars.Orchestrator_DB_Pool, workflowUUID)
	if err == nil {
		c.JSON(http.StatusOK, gin.H{
			"workflow_id": workflowUUID.String(),
			"ticket":      *existingTicket,
		})
		return
	}
	if err != pgx.ErrNoRows {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query ticket: " + err.Error()})
		return
	}

	wf, err := db.GetWorkflowByID(ctx, shared_vars.Orchestrator_DB_Pool, workflowUUID, false)
	if err != nil {
		if err == pgx.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "ticket not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query workflow: " + err.Error()})
		return
	}

	if wf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_COMPLETED {
		c.JSON(http.StatusNotFound, gin.H{
			"error":          "ticket not found",
			"workflow_state": wf.WorkflowState,
		})
		return
	}

	paymentInfo, _ := resolveCompletedPayment(wf)

	tx, err := shared_vars.Orchestrator_DB_Pool.Begin(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to begin transaction: " + err.Error()})
		return
	}
	defer tx.Rollback(ctx)

	lockedWf, err := db.GetWorkflowByID(ctx, tx, workflowUUID, true)
	if err != nil {
		if err == pgx.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "ticket not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to lock workflow: " + err.Error()})
		return
	}

	existingTicket, err = db.GetTicketByWorkflowID(ctx, tx, workflowUUID)
	if err == nil {
		c.JSON(http.StatusOK, gin.H{
			"workflow_id": workflowUUID.String(),
			"ticket":      *existingTicket,
		})
		return
	}
	if err != pgx.ErrNoRows {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to re-check ticket: " + err.Error()})
		return
	}

	if lockedWf.WorkflowState != shared_vars.ORCHESTRATOR_WORKFLOW_COMPLETED {
		c.JSON(http.StatusNotFound, gin.H{
			"error":          "ticket not found",
			"workflow_state": lockedWf.WorkflowState,
		})
		return
	}

	lockedPayload := db.UnmarshalWorkflowPayload(lockedWf.Payload)
	generatedTicket, err := db.GenerateAndStoreTicket(ctx, tx, lockedWf, lockedPayload, paymentInfo)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate ticket: " + err.Error()})
		return
	}

	if err := tx.Commit(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to commit ticket generation: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"workflow_id": workflowUUID.String(),
		"ticket":      generatedTicket,
	})
}

func GetWorkflowByID(c *gin.Context) {
	workflowUUID, ok := parseWorkflowIDAndPool(c)
	if !ok {
		return
	}

	wf, err := db.GetWorkflowByID(c.Request.Context(), shared_vars.Orchestrator_DB_Pool, workflowUUID, false)
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
