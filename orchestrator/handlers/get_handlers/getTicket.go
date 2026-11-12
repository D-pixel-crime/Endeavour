package gethandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/D-pixel-crime/Endeavour/orchestrator/db"
	"github.com/D-pixel-crime/Endeavour/orchestrator/db/models"
	shared_vars "github.com/D-pixel-crime/Endeavour/orchestrator/shared"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type paymentStatusResponse struct {
	WorkflowID    string   `json:"workflow_id"`
	PaymentID     string   `json:"payment_id"`
	Amount        int      `json:"amount"`
	PaymentMethod string   `json:"payment_method"`
	Status        string   `json:"status"`
	WorkflowState string   `json:"workflow_state"`
	Completed     bool     `json:"completed"`
	SeatIDs       []string `json:"seat_ids"`
	NumSeats      int      `json:"num_seats"`
}

var paymentHTTPClient = &http.Client{Timeout: 5 * time.Second}

var fetchPaymentStatus = defaultFetchPaymentStatus

func defaultFetchPaymentStatus(ctx context.Context, workflowID uuid.UUID) (*paymentStatusResponse, error) {
	baseURL := strings.TrimRight(shared_vars.PaymentServiceURL, "/")
	if baseURL == "" {
		baseURL = "http://localhost:3001"
	}

	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, baseURL+"/payment/"+workflowID.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build payment status request: %w", err)
	}

	resp, err := paymentHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("payment service request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return &paymentStatusResponse{
			WorkflowID: workflowID.String(),
			Completed:  false,
		}, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("payment service returned unexpected status %d", resp.StatusCode)
	}

	var statusResp paymentStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statusResp); err != nil {
		return nil, fmt.Errorf("failed to decode payment status response: %w", err)
	}
	return &statusResp, nil
}

func isTicketEligibleState(state shared_vars.Orchestrator_WorkflowState) bool {
	return state == shared_vars.ORCHESTRATOR_WORKFLOW_PAYMENT ||
		state == shared_vars.ORCHESTRATOR_WORKFLOW_COMPLETED
}

func resolveCompletedPayment(ctx context.Context, wf *models.Workflow) (db.PaymentInfo, bool, error) {
	currentPayload := db.UnmarshalWorkflowPayload(wf.Payload)
	paymentInfo, paymentDone := db.ExtractPaymentInfoFromPayload(currentPayload)

	if wf.WorkflowState == shared_vars.ORCHESTRATOR_WORKFLOW_COMPLETED {
		if paymentInfo.Status == "" {
			paymentInfo.Status = shared_vars.STATUS_COMPLETED
		}
		if paymentInfo.PaymentID == "" || paymentInfo.Amount <= 0 {
			if statusResp, err := fetchPaymentStatus(ctx, wf.ID); err == nil && statusResp.Completed {
				if paymentInfo.PaymentID == "" {
					paymentInfo.PaymentID = statusResp.PaymentID
				}
				if paymentInfo.Amount <= 0 {
					paymentInfo.Amount = statusResp.Amount
				}
				if paymentInfo.PaymentMethod == "" {
					paymentInfo.PaymentMethod = statusResp.PaymentMethod
				}
				if len(paymentInfo.SeatIDs) == 0 {
					paymentInfo.SeatIDs = statusResp.SeatIDs
				}
				if paymentInfo.NumSeats <= 0 {
					paymentInfo.NumSeats = statusResp.NumSeats
				}
			}
		}
		return paymentInfo, true, nil
	}

	if paymentDone {
		return paymentInfo, true, nil
	}

	statusResp, err := fetchPaymentStatus(ctx, wf.ID)
	if err != nil {
		return db.PaymentInfo{}, false, err
	}
	if !statusResp.Completed {
		return db.PaymentInfo{}, false, nil
	}

	return db.PaymentInfo{
		PaymentID:     statusResp.PaymentID,
		Amount:        statusResp.Amount,
		PaymentMethod: statusResp.PaymentMethod,
		Status:        statusResp.Status,
		SeatIDs:       statusResp.SeatIDs,
		NumSeats:      statusResp.NumSeats,
	}, true, nil
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

	if !isTicketEligibleState(wf.WorkflowState) {
		c.JSON(http.StatusNotFound, gin.H{
			"error":          "ticket not found",
			"workflow_state": wf.WorkflowState,
		})
		return
	}

	paymentInfo, paymentCompleted, err := resolveCompletedPayment(ctx, wf)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "failed to verify payment status: " + err.Error()})
		return
	}
	if !paymentCompleted {
		c.JSON(http.StatusNotFound, gin.H{
			"error":          "ticket not found",
			"workflow_state": wf.WorkflowState,
		})
		return
	}

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

	if !isTicketEligibleState(lockedWf.WorkflowState) {
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
