package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/D-pixel-crime/Endeavour/payment/shared"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type paymentStatusResponse struct {
	WorkflowID    string    `json:"workflow_id"`
	PaymentID     string    `json:"payment_id,omitempty"`
	Amount        int       `json:"amount,omitempty"`
	PaymentMethod string    `json:"payment_method,omitempty"`
	Status        string    `json:"status"`
	WorkflowState string    `json:"workflow_state,omitempty"`
	Completed     bool      `json:"completed"`
	SeatIDs       []string  `json:"seat_ids,omitempty"`
	NumSeats      int       `json:"num_seats,omitempty"`
	CreatedAt     time.Time `json:"created_at,omitempty"`
}

func GetPaymentByWorkflowID(c *gin.Context) {
	workflowIDParam := c.Param("workflow_id")
	workflowUUID, err := uuid.Parse(workflowIDParam)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid workflow_id format"})
		return
	}

	if shared.DBPool == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database pool is not initialized"})
		return
	}

	ctx := c.Request.Context()
	query := `
		SELECT p.id, p.workflow_id, p.amount, COALESCE(p.payment_method, ''), p.status, p.created_at,
		       COALESCE(w.workflow_state::text, ''), w.payload
		FROM payments p
		LEFT JOIN workflows w ON w.id = p.workflow_id
		WHERE p.workflow_id = $1`

	var (
		paymentID     uuid.UUID
		wfID          uuid.UUID
		amount        int
		paymentMethod string
		status        string
		createdAt     time.Time
		workflowState string
		rawPayload    []byte
	)

	err = shared.DBPool.QueryRow(ctx, query, workflowUUID).Scan(
		&paymentID, &wfID, &amount, &paymentMethod, &status, &createdAt, &workflowState, &rawPayload,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			var wfState string
			wfErr := shared.DBPool.QueryRow(ctx, `SELECT workflow_state::text FROM workflows WHERE id = $1`, workflowUUID).Scan(&wfState)
			if wfErr == nil {
				c.JSON(http.StatusOK, paymentStatusResponse{
					WorkflowID:    workflowUUID.String(),
					Status:        wfState,
					WorkflowState: wfState,
					Completed:     false,
				})
				return
			}
			if wfErr == pgx.ErrNoRows {
				c.JSON(http.StatusNotFound, gin.H{
					"error":       "payment not found",
					"workflow_id": workflowUUID.String(),
					"completed":   false,
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query payment workflow: " + wfErr.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query payment: " + err.Error()})
		return
	}

	completed := status == shared.STATUS_COMPLETED &&
		(workflowState == "" || workflowState == string(shared.PAYMENT_WORKFLOW_COMPLETED))

	resp := paymentStatusResponse{
		WorkflowID:    wfID.String(),
		PaymentID:     paymentID.String(),
		Amount:        amount,
		PaymentMethod: paymentMethod,
		Status:        status,
		WorkflowState: workflowState,
		Completed:     completed,
		CreatedAt:     createdAt,
	}

	if len(rawPayload) > 0 {
		var payload struct {
			SeatIDs  []string `json:"seat_ids"`
			NumSeats int      `json:"num_seats"`
		}
		if json.Unmarshal(rawPayload, &payload) == nil {
			resp.SeatIDs = payload.SeatIDs
			resp.NumSeats = payload.NumSeats
		}
	}

	c.JSON(http.StatusOK, resp)
}

