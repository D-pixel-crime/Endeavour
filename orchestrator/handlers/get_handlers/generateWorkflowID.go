package gethandlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type ResBody struct {
	WorkflowID uuid.UUID `json:"workflow_id"`
}

func GenerateWorkflowID(c *gin.Context) {
	newUUID, err := uuid.NewV7()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to generate workflow UUID",
		})
		return
	}

	resBody := ResBody{
		WorkflowID: newUUID,
	}

	c.JSON(http.StatusOK, resBody)
}
