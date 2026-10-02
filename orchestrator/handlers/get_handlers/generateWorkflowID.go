package gethandlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type ResBody struct {
	WorkflowID string `json:"workflow_id"`
}

func GenerateWorkflowID(c *gin.Context) {
	newUUID := uuid.New().String()

	resBody := ResBody{
		WorkflowID: newUUID,
	}

	c.JSON(http.StatusOK, resBody)
}
