package routes

import (
	get_handlers "github.com/D-pixel-crime/Endeavour/orchestrator/handlers/get_handlers"
	"github.com/gin-gonic/gin"
)

func GetRoutes(getRouter *gin.RouterGroup) {
	getRouter.GET("/workflow_id", get_handlers.GenerateWorkflowID)
}
