package api_routes

import (
	get_handlers "github.com/D-pixel-crime/Endeavour/orchestrator/handlers/get_handlers"
	"github.com/gin-gonic/gin"
)

func GetRoutes(getRouter *gin.RouterGroup) {
	getRouter.GET("/workflow_id", get_handlers.GenerateWorkflowID)
	getRouter.GET("/ticket/:workflow_id", get_handlers.GetTicketByWorkflowID)
	getRouter.GET("/tickets/:workflow_id", get_handlers.GetTicketByWorkflowID)
	getRouter.GET("/workflow/:workflow_id", get_handlers.GetWorkflowByID)
}

