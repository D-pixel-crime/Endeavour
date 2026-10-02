package routes

import (
	post_handlers "github.com/D-pixel-crime/Endeavor/orchestrator/handlers/post_handlers"
	"github.com/gin-gonic/gin"
)

func PostRoutes(getRouter *gin.RouterGroup) {
	getRouter.POST("/create_workflow", post_handlers.CreateWorkflow)
}
