package routes

import (
	posthandlers "github.com/D-pixel-crime/Endeavor/orchestrator/handlers/post_handlers"
	"github.com/gin-gonic/gin"
)

func PostRoutes(postRouter *gin.RouterGroup) {
	postRouter.POST("/create_workflow", posthandlers.CreateWorkflow)
}
