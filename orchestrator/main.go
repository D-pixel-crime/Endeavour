package main

import (
	"log"
	"os"

	db "github.com/D-pixel-crime/Endeavor/orchestrator/db"
	shared_vars "github.com/D-pixel-crime/Endeavor/orchestrator/shared"
	"github.com/joho/godotenv"

	routes "github.com/D-pixel-crime/Endeavor/orchestrator/routes"

	"github.com/gin-gonic/gin"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Failed to load .env file: %v", err)
	}

	var err error
	shared_vars.Orchestrator_DB_Pool, err = db.ConnectToDb()
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer shared_vars.Orchestrator_DB_Pool.Close()

	r := gin.Default()

	getRouter := r.Group("/get")
	routes.GetRoutes(getRouter)

	postRouter := r.Group("/post")
	routes.PostRoutes(postRouter)

	port := os.Getenv("ORCHESTRATOR_PORT")
	log.Printf("Server is starting on port %s", port)
	if err := r.Run(port); err != nil {
		log.Fatalf("Failed to run the server: %v", err)
	}
}
