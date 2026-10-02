package main

import (
	"context"
	"log"
	"os"

	db "github.com/D-pixel-crime/Endeavor/orchestrator/db"
	"github.com/joho/godotenv"

	routes "github.com/D-pixel-crime/Endeavor/orchestrator/routes"

	"github.com/gin-gonic/gin"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	var err = db.ConnectToDb()
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer db.Conn.Close(context.Background())

	r := gin.Default()

	getRouter := r.Group("/get")
	routes.GetRoutes(getRouter)

	postRouter := r.Group("/post")
	routes.PostRoutes(postRouter)

	port := os.Getenv("PORT")
	log.Printf("Server is running on port %s", port)
	if err := r.Run(port); err != nil {
		log.Fatalf("Failed to run the server: %v", err)
	}
}
