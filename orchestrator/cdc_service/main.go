package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/monitoring"
	booking_cdc "github.com/D-pixel-crime/Endeavour/orchestrator/cdc_service/booking"
	cdc_shared "github.com/D-pixel-crime/Endeavour/orchestrator/cdc_service/shared"
	db_orchestrator "github.com/D-pixel-crime/Endeavour/orchestrator/db"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Failed to load .env file: %v", err)
	}

	var err error
	cdc_shared.DBPool, err = db_orchestrator.ConnectToDb()
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer cdc_shared.DBPool.Close()

	interval_str := os.Getenv("CDC_ORCHESTRATOR_INTERVAL_SECONDS")
	if interval_str == "" {
		log.Fatalf("CDC_ORCHESTRATOR_INTERVAL_SECONDS is not set")
	}

	interval_sec, err := strconv.Atoi(interval_str)
	if err != nil {
		log.Fatalf("Failed to convert interval to seconds: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := booking_cdc.InitBooking(ctx); err != nil {
		log.Fatalf("Failed to initialize booking CDC: %v", err)
	}

	monitor := monitoring.NewQueueMonitor(booking_cdc.BookingSQSClient)

	r := gin.Default()

	booking_cdc.RegisterRoutes(r, monitor)

	port := os.Getenv("CDC_ORCHESTRATOR_PORT")
	if port == "" {
		port = "8081"
	}

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Starting HTTP server on :%s", port)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	go booking_cdc.StartBookingWorker(ctx, time.Duration(interval_sec)*time.Second)

	<-ctx.Done()
	log.Print("Shutting down CDC Orchestrator Worker...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Shutdown complete")
}
