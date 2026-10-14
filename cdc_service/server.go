package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/D-pixel-crime/Endeavour/aws_sqs_shared/monitoring"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/gin-gonic/gin"
)

type Server struct {
	httpServer *http.Server
	monitor    *monitoring.QueueMonitor
	router     *Router
	port       string
}

func NewServer(port string, sqsClient *sqs.Client, router *Router) *Server {
	monitor := monitoring.NewQueueMonitor(sqsClient)
	s := &Server{
		monitor: monitor,
		router:  router,
		port:    port,
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/health", s.healthHandler)
	r.GET("/stats", s.statsHandler)

	s.httpServer = &http.Server{
		Addr:         ":" + port,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return s
}

func (s *Server) Start() {
	go func() {
		log.Printf("Starting CDC HTTP server on :%s", s.port)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("CDC HTTP server error: %v", err)
		}
	}()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) healthHandler(c *gin.Context) {
	ctx := c.Request.Context()
	queues := s.router.AllQueues()

	unhealthy := make(map[string]string)
	for name, queueURL := range queues {
		if err := s.monitor.HealthCheck(ctx, queueURL); err != nil {
			unhealthy[name] = err.Error()
		}
	}

	if len(unhealthy) > 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unhealthy",
			"errors": unhealthy,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
	})
}

func (s *Server) statsHandler(c *gin.Context) {
	ctx := c.Request.Context()
	queues := s.router.AllQueues()

	if s.router.mode == ModeSingle {
		for _, queueURL := range queues {
			stats, err := s.monitor.GetQueueStats(ctx, queueURL)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, stats)
			return
		}
	}

	statsMap := make(map[string]any)
	for name, queueURL := range queues {
		stats, err := s.monitor.GetQueueStats(ctx, queueURL)
		if err != nil {
			statsMap[name] = gin.H{"error": err.Error()}
		} else {
			statsMap[name] = stats
		}
	}

	c.JSON(http.StatusOK, statsMap)
}
