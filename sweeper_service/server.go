package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	httpServer *http.Server
	dbPool     *pgxpool.Pool
	sweeper    *Sweeper
	port       string
}

func NewServer(port string, dbPool *pgxpool.Pool, sweeper *Sweeper) *Server {
	s := &Server{
		dbPool:  dbPool,
		sweeper: sweeper,
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
		log.Printf("Starting Sweeper HTTP server on :%s", s.port)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Sweeper HTTP server error: %v", err)
		}
	}()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) healthHandler(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	if err := s.dbPool.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unhealthy",
			"error":  "database ping failed: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
	})
}

func (s *Server) statsHandler(c *gin.Context) {
	c.JSON(http.StatusOK, s.sweeper.GetStats())
}
