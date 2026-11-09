package middleware

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// ConcurrencyLimiter limits the number of simultaneous in-flight requests.
type ConcurrencyLimiter struct {
	sem           chan struct{}
	maxConcurrent int
}

// NewConcurrencyLimiter creates a new ConcurrencyLimiter bounded to maxConcurrent.
func NewConcurrencyLimiter(maxConcurrent int) *ConcurrencyLimiter {
	if maxConcurrent <= 0 {
		maxConcurrent = 25
	}
	return &ConcurrencyLimiter{
		sem:           make(chan struct{}, maxConcurrent),
		maxConcurrent: maxConcurrent,
	}
}

// Middleware returns a Gin middleware that enforces in-flight concurrency limits.
func (cl *ConcurrencyLimiter) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		select {
		case cl.sem <- struct{}{}:
			defer func() {
				<-cl.sem
			}()
			c.Next()
		default:
			// Capacity exhausted: immediately shed load to protect DB connection pools
			c.Header("Retry-After", "1")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":               fmt.Sprintf("Server is experiencing high concurrency load (%d max in-flight). Please retry shortly.", cl.maxConcurrent),
				"retry_after_seconds": 1,
			})
		}
	}
}
