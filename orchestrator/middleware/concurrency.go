package middleware

import (
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/gin-gonic/gin"
)

// ConcurrencyLimiter limits the number of simultaneous in-flight requests.
type ConcurrencyLimiter struct {
	sem           chan struct{}
	maxConcurrent int
	inFlight      int64
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

// InFlight returns the current number of requests actively executing.
func (cl *ConcurrencyLimiter) InFlight() int64 {
	return atomic.LoadInt64(&cl.inFlight)
}

// MaxConcurrent returns the configured capacity.
func (cl *ConcurrencyLimiter) MaxConcurrent() int {
	return cl.maxConcurrent
}

// Middleware returns a Gin middleware that enforces in-flight concurrency limits.
func (cl *ConcurrencyLimiter) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		select {
		case cl.sem <- struct{}{}:
			atomic.AddInt64(&cl.inFlight, 1)
			defer func() {
				atomic.AddInt64(&cl.inFlight, -1)
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
