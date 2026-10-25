package middleware

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/time/rate"
)

// clientEntry holds rate limiter state for a specific client.
type clientEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// TokenBucketLimiter tracks per-client rate limiters using the token bucket algorithm.
type TokenBucketLimiter struct {
	mu              sync.RWMutex
	clients         map[string]*clientEntry
	rate            rate.Limit
	burst           int
	cleanupTTL      time.Duration
	cleanupInterval time.Duration
	stopChan        chan struct{}
	stopOnce        sync.Once
}

// LimiterConfig holds configuration for the Token Bucket Limiter.
type LimiterConfig struct {
	RPS             float64
	Burst           int
	CleanupTTL      time.Duration
	CleanupInterval time.Duration
}

// NewTokenBucketLimiter creates a new TokenBucketLimiter with background cleanup of idle clients.
func NewTokenBucketLimiter(cfg LimiterConfig) *TokenBucketLimiter {
	if cfg.RPS <= 0 {
		cfg.RPS = 10
	}
	if cfg.Burst <= 0 {
		cfg.Burst = int(cfg.RPS) * 2
		if cfg.Burst < 1 {
			cfg.Burst = 1
		}
	}
	if cfg.CleanupTTL <= 0 {
		cfg.CleanupTTL = 5 * time.Minute
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = 1 * time.Minute
	}

	tbl := &TokenBucketLimiter{
		clients:         make(map[string]*clientEntry),
		rate:            rate.Limit(cfg.RPS),
		burst:           cfg.Burst,
		cleanupTTL:      cfg.CleanupTTL,
		cleanupInterval: cfg.CleanupInterval,
		stopChan:        make(chan struct{}),
	}

	go tbl.cleanupLoop()

	return tbl
}

// cleanupLoop removes clients that haven't been seen within cleanupTTL.
func (tbl *TokenBucketLimiter) cleanupLoop() {
	ticker := time.NewTicker(tbl.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tbl.stopChan:
			return
		case now := <-ticker.C:
			tbl.mu.Lock()
			for ip, entry := range tbl.clients {
				if now.Sub(entry.lastSeen) > tbl.cleanupTTL {
					delete(tbl.clients, ip)
				}
			}
			tbl.mu.Unlock()
		}
	}
}

// Stop cleanly terminates the cleanup goroutine.
func (tbl *TokenBucketLimiter) Stop() {
	tbl.stopOnce.Do(func() {
		close(tbl.stopChan)
	})
}

// getLimiter returns or creates the rate.Limiter for a given client identifier.
func (tbl *TokenBucketLimiter) getLimiter(clientKey string) *rate.Limiter {
	tbl.mu.Lock()
	defer tbl.mu.Unlock()

	now := time.Now()
	entry, exists := tbl.clients[clientKey]
	if !exists {
		limiter := rate.NewLimiter(tbl.rate, tbl.burst)
		tbl.clients[clientKey] = &clientEntry{
			limiter:  limiter,
			lastSeen: now,
		}
		return limiter
	}

	entry.lastSeen = now
	return entry.limiter
}

// ExtractClientIP extracts the real client IP, respecting proxy headers if present.
func ExtractClientIP(c *gin.Context) string {
	// 1. Check X-Forwarded-For (standard reverse proxy / load balancer header)
	if xff := c.GetHeader("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			ip := strings.TrimSpace(parts[0])
			if ip != "" {
				return ip
			}
		}
	}

	// 2. Check X-Real-IP
	if xrip := c.GetHeader("X-Real-IP"); xrip != "" {
		ip := strings.TrimSpace(xrip)
		if ip != "" {
			return ip
		}
	}

	// 3. Fallback to Gin's ClientIP()
	ip := c.ClientIP()
	if ip == "" {
		return "unknown"
	}
	return ip
}

// Middleware returns a Gin middleware handler enforcing token bucket rate limiting.
func (tbl *TokenBucketLimiter) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		clientKey := ExtractClientIP(c)
		limiter := tbl.getLimiter(clientKey)

		// Set rate limit metadata headers
		c.Header("X-RateLimit-Limit", strconv.Itoa(tbl.burst))

		if !limiter.Allow() {
			c.Header("X-RateLimit-Remaining", "0")
			c.Header("Retry-After", "1")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":               fmt.Sprintf("Rate limit exceeded (%d requests burst limit). Please retry shortly.", tbl.burst),
				"retry_after_seconds": 1,
			})
			return
		}

		// Calculate approximate tokens remaining
		tokens := int(limiter.Tokens())
		if tokens < 0 {
			tokens = 0
		}
		c.Header("X-RateLimit-Remaining", strconv.Itoa(tokens))

		c.Next()
	}
}
