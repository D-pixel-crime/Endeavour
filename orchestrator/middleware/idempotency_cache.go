package middleware

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

type cacheItem struct {
	state     string
	expiresAt time.Time
}

// GlobalIdempotencyCache is the shared package instance used across handlers.
var GlobalIdempotencyCache = NewIdempotencyCache(60*time.Second, 30*time.Second)

// IdempotencyCache provides fast in-memory caching and in-flight deduplication
// for workflow creation to protect the database from duplicate check floods.
type IdempotencyCache struct {
	mu              sync.RWMutex
	cache           map[uuid.UUID]cacheItem
	inFlight        map[uuid.UUID]chan struct{}
	ttl             time.Duration
	cleanupInterval time.Duration
	stopChan        chan struct{}
	stopOnce        sync.Once
}

// NewIdempotencyCache initializes a thread-safe idempotency cache with automatic expiration.
func NewIdempotencyCache(ttl time.Duration, cleanupInterval time.Duration) *IdempotencyCache {
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	if cleanupInterval <= 0 {
		cleanupInterval = 30 * time.Second
	}

	ic := &IdempotencyCache{
		cache:           make(map[uuid.UUID]cacheItem),
		inFlight:        make(map[uuid.UUID]chan struct{}),
		ttl:             ttl,
		cleanupInterval: cleanupInterval,
		stopChan:        make(chan struct{}),
	}

	go ic.cleanupLoop()

	return ic
}

func (ic *IdempotencyCache) cleanupLoop() {
	ticker := time.NewTicker(ic.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ic.stopChan:
			return
		case now := <-ticker.C:
			ic.mu.Lock()
			for id, item := range ic.cache {
				if now.After(item.expiresAt) {
					delete(ic.cache, id)
				}
			}
			ic.mu.Unlock()
		}
	}
}

// Stop terminates the background cleanup goroutine.
func (ic *IdempotencyCache) Stop() {
	ic.stopOnce.Do(func() {
		close(ic.stopChan)
	})
}

// Get returns the cached workflow state if present and not expired.
func (ic *IdempotencyCache) Get(id uuid.UUID) (string, bool) {
	ic.mu.RLock()
	defer ic.mu.RUnlock()

	item, exists := ic.cache[id]
	if !exists {
		return "", false
	}

	if time.Now().After(item.expiresAt) {
		return "", false
	}

	return item.state, true
}

// Set stores or updates the workflow state in cache with the configured TTL.
func (ic *IdempotencyCache) Set(id uuid.UUID, state string) {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	ic.cache[id] = cacheItem{
		state:     state,
		expiresAt: time.Now().Add(ic.ttl),
	}
}

// TryLock attempts to lock a workflow ID for in-flight execution.
// If another request is currently processing this workflow ID, returns acquired = false.
func (ic *IdempotencyCache) TryLock(id uuid.UUID) (unlock func(), acquired bool) {
	ic.mu.Lock()
	defer ic.mu.Unlock()

	if _, exists := ic.inFlight[id]; exists {
		return nil, false
	}

	ch := make(chan struct{})
	ic.inFlight[id] = ch

	unlock = func() {
		ic.mu.Lock()
		defer ic.mu.Unlock()
		delete(ic.inFlight, id)
		close(ch)
	}

	return unlock, true
}

// WaitForInFlight waits for an in-flight workflow creation with the given ID to complete,
// up to the context timeout or waitTimeout. Returns true if completed, false if timed out.
func (ic *IdempotencyCache) WaitForInFlight(ctx context.Context, id uuid.UUID, waitTimeout time.Duration) bool {
	ic.mu.RLock()
	ch, exists := ic.inFlight[id]
	ic.mu.RUnlock()

	if !exists {
		return true
	}

	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	case <-time.After(waitTimeout):
		return false
	}
}
