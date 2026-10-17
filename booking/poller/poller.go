package poller

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/D-pixel-crime/Endeavour/booking/handlers"
)

const defaultMaxConcurrentPulls = 5

func resolveMaxConcurrency(maxConcurrency []int) int {
	if len(maxConcurrency) > 0 && maxConcurrency[0] > 0 {
		return maxConcurrency[0]
	}
	if envStr := os.Getenv("BOOKING_MAX_CONCURRENT_PULLS"); envStr != "" {
		if val, err := strconv.Atoi(envStr); err == nil && val > 0 {
			return val
		}
	}
	return defaultMaxConcurrentPulls
}

func StartPoller(ctx context.Context, interval time.Duration, maxConcurrency ...int) {
	limit := resolveMaxConcurrency(maxConcurrency)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sem := make(chan struct{}, limit)
	var workerWg sync.WaitGroup

	log.Printf("Booking Ingress Poller started (interval: %v, maxConcurrency: %d)", interval, limit)

	for {
		select {
		case <-ctx.Done():
			log.Print("Booking Ingress Poller is stopping, waiting for in-flight tasks...")
			workerWg.Wait()
			log.Print("Booking Ingress Poller stopped cleanly")
			return

		case <-ticker.C:
			select {
			case sem <- struct{}{}:
			default:
				continue
			}

			workerWg.Add(1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Recovered from panic in booking ingress worker: %v", r)
					}
					<-sem
					workerWg.Done()
				}()
				pullAndProcessIngress(ctx)
			}()
		}
	}
}

func pullAndProcessIngress(ctx context.Context) {
	messages, err := PullBatchFromSQS(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Failed to pull messages from booking ingress SQS: %v", err)
		return
	}

	if len(messages) == 0 {
		return
	}

	processCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	handlers.ProcessIngressBatch(processCtx, messages)
}
