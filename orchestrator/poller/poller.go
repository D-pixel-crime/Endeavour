package poller

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	egresshandlers "github.com/D-pixel-crime/Endeavour/orchestrator/handlers/egress_handlers"
)

const defaultMaxConcurrentPulls = 5

func resolveMaxConcurrency(maxConcurrency []int) int {
	if len(maxConcurrency) > 0 && maxConcurrency[0] > 0 {
		return maxConcurrency[0]
	}
	if envStr := os.Getenv("ORCHESTRATOR_MAX_CONCURRENT_PULLS"); envStr != "" {
		if val, err := strconv.Atoi(envStr); err == nil && val > 0 {
			return val
		}
	}
	return defaultMaxConcurrentPulls
}

func StartBookingEgressPoller(ctx context.Context, interval time.Duration, maxConcurrency ...int) {
	limit := resolveMaxConcurrency(maxConcurrency)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sem := make(chan struct{}, limit)
	var workerWg sync.WaitGroup

	log.Printf("Orchestrator Booking Egress Poller started (interval: %v, maxConcurrency: %d)", interval, limit)

	for {
		select {
		case <-ctx.Done():
			log.Print("Orchestrator Booking Egress Poller is stopping, waiting for in-flight tasks...")
			workerWg.Wait()
			log.Print("Orchestrator Booking Egress Poller stopped cleanly")
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
						log.Printf("Recovered from panic in booking egress worker: %v", r)
					}
					<-sem
					workerWg.Done()
				}()
				pullAndProcessBookingEgress(ctx)
			}()
		}
	}
}

func pullAndProcessBookingEgress(ctx context.Context) {
	messages, err := PullBatchFromBookingEgressSQS(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Failed to pull messages from booking egress SQS: %v", err)
		return
	}

	if len(messages) == 0 {
		return
	}

	processCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	egresshandlers.ProcessBookingEgressBatch(processCtx, messages)
}

func StartPaymentEgressPoller(ctx context.Context, interval time.Duration, maxConcurrency ...int) {
	limit := resolveMaxConcurrency(maxConcurrency)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sem := make(chan struct{}, limit)
	var workerWg sync.WaitGroup

	log.Printf("Orchestrator Payment Egress Poller started (interval: %v, maxConcurrency: %d)", interval, limit)

	for {
		select {
		case <-ctx.Done():
			log.Print("Orchestrator Payment Egress Poller is stopping, waiting for in-flight tasks...")
			workerWg.Wait()
			log.Print("Orchestrator Payment Egress Poller stopped cleanly")
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
						log.Printf("Recovered from panic in payment egress worker: %v", r)
					}
					<-sem
					workerWg.Done()
				}()
				pullAndProcessPaymentEgress(ctx)
			}()
		}
	}
}

func pullAndProcessPaymentEgress(ctx context.Context) {
	messages, err := PullBatchFromPaymentEgressSQS(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Failed to pull messages from payment egress SQS: %v", err)
		return
	}

	if len(messages) == 0 {
		return
	}

	processCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	egresshandlers.ProcessPaymentEgressBatch(processCtx, messages)
}
