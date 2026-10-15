package poller

import (
	"context"
	"log"
	"time"

	egresshandlers "github.com/D-pixel-crime/Endeavour/orchestrator/handlers/egress_handlers"
)

// StartBookingEgressPoller periodically pulls and processes message batches from the booking egress SQS queue.
func StartBookingEgressPoller(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("Orchestrator Booking Egress Poller started (interval: %v)", interval)

	for {
		select {
		case <-ctx.Done():
			log.Print("Orchestrator Booking Egress Poller is stopping!")
			return

		case <-ticker.C:
			messages, err := PullBatchFromBookingEgressSQS(ctx)
			if err != nil {
				log.Printf("Failed to pull messages from booking egress SQS: %v", err)
				continue
			}

			if len(messages) == 0 {
				continue
			}

			go egresshandlers.ProcessBookingEgressBatch(ctx, messages)
		}
	}
}

// StartPoller provides a unified entrypoint to start orchestrator egress polling.
func StartPoller(ctx context.Context, interval time.Duration) {
	StartBookingEgressPoller(ctx, interval)
}
