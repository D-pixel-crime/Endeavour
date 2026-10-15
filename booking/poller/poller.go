package poller

import (
	"context"
	"log"
	"time"

	"github.com/D-pixel-crime/Endeavour/booking/handlers"
)

func StartPoller(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("Booking Ingress Poller started (interval: %v)", interval)

	for {
		select {
		case <-ctx.Done():
			log.Print("Booking Ingress Poller is stopping!")
			return

		case <-ticker.C:
			messages, err := PullBatchFromSQS(ctx)
			if err != nil {
				log.Printf("Failed to pull messages from booking ingress SQS: %v", err)
				continue
			}

			if len(messages) == 0 {
				continue
			}

			go handlers.ProcessIngressBatch(ctx, messages)
		}
	}
}
