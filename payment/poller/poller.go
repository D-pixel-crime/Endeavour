package poller

import (
	"context"
	"log"
	"time"

	"github.com/D-pixel-crime/Endeavour/payment/handlers"
)

func StartPoller(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("Payment Ingress Poller started (interval: %v)", interval)

	for {
		select {
		case <-ctx.Done():
			log.Print("Payment Ingress Poller is stopping!")
			return

		case <-ticker.C:
			messages, err := PullBatchFromSQS(ctx)
			if err != nil {
				log.Printf("Failed to pull messages from payment ingress SQS: %v", err)
				continue
			}

			if len(messages) == 0 {
				continue
			}

			go handlers.ProcessIngressBatch(ctx, messages)
		}
	}
}
