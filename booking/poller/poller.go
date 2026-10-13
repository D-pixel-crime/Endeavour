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

	for {
		select {
		case <-ctx.Done():
			log.Print("Booking Poller is Stopping!")
			return

		case <-ticker.C:
			{
				messages, err := PullBatchFromSQS(ctx)
				if err != nil {
					log.Print("Failed to Pull Messages from SQS!")
					continue
				}

				if len(messages) == 0 {
					log.Print("No messages received from SQS")
					continue
				}

				go handlers.ProcessIngressBatch(ctx, messages)
			}
		}
	}
}
