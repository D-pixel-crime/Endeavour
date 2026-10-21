package consumer

import (
	"context"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type BatchHandler func(ctx context.Context, messages []*Message)

type PollerConfig struct {
	Name              string
	Client            *sqs.Client
	QueueURL          string
	Interval          time.Duration
	MaxConcurrency    int
	BatchSize         int32
	VisibilityTimeout int32
	ProcessTimeout    time.Duration
}

type BatchPoller struct {
	cfg     PollerConfig
	handler BatchHandler
}

func NewBatchPoller(cfg PollerConfig, handler BatchHandler) *BatchPoller {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 5
	}
	if cfg.BatchSize <= 0 || cfg.BatchSize > 10 {
		cfg.BatchSize = 10
	}
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = 30
	}

	// processTimeout must always be strictly less than visibilityTimeout
	visibilityTimeout := time.Duration(cfg.VisibilityTimeout) * time.Second
	defaultProcessTimeout := visibilityTimeout - 5*time.Second
	if defaultProcessTimeout <= 0 {
		defaultProcessTimeout = visibilityTimeout * 3 / 4
	}

	if cfg.ProcessTimeout <= 0 || cfg.ProcessTimeout >= visibilityTimeout {
		if cfg.ProcessTimeout >= visibilityTimeout {
			log.Printf("[%s Poller] Warning: processTimeout (%v) must be less than visibilityTimeout (%v); adjusting processTimeout to %v",
				cfg.Name, cfg.ProcessTimeout, visibilityTimeout, defaultProcessTimeout)
		}
		cfg.ProcessTimeout = defaultProcessTimeout
	}

	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.Name == "" {
		cfg.Name = "SQS"
	}

	return &BatchPoller{
		cfg:     cfg,
		handler: handler,
	}
}

func (p *BatchPoller) Start(ctx context.Context) {
	if p.cfg.Client == nil || p.cfg.QueueURL == "" {
		log.Printf("[%s Poller] Cannot start: missing SQS client or Queue URL", p.cfg.Name)
		return
	}
	if !strings.Contains(p.cfg.QueueURL, ".fifo") {
		log.Fatalf("[%s Poller] Queue %s must be a FIFO queue (must contain .fifo)", p.cfg.Name, p.cfg.QueueURL)
	}

	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()

	sem := make(chan struct{}, p.cfg.MaxConcurrency)
	var workerWg sync.WaitGroup

	log.Printf("[%s Poller] Started (interval: %v, maxConcurrency: %d, visibility: %ds, processTimeout: %v)",
		p.cfg.Name, p.cfg.Interval, p.cfg.MaxConcurrency, p.cfg.VisibilityTimeout, p.cfg.ProcessTimeout)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s Poller] Stopping, waiting for in-flight tasks...", p.cfg.Name)
			workerWg.Wait()
			log.Printf("[%s Poller] Stopped cleanly", p.cfg.Name)
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
						log.Printf("[%s Poller] Recovered from panic in worker: %v", p.cfg.Name, r)
					}
					<-sem
					workerWg.Done()
				}()

				p.pullAndProcess(ctx)
			}()
		}
	}
}

func (p *BatchPoller) pullAndProcess(ctx context.Context) {
	messages, err := ReceiveMessages(
		ctx,
		p.cfg.Client,
		p.cfg.QueueURL,
		p.cfg.BatchSize,
		0,
		p.cfg.VisibilityTimeout,
	)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("[%s Poller] Failed to pull messages from SQS: %v", p.cfg.Name, err)
		return
	}

	if len(messages) == 0 {
		return
	}

	receivedAt := time.Now()
	visibilityTimeout := time.Duration(p.cfg.VisibilityTimeout) * time.Second
	currentDeadline := receivedAt.Add(visibilityTimeout)

	batchTimeout := p.cfg.ProcessTimeout
	var toExtend []*Message
	var maxRequiredVisibilitySec int32 = p.cfg.VisibilityTimeout

	// Check completion time from createdAt and processTimeout for each message
	for _, msg := range messages {
		if msg == nil {
			continue
		}

		createdAt := msg.GetCreatedAt()
		procTimeout := p.cfg.ProcessTimeout
		if msg.Payload != nil {
			if ts, ok := msg.Payload["timeout_seconds"].(float64); ok && ts > 0 {
				procTimeout = time.Duration(ts) * time.Second
			} else if tp, ok := msg.Payload["timeout_parameter"].(float64); ok && tp > 0 {
				procTimeout = time.Duration(tp) * time.Second
			}
		}

		if procTimeout > batchTimeout {
			batchTimeout = procTimeout
		}

		// Calculate completion time from createdAt and processTimeout
		completionTime := createdAt.Add(procTimeout)

		// If completion time exceeds the visibilityPeriod, mark for visibility extension
		if completionTime.After(currentDeadline) || receivedAt.Add(procTimeout).After(currentDeadline) {
			toExtend = append(toExtend, msg)
			neededSec := int32(math.Ceil(time.Until(completionTime).Seconds())) + 5
			if neededSec > maxRequiredVisibilitySec {
				maxRequiredVisibilitySec = neededSec
			}
		}
	}

	// Send API request to extend visibility if completion time exceeds visibilityPeriod
	if len(toExtend) > 0 {
		if maxRequiredVisibilitySec > 43200 { // AWS SQS maximum visibility timeout is 12 hours
			maxRequiredVisibilitySec = 43200
		}
		log.Printf("[%s Poller] Completion time from createdAt and processTimeout exceeds visibilityPeriod (%v); extending visibility to %ds for %d message(s)",
			p.cfg.Name, visibilityTimeout, maxRequiredVisibilitySec, len(toExtend))
		if _, err := ExtendAllBatchesVisibility(ctx, p.cfg.Client, p.cfg.QueueURL, toExtend, maxRequiredVisibilitySec); err != nil {
			log.Printf("[%s Poller] Failed to extend visibility: %v", p.cfg.Name, err)
		}
	}

	processCtx, cancel := context.WithTimeout(ctx, batchTimeout)
	defer cancel()

	// In-flight visibility extension heartbeat
	done := make(chan struct{})
	defer close(done)

	go func() {
		heartbeatInterval := time.Duration(p.cfg.VisibilityTimeout/2) * time.Second
		if heartbeatInterval < time.Second {
			heartbeatInterval = time.Second
		}
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-processCtx.Done():
				return
			case <-ticker.C:
				log.Printf("[%s Poller] Processing still in-flight, extending visibility by %ds for %d message(s)",
					p.cfg.Name, p.cfg.VisibilityTimeout, len(messages))
				_, _ = ExtendAllBatchesVisibility(ctx, p.cfg.Client, p.cfg.QueueURL, messages, p.cfg.VisibilityTimeout)
			}
		}
	}()

	p.handler(processCtx, messages)
}
