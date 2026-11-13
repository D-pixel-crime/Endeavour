package consumer

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// MessageHandler processes a single SQS message and returns shouldDelete=true if it should be acknowledged.
type MessageHandler func(ctx context.Context, msg *Message) (shouldDelete bool, err error)

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
	handler MessageHandler
}

func NewBatchPoller(cfg PollerConfig, handler MessageHandler) *BatchPoller {
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

	processCtx, cancel := context.WithTimeout(ctx, p.cfg.ProcessTimeout)
	defer cancel()

	p.processMessageBatch(processCtx, messages)
}

func (p *BatchPoller) processMessageBatch(ctx context.Context, messages []*Message) {
	var toDelete []*Message

	for _, msg := range messages {
		if msg == nil {
			continue
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[%s Poller] Recovered from panic processing message %s: %v", p.cfg.Name, msg.ID, r)
				}
			}()

			shouldDelete, err := p.handler(ctx, msg)
			if err != nil {
				log.Printf("[%s Poller] Error processing message %s: %v", p.cfg.Name, msg.ID, err)
				return
			}

			if shouldDelete {
				toDelete = append(toDelete, msg)
			}
		}()
	}

	if len(toDelete) > 0 {
		DeleteAllBatches(ctx, p.cfg.Client, p.cfg.QueueURL, toDelete)
	}
}
