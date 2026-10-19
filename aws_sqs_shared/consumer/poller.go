package consumer

import (
	"context"
	"log"
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
	if cfg.ProcessTimeout <= 0 {
		cfg.ProcessTimeout = 20 * time.Second
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

	ticker := time.NewTicker(p.cfg.Interval)
	defer ticker.Stop()

	sem := make(chan struct{}, p.cfg.MaxConcurrency)
	var workerWg sync.WaitGroup

	log.Printf("[%s Poller] Started (interval: %v, maxConcurrency: %d)", p.cfg.Name, p.cfg.Interval, p.cfg.MaxConcurrency)

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

	processCtx, cancel := context.WithTimeout(context.Background(), p.cfg.ProcessTimeout)
	defer cancel()

	p.handler(processCtx, messages)
}
