package main

import (
	"fmt"
	"maps"
	"strings"
)

type Router struct {
	mode           Mode
	singleQueueURL string
	multiQueues    map[string]string
}

func NewRouter(cfg *Config) *Router {
	r := &Router{
		mode:           cfg.Mode,
		singleQueueURL: cfg.TargetQueueURL,
		multiQueues:    make(map[string]string),
	}
	for k, v := range cfg.TargetQueues {
		r.multiQueues[strings.ToUpper(strings.TrimSpace(k))] = strings.TrimSpace(v)
	}
	return r
}

func (r *Router) ResolveQueue(workerType string) (string, error) {
	if r.mode == ModeSingle {
		if r.singleQueueURL == "" {
			return "", fmt.Errorf("single target queue URL is not configured")
		}
		return r.singleQueueURL, nil
	}

	normalized := strings.ToUpper(strings.TrimSpace(workerType))
	queueURL, ok := r.multiQueues[normalized]
	if !ok || queueURL == "" {
		return "", fmt.Errorf("queue URL not configured for worker type: %s", workerType)
	}
	return queueURL, nil
}

func (r *Router) AllQueues() map[string]string {
	if r.mode == ModeSingle {
		return map[string]string{"default": r.singleQueueURL}
	}
	res := make(map[string]string, len(r.multiQueues))
	maps.Copy(res, r.multiQueues)
	return res
}
