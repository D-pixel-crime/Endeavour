package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Mode string

const (
	ModeSingle Mode = "SINGLE"
	ModeMulti  Mode = "MULTI"
)

type Config struct {
	ServiceName  string
	Mode         Mode
	DatabaseURL  string
	PollInterval time.Duration
	BatchSize    int
	HTTPPort     string

	TargetQueueURL string
	TargetQueues map[string]string
}

func LoadConfig() (*Config, error) {
	serviceName := os.Getenv("CDC_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "cdc-service"
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		user := os.Getenv("DB_USER")
		pass := os.Getenv("DB_PASSWORD")
		host := os.Getenv("DB_HOST")
		port := os.Getenv("DB_PORT")
		name := os.Getenv("DB_NAME")
		sslMode := os.Getenv("DB_SSLMODE")
		if sslMode == "" {
			sslMode = "disable"
		}
		if user != "" && host != "" && name != "" {
			if port == "" {
				port = "5432"
			}
			dbURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
				url.QueryEscape(user), url.QueryEscape(pass), host, port, name, sslMode)
		}
	}
	if dbURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is not set")
	}

	intervalSec := 5
	intervalStr := os.Getenv("CDC_ORCHESTRATOR_INTERVAL_SECONDS")
	if intervalStr == "" {
		intervalStr = os.Getenv("CDC_BOOKING_INTERVAL_SECONDS")
	}
	if intervalStr == "" {
		intervalStr = os.Getenv("CDC_PAYMENT_INTERVAL_SECONDS")
	}

	if intervalStr != "" {
		parsed, err := strconv.Atoi(intervalStr)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("invalid poll interval '%s': must be a positive integer", intervalStr)
		}
		intervalSec = parsed
	}

	batchSize := 10
	if bsStr := os.Getenv("CDC_BATCH_SIZE"); bsStr != "" {
		parsed, err := strconv.Atoi(bsStr)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("invalid CDC_BATCH_SIZE '%s'", bsStr)
		}
		if parsed > 10 {
			log.Printf("Reducing Batch size to a maximum of 10!")
			parsed = 10
		}
		batchSize = parsed
	}

	port := os.Getenv("CDC_PORT")
	if port == "" {
		port = os.Getenv("PORT")
	}
	if port == "" {
		port = os.Getenv("CDC_BOOKING_PORT")
	}
	if port == "" {
		port = os.Getenv("CDC_ORCHESTRATOR_PORT")
	}
	if port == "" {
		port = os.Getenv("CDC_ORCHESTRATOR_PORT")
	}
	if port == "" {
		port = "8080"
	}

	targetQueues := make(map[string]string)

	if multiStr := os.Getenv("TARGET_QUEUES"); multiStr != "" {
		pairs := strings.SplitSeq(multiStr, ",")
		for pair := range pairs {
			parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
			if len(parts) == 2 {
				workerType := strings.ToUpper(strings.TrimSpace(parts[0]))
				queueURL := strings.TrimSpace(parts[1])
				if workerType != "" && queueURL != "" {
					targetQueues[workerType] = queueURL
				}
			}
		}
	}

	for _, env := range os.Environ() {
		if strings.HasPrefix(env, "TARGET_QUEUE_") {
			parts := strings.SplitN(env, "=", 2)
			if len(parts) == 2 && parts[1] != "" {
				workerType := strings.ToUpper(strings.TrimPrefix(parts[0], "TARGET_QUEUE_"))
				targetQueues[workerType] = parts[1]
			}
		}
	}

	if _, ok := targetQueues["BOOKING"]; !ok {
		if u := os.Getenv("BOOKING_INGRESS_SQS_URL"); u != "" {
			targetQueues["BOOKING"] = u
		}
	}
	if _, ok := targetQueues["PAYMENT"]; !ok {
		if u := os.Getenv("PAYMENT_INGRESS_SQS_URL"); u != "" {
			targetQueues["PAYMENT"] = u
		}
	}

	singleQueueURL := os.Getenv("TARGET_SQS_URL")
	if singleQueueURL == "" {
		singleQueueURL = os.Getenv("EGRESS_SQS_URL")
	}
	if singleQueueURL == "" {
		singleQueueURL = os.Getenv("BOOKING_EGRESS_SQS_URL")
	}
	if singleQueueURL == "" {
		singleQueueURL = os.Getenv("PAYMENT_EGRESS_SQS_URL")
	}

	modeStr := strings.ToUpper(strings.TrimSpace(os.Getenv("CDC_MODE")))
	var mode Mode
	switch modeStr {
	case "SINGLE":
		mode = ModeSingle
	case "MULTI", "DUAL":
		mode = ModeMulti
	default:
		if len(targetQueues) > 1 {
			mode = ModeMulti
		} else {
			mode = ModeSingle
		}
	}

	if mode == ModeSingle {
		if singleQueueURL == "" && len(targetQueues) == 1 {
			for _, u := range targetQueues {
				singleQueueURL = u
				break
			}
		}
		if singleQueueURL == "" {
			return nil, fmt.Errorf("TARGET_SQS_URL is required for SINGLE mode")
		}
	} else {
		if len(targetQueues) == 0 {
			return nil, fmt.Errorf("TARGET_QUEUES (or individual TARGET_QUEUE_<WORKER>) is required for MULTI mode")
		}
	}

	return &Config{
		ServiceName:    serviceName,
		Mode:           mode,
		DatabaseURL:    dbURL,
		PollInterval:   time.Duration(intervalSec) * time.Second,
		BatchSize:      batchSize,
		HTTPPort:       port,
		TargetQueueURL: singleQueueURL,
		TargetQueues:   targetQueues,
	}, nil
}
