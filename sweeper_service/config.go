package main

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL    string
	PollInterval   time.Duration
	BatchSize      int
	TaskTimeoutSec int
}

func LoadConfig() (*Config, error) {
	dbURL := os.Getenv("SWEEPER_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("ORCHESTRATOR_DB_URL")
	}
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
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
		return nil, fmt.Errorf("database URL not configured; set SWEEPER_DATABASE_URL or ORCHESTRATOR_DB_URL")
	}

	pollInterval := 5 * time.Second
	if intervalStr := os.Getenv("SWEEPER_POLL_INTERVAL_SECONDS"); intervalStr != "" {
		if sec, err := strconv.Atoi(intervalStr); err == nil && sec > 0 {
			pollInterval = time.Duration(sec) * time.Second
		}
	}

	batchSize := 50
	if batchStr := os.Getenv("SWEEPER_BATCH_SIZE"); batchStr != "" {
		if b, err := strconv.Atoi(batchStr); err == nil && b > 0 {
			batchSize = b
		}
	}

	visTimeout := 30
	if visStr := os.Getenv("SWEEPER_VISIBILITY_TIMEOUT_SECONDS"); visStr != "" {
		if v, err := strconv.Atoi(visStr); err == nil && v > 0 {
			visTimeout = v
		}
	} else if visStr := os.Getenv("ORCHESTRATOR_VISIBILITY_TIMEOUT_SECONDS"); visStr != "" {
		if v, err := strconv.Atoi(visStr); err == nil && v > 0 {
			visTimeout = v
		}
	}

	taskTimeout := visTimeout
	if timeoutStr := os.Getenv("SWEEPER_TASK_TIMEOUT_SECONDS"); timeoutStr != "" {
		if t, err := strconv.Atoi(timeoutStr); err == nil && t > 0 {
			taskTimeout = t
		}
	}

	return &Config{
		DatabaseURL:    dbURL,
		PollInterval:   pollInterval,
		BatchSize:      batchSize,
		TaskTimeoutSec: taskTimeout,
	}, nil
}
