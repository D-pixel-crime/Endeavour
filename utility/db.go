package utility

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ConnectToDB opens a pgxpool connection pool for dsn and verifies connectivity.
func ConnectToDB(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	if dsn == "" {
		return nil, fmt.Errorf("database connection string is empty")
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var version string
	if err := pool.QueryRow(checkCtx, "SELECT version()").Scan(&version); err != nil {
		pool.Close()
		return nil, err
	}

	log.Println("Connected to:", version)
	return pool, nil
}

// ConnectToDBFromEnv opens a pgxpool connection pool using the DSN in envKey.
func ConnectToDBFromEnv(ctx context.Context, envKey string) (*pgxpool.Pool, error) {
	dsn := os.Getenv(envKey)
	if dsn == "" {
		return nil, fmt.Errorf("%s is not set", envKey)
	}
	return ConnectToDB(ctx, dsn)
}

