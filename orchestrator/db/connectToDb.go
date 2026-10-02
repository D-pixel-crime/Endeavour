package db

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ConnectToDb() (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(context.Background(), os.Getenv("ORCHESTRATOR_DB_URL"))
	if err != nil {
		return nil, err
	}

	var version string
	if err = pool.QueryRow(context.Background(), "SELECT version()").Scan(&version); err != nil {
		return nil, err
	}

	log.Println("Connected to:", version)

	return pool, nil
}
