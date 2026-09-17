package db

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5"
)

func ConnectToDb() (*pgx.Conn, error) {
	conn, err := pgx.Connect(context.Background(), os.Getenv("SUPABASE_DB_URL"))
	if err != nil {
		return nil, err
	}

	return conn, nil
}
