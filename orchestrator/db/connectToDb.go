package db

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
)

var Conn *pgx.Conn

func ConnectToDb() error {
	var err error
	Conn, err = pgx.Connect(context.Background(), os.Getenv("SUPABASE_DB_URL"))
	if err != nil {
		return err
	}

	var version string
	if err := Conn.QueryRow(context.Background(), "SELECT version()").Scan(&version); err != nil {
		return err
	}

	log.Println("Connected to:", version)

	return nil
}
